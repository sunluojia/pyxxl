import logging
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional, Union

from pyxxl.context import g
from pyxxl.log import executor_logger
from pyxxl.types import LogRequest, LogResponse
from pyxxl.utils import try_import

from .common import MAX_LOG_TAIL_LINES, TASK_FORMATTER, LogBase, PyxxlStreamHandler

if TYPE_CHECKING:
    from logging import Handler

    import redis
else:
    redis = try_import("redis")


KEY_PREFIX = "pyxxl:log:{app}:{log_id}"


class RedisHandler(logging.Handler):
    """基于 Redis List 的任务日志 handler，并限制尾部保留行数。"""

    terminator = "\n"

    def __init__(
        self,
        key: str,
        ttl: int,
        rclient: "redis.Redis",
        *,
        level: int = logging.NOTSET,
        max_lines: Optional[int] = None,
    ) -> None:
        super().__init__(level)
        self.rclient = rclient
        self.key = key
        self.ttl = ttl
        self.max_lines = max_lines or MAX_LOG_TAIL_LINES

    def emit(self, record: Any) -> None:
        try:
            xxl_kwargs = g.try_get_run_data()
            record.logId = xxl_kwargs.logId if xxl_kwargs else "NotInTask"
            # 用 pipeline 保证一次日志写入时 append/trim/expire 尽量原子。
            p = self.rclient.pipeline()
            p.rpush(self.key, self.format(record) + self.terminator)
            p.ltrim(self.key, -self.max_lines, -1)
            p.expire(self.key, self.ttl)
            p.execute()
        except redis.RedisError as e:  # pragma: no cover
            print("log to redis failed. %s" % str(e))  # pragma: no cover


class RedisLog(LogBase):
    """基于 Redis 的任务日志后端，适合多实例或无本地磁盘场景。"""

    def __init__(
        self,
        app: str,
        redis_client: Union[str, "redis.ConnectionPool"],
        log_tail_lines: int = 0,
        expired_days: float = 14,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if redis is None:
            raise ImportError("Depend on redis. pip install redis or pip install pyxxl[redis].")  # pragma: no cover
        self.executor_logger = logger or executor_logger
        self.app = app
        self.log_tail_lines = log_tail_lines or MAX_LOG_TAIL_LINES
        self.expired_seconds = round(expired_days * 3600 * 24)
        if isinstance(redis_client, str):
            rclient = redis.Redis.from_url(redis_client)
        elif isinstance(redis_client, redis.ConnectionPool):
            rclient = redis.Redis(connection_pool=redis_client)
        else:
            raise TypeError(
                "pool expect Union[str, redis.ConnectionPool], got %s." % type(redis_client)
            )  # pragma: no cover
        self.rclient = rclient

    def get_logger(self, log_id: int, *, stdout: bool = True, level: int = logging.INFO) -> logging.Logger:
        logger = logging.getLogger("pyxxl.task_log.redis.task-{%s}" % log_id)
        logger.propagate = False
        logger.setLevel(level)
        handlers: list[Handler] = [PyxxlStreamHandler()] if stdout else []
        handlers.append(RedisHandler(self.key(log_id), self.expired_seconds, self.rclient))
        for h in handlers:
            h.setFormatter(TASK_FORMATTER)
            h.setLevel(level)
            logger.addHandler(h)
        return logger

    def key(self, log_id: int) -> str:
        return KEY_PREFIX.format(app=self.app, log_id=log_id)

    async def read_task_logs(self, log_id: int, *, key: Optional[str] = None) -> str:
        key = key or self.key(log_id)
        # redis-py 仍是同步客户端，这里先保持现状，后续再做异步优化。
        try:
            return "".join(i.decode() for i in self.rclient.lrange(key, 0, -1))
        except redis.RedisError as err:  # pragma: no cover
            self.executor_logger.warning("Read redis task logs failed key=%s error=%s", key, err)
            return ""

    async def get_logs(self, request: LogRequest, *, key: Optional[str] = None) -> LogResponse:
        key = key or self.key(request["logId"])
        from_line = request["fromLineNum"] - 1
        to_line = request["fromLineNum"] - 1 + self.log_tail_lines
        try:
            llen = self.rclient.llen(key)
        except redis.RedisError as err:  # pragma: no cover
            self.executor_logger.warning("Read redis logs failed key=%s error=%s", key, err)
            return LogResponse(
                fromLineNum=request["fromLineNum"],
                toLineNum=request["fromLineNum"],
                logContent="No such logid logs.",
                isEnd=True,
            )
        if from_line >= llen:
            logs = "No such logid logs." if llen == 0 else ""
            to_line_num = request["fromLineNum"]
        else:
            # Redis LRANGE 的结束下标是闭区间，和 Python 切片不同。
            try:
                logs = "".join(i.decode() for i in self.rclient.lrange(key, from_line, to_line - 1))
            except redis.RedisError as err:  # pragma: no cover
                self.executor_logger.warning("Read redis log slice failed key=%s error=%s", key, err)
                logs = ""
            to_line_num = to_line

        return LogResponse(
            fromLineNum=request["fromLineNum"],
            toLineNum=to_line_num,
            logContent=logs,
            isEnd=llen <= to_line,
        )

    @asynccontextmanager
    async def mock_write(self, *lines: Any) -> AsyncGenerator[str, None]:
        key = self.key(int(time.time() * 1000))
        self.rclient.rpush(key, *lines)
        yield key
        self.rclient.delete(key)

    @asynccontextmanager
    async def mock_logger(self, log_id: int) -> AsyncGenerator[LogBase, None]:
        yield self
        self.rclient.delete(self.key(log_id))
