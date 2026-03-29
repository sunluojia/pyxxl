import asyncio
import logging
import os
from multiprocessing import Process
from typing import Any, AsyncGenerator, NamedTuple, Optional

from aiohttp import web

from pyxxl.config import ExecutorConfig
from pyxxl.logger import DiskLog, LogBase, RedisLog
from pyxxl.protocol import XXL, create_app
from pyxxl.runtime import Executor as RuntimeExecutor
from pyxxl.runtime import JobHandler
from pyxxl.utils import setup_logging, try_import

if try_import("prometheus_client"):
    from pyxxl.monitoring import failed, success

    class InstrumentedExecutor(RuntimeExecutor):
        """把 Prometheus 指标回调接到执行器生命周期里。"""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            user_succeeded_callback = kwargs.pop("succeeded_callback", None)
            user_failed_callback = kwargs.pop("failed_callback", None)

            def succeeded_callback() -> None:
                success()
                if user_succeeded_callback is not None:
                    user_succeeded_callback()

            def failed_callback(reason: str) -> None:
                failed(reason)
                if user_failed_callback is not None:
                    user_failed_callback(reason)

            super().__init__(
                *args,
                succeeded_callback=succeeded_callback,
                failed_callback=failed_callback,
                **kwargs,
            )

else:
    InstrumentedExecutor = RuntimeExecutor


async def server_info_ctx(app: web.Application) -> AsyncGenerator:
    """记录执行器进程生命周期，便于排查部署问题。"""

    pid = os.getpid()
    state: ExecutorAppState = app["pyxxl_state"]
    state.executor_logger.info("start executor server with pid %s.", pid)
    yield
    state.executor_logger.info("stop executor server. pid=%s.", pid)


class ExecutorAppState(NamedTuple):
    xxl_client: XXL
    executor: InstrumentedExecutor
    task_log: LogBase
    executor_logger: logging.Logger


class StateHolder:
    """在应用启动前预置到 aiohttp app 中，避免启动后再修改 app state。"""

    def __init__(self) -> None:
        self.state: Optional[ExecutorAppState] = None

    def set(self, state: ExecutorAppState) -> None:
        self.state = state

    def __getattr__(self, item: str) -> Any:
        if self.state is None:
            raise AttributeError(f"pyxxl state is not ready: {item}")
        return getattr(self.state, item)


class PyxxlRunner:
    """面向用户的高层入口，负责组装配置、HTTP 服务、注册循环和关闭流程。"""

    daemon: Optional[Process] = None
    _logging_setup: bool = False

    def __init__(self, config: ExecutorConfig, handler: Optional[JobHandler] = None):
        self.handler = handler or JobHandler(logger=config.executor_logger)
        self.config = config
        self.log_level = logging.DEBUG if self.config.debug else logging.INFO

    async def _register_task(self, xxl_client: XXL) -> None:
        """持续向 admin 注册执行器，行为上对齐 Java `ExecutorRegistryThread`。"""

        try:
            while True:
                await xxl_client.registry(self.config.executor_app_name, self.config.executor_baseurl)
                await asyncio.sleep(10)
        finally:
            self.config.executor_logger.warning("Register task is exit.")

    def _get_xxl_client(self) -> XXL:
        """按规范化配置创建 admin 客户端。"""

        return XXL(
            self.config.admin_baseurls,
            token=self.config.access_token,
            logger=self.config.executor_logger,
            retry_times=self.config.http_retry_times,
            retry_duration=self.config.http_retry_duration,
            http_timeout=self.config.http_timeout,
        )

    def _get_log_backend(self) -> LogBase:
        """根据配置选择任务日志后端。"""

        if self.config.log_target == "disk":
            return DiskLog(
                log_path=self.config.log_local_dir,
                expired_days=self.config.log_expired_days,
                logger=self.config.executor_logger,
            )

        if self.config.log_target == "redis":
            return RedisLog(
                self.config.executor_app_name,
                self.config.log_redis_uri,
                expired_days=self.config.log_expired_days,
                logger=self.config.executor_logger,
            )

        raise NotImplementedError

    async def _cleanup_ctx(self, app: web.Application) -> AsyncGenerator:
        task_log = self._get_log_backend()
        xxl_client = self._get_xxl_client()
        executor = InstrumentedExecutor(
            xxl_client,
            config=self.config,
            handler=self.handler,
            logger_factory=task_log,
        )

        state = ExecutorAppState(
            xxl_client=xxl_client,
            executor=executor,
            task_log=task_log,
            executor_logger=self.config.executor_logger,
        )
        holder: StateHolder = app["pyxxl_state"]
        holder.set(state)

        await state.executor.start_callback_manager()
        executor_log_task = asyncio.create_task(
            state.task_log.expired_loop(self.config.log_clean_interval),
            name="log_task",
        )
        register_task = asyncio.create_task(self._register_task(state.xxl_client), name="register_task")

        if state.executor.handler:
            state.executor_logger.info("register with handlers %s", list(executor.handler.handlers_info()))
        else:
            state.executor_logger.warning("register with handlers is empty")  # pragma: no cover

        yield

        register_task.cancel()
        executor_log_task.cancel()
        await state.xxl_client.registryRemove(self.config.executor_app_name, self.config.executor_baseurl)
        if self.config.graceful_close:
            await state.executor.graceful_close(self.config.graceful_timeout)
        else:
            await state.executor.shutdown()
            await state.executor.stop_callback_manager(timeout=1, close=True)

        if self.config.graceful_close:
            await state.executor.stop_callback_manager(timeout=1, close=True)

        await state.xxl_client.close()
        state.executor_logger.info("cleanup executor success.")

    def create_server_app(self) -> web.Application:
        """创建带有执行器清理上下文的 aiohttp 应用。"""

        app = create_app()
        app["pyxxl_state"] = StateHolder()
        app.cleanup_ctx.append(self._cleanup_ctx)
        app.cleanup_ctx.append(server_info_ctx)
        return app

    def _setup_logging(self) -> None:
        if not self._logging_setup:
            setup_logging(self.config.executor_log_path, "pyxxl", level=self.log_level)
            self._logging_setup = True

    def run_executor(self, handle_signals: bool = True) -> None:
        """以前台方式启动执行器。"""

        self._setup_logging()
        web.run_app(
            self.create_server_app(),
            port=self.config.executor_listen_port,
            host=self.config.executor_listen_host,
            handle_signals=handle_signals,
        )

    def _runner(self) -> None:
        self.run_executor(handle_signals=True)

    def run_with_daemon(self) -> None:
        """以守护进程方式启动执行器。"""

        daemon = Process(target=self._runner, name="pyxxljob", daemon=True)
        daemon.start()
        self.daemon = daemon

    @property
    def register(self) -> Any:
        return self.handler.register

    @property
    def register_async(self) -> Any:
        return self.handler.register_async

    @property
    def register_thread(self) -> Any:
        return self.handler.register_thread

    @property
    def register_process(self) -> Any:
        return self.handler.register_process
