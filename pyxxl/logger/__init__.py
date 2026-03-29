import logging
from contextlib import contextmanager
from typing import Generator

from pyxxl.context import g

from .common import LogBase
from .disk import DiskLog
from .redis import RedisLog


@contextmanager
def new_logger(factory: LogBase, log_id: int) -> Generator[logging.Logger, None, None]:
    """为单次任务执行创建上下文绑定的日志对象。"""

    logger = factory.get_logger(log_id)
    token = g.set_task_logger(logger)
    yield logger
    factory.after_running(logger)
    g._LOGGER.reset(token)
