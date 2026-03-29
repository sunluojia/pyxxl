import logging
from contextvars import ContextVar, Token
from typing import Any, Optional

from pyxxl.model import RunData


class GlobalVars:
    """执行期上下文，向任务函数暴露当前调度数据、任务日志和取消信号。"""

    _DATA: ContextVar[Optional[RunData]] = ContextVar("_DATA", default=None)
    _LOGGER: ContextVar[Optional[logging.Logger]] = ContextVar("_LOGGER", default=None)
    _EVENT: ContextVar[Any] = ContextVar("_EVENT", default=None)

    @classmethod
    def set_xxl_run_data(cls, data: RunData) -> None:
        cls._DATA.set(data)

    @classmethod
    def try_get_run_data(cls) -> Optional[RunData]:
        return cls._DATA.get()

    @property
    def xxl_run_data(self) -> RunData:
        data = self._DATA.get()
        if data is None:
            raise RuntimeError("当前上下文中不存在任务运行参数。")
        return data

    @classmethod
    def set_task_logger(cls, logger: logging.Logger) -> Token:
        return cls._LOGGER.set(logger)

    @property
    def logger(self) -> logging.Logger:  # pragma: no cover
        logger = self._LOGGER.get()
        if logger is None:
            raise RuntimeError("当前上下文中不存在任务日志对象。")
        return logger

    @classmethod
    def set_cancel_event(cls, event: Any) -> None:
        cls._EVENT.set(event)

    @property
    def cancel_event(self) -> Any:  # pragma: no cover
        event = self._EVENT.get()
        if event is None:
            raise RuntimeError("当前上下文中不存在取消信号。")
        return event


g = GlobalVars()
