import importlib.metadata

from .app import PyxxlRunner
from .config import ExecutorConfig
from .context import g
from .model import ExecutorBlockStrategy, HandlerRunMode, RunData
from .protocol.admin_client import JsonType, Response, XXL
from .runtime import CallbackManager, CallbackRequest, Executor, HandlerInfo, JobHandler, XXLTask

try:
    __version__ = importlib.metadata.version("pyxxl")
except importlib.metadata.PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"


__all__ = [
    "CallbackManager",
    "CallbackRequest",
    "Executor",
    "ExecutorBlockStrategy",
    "ExecutorConfig",
    "HandlerInfo",
    "HandlerRunMode",
    "JobHandler",
    "JsonType",
    "PyxxlRunner",
    "Response",
    "RunData",
    "XXL",
    "XXLTask",
    "g",
]
