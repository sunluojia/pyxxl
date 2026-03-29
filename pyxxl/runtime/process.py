from __future__ import annotations

import asyncio
import importlib
import multiprocessing as mp
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from pyxxl import error
from pyxxl.config import ExecutorConfig
from pyxxl.context import g
from pyxxl.logger import DiskLog, LogBase, RedisLog, new_logger
from pyxxl.model import HandlerRunMode, RunData

PROCESS_CONTEXT = mp.get_context("spawn")


@dataclass(frozen=True)
class ProcessLogConfig:
    """子进程写任务日志时所需的最小日志配置。"""

    log_target: str
    log_local_dir: str
    log_expired_days: float
    log_redis_uri: str
    executor_app_name: str


def resolve_process_handler(module_name: str, qualname: str) -> Callable[..., Any]:
    """在子进程里按模块路径重新定位顶层函数。"""

    if "<locals>" in qualname:
        raise RuntimeError("Process mode handler must be a top-level importable function.")

    obj: Any = importlib.import_module(module_name)
    for attr in qualname.split("."):
        obj = getattr(obj, attr)
    return obj


def build_process_log_factory(config: ProcessLogConfig) -> LogBase:
    """根据配置为子进程构造日志后端。"""

    if config.log_target == "disk":
        return DiskLog(log_path=config.log_local_dir, expired_days=config.log_expired_days)
    if config.log_target == "redis":
        return RedisLog(
            config.executor_app_name,
            config.log_redis_uri,
            expired_days=config.log_expired_days,
        )
    raise NotImplementedError(f"Unsupported process log target: {config.log_target}")


def normalize_handler_mode(mode: Optional[Union[str, HandlerRunMode]]) -> Optional[HandlerRunMode]:
    """把用户传入的执行模式统一转换成枚举。"""

    if mode is None or isinstance(mode, HandlerRunMode):
        return mode

    raw_mode = str(mode).strip().lower()
    for candidate in HandlerRunMode:
        if raw_mode in {candidate.value, candidate.name.lower()}:
            return candidate

    raise error.JobRegisterError(f"unknown handler run mode [{mode}].")


def process_handler_entry(
    module_name: str,
    qualname: str,
    run_data_payload: Dict[str, Any],
    log_config: ProcessLogConfig,
    cancel_event: Any,
    result_queue: Any,
) -> None:
    """子进程入口：恢复上下文、执行用户函数，并把结果写回父进程。"""

    run_data = RunData.from_dict(run_data_payload)
    g.set_xxl_run_data(run_data)
    g.set_cancel_event(cancel_event)

    task_log = build_process_log_factory(log_config)
    with new_logger(task_log, run_data.logId) as task_logger:
        try:
            handler = resolve_process_handler(module_name, qualname)
            result = handler()
            if asyncio.iscoroutine(result):
                raise TypeError("Process mode does not support async handlers.")
            result_queue.put({"ok": True, "result": result})
        except BaseException as err:  # pylint: disable=broad-except
            task_logger.exception(err, exc_info=True)
            result_queue.put(
                {
                    "ok": False,
                    "error": str(err),
                    "traceback": traceback.format_exc(),
                }
            )


def build_process_log_config(config: ExecutorConfig) -> ProcessLogConfig:
    """从执行器总配置中抽取子进程可序列化的日志参数。"""

    return ProcessLogConfig(
        log_target=config.log_target,
        log_local_dir=config.log_local_dir,
        log_expired_days=config.log_expired_days,
        log_redis_uri=config.log_redis_uri,
        executor_app_name=config.executor_app_name,
    )
