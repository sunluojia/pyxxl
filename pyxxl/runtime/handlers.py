from __future__ import annotations

import asyncio
import inspect
import queue
import threading
import warnings
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from pyxxl import error
from pyxxl.config import ExecutorConfig
from pyxxl.context import g
from pyxxl.log import executor_logger
from pyxxl.model import HandlerRunMode, RunData
from pyxxl.runtime.process import (
    PROCESS_CONTEXT,
    build_process_log_config,
    normalize_handler_mode,
    process_handler_entry,
)
from pyxxl.types import DecoratedCallable


@dataclass
class HandlerInfo:
    """已注册任务函数的运行时包装对象。"""

    handler: Callable
    mode: Optional[HandlerRunMode] = None
    is_async: bool = False
    module_name: str = ""
    qualname: str = ""

    def __str__(self) -> str:
        mode_name = self.mode.value if self.mode else "unknown"
        return "<HandlerInfo {} mode={}>".format(self.handler.__name__, mode_name)

    def __post_init__(self) -> None:
        self.is_async = asyncio.iscoroutinefunction(self.handler)
        self.module_name = self.handler.__module__
        self.qualname = self.handler.__qualname__
        self.mode = self.mode or (HandlerRunMode.ASYNC if self.is_async else HandlerRunMode.THREAD)

        if self.mode == HandlerRunMode.ASYNC and not self.is_async:
            raise error.JobRegisterError("async mode requires an async handler.")
        if self.mode in (HandlerRunMode.THREAD, HandlerRunMode.PROCESS) and self.is_async:
            raise error.JobRegisterError(f"{self.mode.value} mode does not support async handlers.")
        if self.mode == HandlerRunMode.PROCESS and not self._is_process_safe():
            raise error.JobRegisterError(
                "process mode requires a top-level importable function; nested/local functions are not supported."
            )

    def _is_process_safe(self) -> bool:
        return inspect.isfunction(self.handler) and "<locals>" not in self.qualname

    async def start(
        self,
        timeout: int,
        run_data: Optional[RunData] = None,
        config: Optional[ExecutorConfig] = None,
    ) -> Any:
        """按指定模式启动任务函数。"""

        if self.mode == HandlerRunMode.ASYNC:
            return await asyncio.wait_for(self.handler(), timeout=timeout)
        if self.mode == HandlerRunMode.THREAD:
            return await self._start_thread(timeout)
        return await self._start_process(timeout, run_data, config)

    async def _start_thread(self, timeout: int) -> Any:
        """
        在线程池中运行同步函数。

        Python 线程无法像 Java `interrupt` 那样强制打断，所以这里只能提供协作式取消信号。
        """

        event = threading.Event()
        g.set_cancel_event(event)
        try:
            return await asyncio.wait_for(asyncio.to_thread(self.handler), timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError) as err:
            event.set()
            raise err

    async def _start_process(
        self,
        timeout: int,
        run_data: Optional[RunData],
        config: Optional[ExecutorConfig],
    ) -> Any:
        if run_data is None or config is None:
            raise ValueError("process 模式必须传入 RunData 和 ExecutorConfig。")

        cancel_event = PROCESS_CONTEXT.Event()
        result_queue = PROCESS_CONTEXT.Queue(maxsize=1)
        process = PROCESS_CONTEXT.Process(
            target=process_handler_entry,
            args=(
                self.module_name,
                self.qualname,
                asdict(run_data),
                build_process_log_config(config),
                cancel_event,
                result_queue,
            ),
            name=f"pyxxl_proc_{run_data.jobId}_{run_data.logId}",
            daemon=True,
        )
        process.start()

        try:
            return await asyncio.wait_for(self._wait_process_result(process, result_queue), timeout=timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            await self._stop_process(process, cancel_event)
            raise
        finally:
            await self._join_process(process)
            self._close_result_queue(result_queue)

    async def _wait_process_result(self, process: Any, result_queue: Any) -> Any:
        payload: Optional[Dict[str, Any]] = None
        while True:
            try:
                payload = result_queue.get_nowait()
                break
            except queue.Empty:
                if not process.is_alive():
                    break
                await asyncio.sleep(0.05)

        if payload is None:
            try:
                payload = result_queue.get_nowait()
            except queue.Empty:
                payload = None

        if payload is None:
            raise RuntimeError(f"Process mode handler exited unexpectedly with code {process.exitcode}.")
        if payload.get("ok"):
            return payload.get("result")

        error_message = payload.get("error") or "Process mode handler failed."
        stack = payload.get("traceback")
        if stack:
            raise RuntimeError(f"{error_message}\n{stack}")
        raise RuntimeError(error_message)

    async def _stop_process(self, process: Any, cancel_event: Any) -> None:
        """先发协作式停止信号，再逐级 terminate / kill。"""

        cancel_event.set()
        for _ in range(10):
            if not process.is_alive():
                return
            await asyncio.sleep(0.05)

        if process.is_alive():
            process.terminate()

        for _ in range(20):
            if not process.is_alive():
                return
            await asyncio.sleep(0.05)

        if process.is_alive() and hasattr(process, "kill"):
            process.kill()

    async def _join_process(self, process: Any) -> None:
        try:
            await asyncio.to_thread(process.join, 0.2)
        except Exception:  # pragma: no cover
            pass

        if hasattr(process, "close"):
            try:
                process.close()
            except ValueError:
                pass

    def _close_result_queue(self, result_queue: Any) -> None:
        try:
            result_queue.close()
            result_queue.join_thread()
        except (AttributeError, OSError, ValueError):
            pass


class JobHandler:
    """任务注册中心，负责把 XXL handler 名称映射到 Python 函数。"""

    def __init__(self, logger: Optional[Any] = None) -> None:
        self._handlers: Dict[str, HandlerInfo] = {}
        self.logger = logger or executor_logger

    def register(
        self,
        *args: Any,
        name: Optional[str] = None,
        replace: bool = False,
        mode: Optional[Union[str, HandlerRunMode]] = None,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        """
        注册任务函数。

        推荐写法：
        - `@handler.register`
        - `@handler.register(name="demo")`
        - `@handler.register(name="demo", mode="process")`
        """

        parsed_name = name
        parsed_mode = normalize_handler_mode(mode)

        if args and callable(args[0]):
            if len(args) != 1:
                raise TypeError("Bare register decorator only accepts the function as a positional argument.")
            return self._decorate(args[0], parsed_name, replace, parsed_mode, explicit_mode=parsed_mode is not None)

        if len(args) > 2:
            raise TypeError("register accepts at most two positional arguments: name and mode.")
        if len(args) >= 1:
            if parsed_name is not None:
                raise TypeError("handler name specified twice.")
            parsed_name = args[0]
        if len(args) == 2:
            if parsed_mode is not None:
                raise TypeError("handler mode specified twice.")
            parsed_mode = normalize_handler_mode(args[1])

        explicit_mode = parsed_mode is not None

        def func_wrapper(func: DecoratedCallable) -> DecoratedCallable:
            return self._decorate(func, parsed_name, replace, parsed_mode, explicit_mode=explicit_mode)

        return func_wrapper

    def _decorate(
        self,
        func: DecoratedCallable,
        handler_name: Optional[str],
        replace: bool,
        mode: Optional[HandlerRunMode],
        *,
        explicit_mode: bool,
    ) -> DecoratedCallable:
        real_name = handler_name or func.__name__
        if real_name in self._handlers and replace is False:
            raise error.JobRegisterError("handler %s already registered." % real_name)

        handler = HandlerInfo(handler=func, mode=mode)
        if handler.mode == HandlerRunMode.THREAD and not explicit_mode:
            warnings.warn(
                "同步函数默认使用 thread 模式，无法像 Java 一样强制中断，建议优先考虑 async 或 process 模式。",
                SyntaxWarning,
                stacklevel=3,
            )

        self._handlers[real_name] = handler
        self.logger.debug("register job %s, mode=%s", real_name, handler.mode.value)
        return func

    def register_async(
        self,
        *args: Any,
        name: Optional[str] = None,
        replace: bool = False,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        return self.register(*args, name=name, replace=replace, mode=HandlerRunMode.ASYNC)

    def register_thread(
        self,
        *args: Any,
        name: Optional[str] = None,
        replace: bool = False,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        return self.register(*args, name=name, replace=replace, mode=HandlerRunMode.THREAD)

    def register_process(
        self,
        *args: Any,
        name: Optional[str] = None,
        replace: bool = False,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        return self.register(*args, name=name, replace=replace, mode=HandlerRunMode.PROCESS)

    def get(self, name: str) -> Optional[HandlerInfo]:
        return self._handlers.get(name)

    def handlers_info(self) -> List[str]:
        return [f"<{name} is_async:{info.is_async} mode:{info.mode.value}>" for name, info in self._handlers.items()]
