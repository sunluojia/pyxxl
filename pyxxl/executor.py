from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, MutableSet, Optional
from uuid import uuid4

from pyxxl import error
from pyxxl.ctx import g
from pyxxl.enum import executorBlockStrategy
from pyxxl.log import executor_logger
from pyxxl.logger import DiskLog, LogBase, new_logger
from pyxxl.schema import RunData
from pyxxl.setting import ExecutorConfig
from pyxxl.types import DecoratedCallable
from pyxxl.xxl_client import XXL

# Track fire-and-forget cleanup tasks so they are not garbage collected mid-flight.
_BACKGROUND_TASKS: MutableSet[asyncio.Task] = set()


def _spawn_task(task: asyncio.Task) -> None:
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)


@dataclass
class HandlerInfo:
    """Runtime wrapper for a registered handler."""

    handler: Callable
    is_async: bool = False

    def __str__(self) -> str:
        return "<HandlerInfo {}>".format(self.handler.__name__)

    def __post_init__(self) -> None:
        self.is_async = asyncio.iscoroutinefunction(self.handler)

    async def start(self, timeout: int) -> Any:
        if self.is_async:
            return await asyncio.wait_for(self.handler(), timeout=timeout)
        # Python thread-pool tasks cannot be interrupted like Java JobThread.
        # Expose a cooperative cancel signal so sync handlers can stop themselves.
        event = threading.Event()
        g.set_cancel_event(event)
        try:
            return await asyncio.wait_for(asyncio.to_thread(self.handler), timeout=timeout)
        except (asyncio.exceptions.TimeoutError, asyncio.CancelledError) as e:
            event.set()
            # logger.debug("Get error for sync task {}".format(self))
            raise e


class XXLTask:
    """Mutable task state for one in-flight or queued XXL trigger."""

    def __init__(self, task: Optional[asyncio.Task], data: RunData):
        self.task = task
        self.data = data
        self.cancel_reason: Optional[str] = None
        self.started = False
        self.cleaned = False

    def __str__(self) -> str:
        return "<XXLTask task={} data={}>".format(self.task, self.data)

    @property
    def cancel(self) -> Any:
        assert self.task is not None
        return self.task.cancel


@dataclass
class CallbackRequest:
    """Serialized callback payload queued for admin delivery/retry."""

    log_id: int
    timestamp: int
    code: int
    msg: Optional[str] = None
    attempts: int = 0
    persisted_path: Optional[str] = None


class CallbackManager:
    """Java TriggerCallbackThread equivalent with local persistence and replay."""

    CALLBACK_FILE_GLOB = "callback-*.json"

    def __init__(
        self,
        xxl_client: XXL,
        loop: asyncio.AbstractEventLoop,
        logger: logging.Logger,
        retry_interval: float,
        store_dir: Path,
    ) -> None:
        self.xxl_client = xxl_client
        self.loop = loop
        self.logger = logger
        self.retry_interval = max(retry_interval, 0.01)
        self.store_dir = store_dir
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self._queue: asyncio.Queue[CallbackRequest] = asyncio.Queue()
        self._worker: Optional[asyncio.Task] = None
        self._worker_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._retry_tasks: MutableSet[asyncio.Task] = set()
        self._pending = 0
        self._pending_empty = asyncio.Event()
        self._pending_empty.set()
        self._closed = False
        self._started = False

    @property
    def pending_count(self) -> int:
        return self._pending

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                return
            self._started = True
            # Recover persisted callback records before accepting new work so the
            # delivery order stays close to the original trigger order.
            await self._replay_persisted_requests()

    async def enqueue(self, request: CallbackRequest) -> None:
        if self._closed:
            self.logger.warning("Callback manager already closed, drop callback logId=%s", request.log_id)
            return
        await self.start()
        await self._queue_request(request)
        self.logger.debug("Enqueued callback logId=%s code=%s", request.log_id, request.code)

    async def stop(self, timeout: Optional[float] = None, close: bool = False) -> bool:
        if close:
            self._closed = True
        drained = True
        if self.pending_count > 0:
            try:
                if timeout is None:
                    await self._pending_empty.wait()
                else:
                    await asyncio.wait_for(self._pending_empty.wait(), timeout)
            except asyncio.TimeoutError:
                drained = False
                self.logger.warning(
                    "Callback queue flush timeout, pending=%s queue=%s delayed=%s",
                    self.pending_count,
                    self._queue.qsize(),
                    len(self._retry_tasks),
                )

        retry_tasks = list(self._retry_tasks)
        for task in retry_tasks:
            task.cancel()
        for task in retry_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        if not drained:
            await self._persist_buffered_requests()

        async with self._worker_lock:
            if self._worker:
                self._worker.cancel()
                try:
                    await self._worker
                except asyncio.CancelledError:
                    pass
                self._worker = None

        if not drained:
            async with self._state_lock:
                dropped = self._pending
                self._pending = 0
                self._pending_empty.set()
            if dropped:
                self.logger.warning("Dropped %s pending callbacks during stop.", dropped)

        return drained

    async def _queue_request(self, request: CallbackRequest) -> None:
        async with self._state_lock:
            self._pending += 1
            self._pending_empty.clear()
        await self._ensure_worker()
        await self._queue.put(request)

    async def _ensure_worker(self) -> None:
        async with self._worker_lock:
            if self._worker and not self._worker.done():
                return
            self._worker = self.loop.create_task(self._worker_loop(), name="pyxxl_callback_worker")

    async def _worker_loop(self) -> None:
        while True:
            request: Optional[CallbackRequest] = await self._queue.get()
            try:
                await self.xxl_client.callback(
                    request.log_id,
                    request.timestamp,
                    code=request.code,
                    msg=request.msg,
                )
                await self._delete_persisted_request(request)
                await self._mark_completed()
                self.logger.debug(
                    "Callback delivered logId=%s attempt=%s",
                    request.log_id,
                    request.attempts + 1,
                )
            except asyncio.CancelledError:
                # Persist the current item before exit so shutdown does not lose
                # completion state for already-finished jobs.
                if request is not None:
                    await self._persist_request(request)
                raise
            except Exception as err:  # pylint: disable=broad-except
                request.attempts += 1
                await self._persist_request(request)
                self.logger.warning(
                    "Callback failed logId=%s attempt=%s retry_in=%ss error=%s",
                    request.log_id,
                    request.attempts,
                    self.retry_interval,
                    err,
                )
                retry_task = self.loop.create_task(self._requeue_later(request))
                self._retry_tasks.add(retry_task)
                retry_task.add_done_callback(self._retry_tasks.discard)

    async def _requeue_later(self, request: CallbackRequest) -> None:
        await asyncio.sleep(self.retry_interval)
        if self._closed:
            return
        await self._ensure_worker()
        await self._queue.put(request)

    async def _mark_completed(self) -> None:
        async with self._state_lock:
            self._pending = max(0, self._pending - 1)
            if self._pending == 0:
                self._pending_empty.set()

    async def _replay_persisted_requests(self) -> None:
        pending_files = sorted(self.store_dir.glob(self.CALLBACK_FILE_GLOB))
        if not pending_files:
            return

        loaded = 0
        for path in pending_files:
            request = self._load_request_from_file(path)
            if request is None:
                path.unlink(missing_ok=True)
                continue
            await self._queue_request(request)
            loaded += 1

        if loaded:
            self.logger.info("Replayed %s persisted callbacks from %s", loaded, self.store_dir)

    def _load_request_from_file(self, path: Path) -> Optional[CallbackRequest]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as err:
            self.logger.warning("Load persisted callback failed path=%s error=%s", path, err)
            return None

        return CallbackRequest(
            log_id=payload["log_id"],
            timestamp=payload["timestamp"],
            code=payload["code"],
            msg=payload.get("msg"),
            attempts=payload.get("attempts", 0),
            persisted_path=path.as_posix(),
        )

    async def _persist_buffered_requests(self) -> None:
        while not self._queue.empty():
            request = self._queue.get_nowait()
            self._queue.task_done()
            await self._persist_request(request)

    async def _persist_request(self, request: CallbackRequest) -> None:
        if request.persisted_path:
            path = Path(request.persisted_path)
        else:
            # Include log_id + timestamp for human inspection and a UUID for
            # collision-free retries across process restarts.
            path = self.store_dir / f"callback-{request.log_id}-{request.timestamp}-{uuid4().hex}.json"
            request.persisted_path = path.as_posix()

        try:
            path.write_text(json.dumps(asdict(request), ensure_ascii=True), encoding="utf-8")
        except OSError as err:
            self.logger.error("Persist callback failed path=%s error=%s", path, err)

    async def _delete_persisted_request(self, request: CallbackRequest) -> None:
        if not request.persisted_path:
            return

        try:
            Path(request.persisted_path).unlink(missing_ok=True)
        except OSError as err:
            self.logger.warning("Delete persisted callback failed path=%s error=%s", request.persisted_path, err)
        finally:
            request.persisted_path = None


class JobHandler:
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._handlers: Dict[str, HandlerInfo] = {}
        self.logger = logger or executor_logger

    def register(
        self, *args: Any, name: Optional[str] = None, replace: bool = False
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        """Register a Python callable as an XXL handler name."""

        def func_wrapper(func: DecoratedCallable) -> DecoratedCallable:
            handler_name = name or func.__name__
            if handler_name in self._handlers and replace is False:
                raise error.JobRegisterError("handler %s already registered." % handler_name)
            handler = HandlerInfo(handler=func)
            if not handler.is_async:
                warnings.warn(
                    "Using the sync method will unknown blocking exception, consider using async method.",
                    SyntaxWarning,
                    stacklevel=2,
                )
            self._handlers[handler_name] = handler
            self.logger.debug("register job %s,is async: %s" % (handler_name, asyncio.iscoroutinefunction(func)))

            return func

        if len(args) == 1:
            return func_wrapper(args[0])

        return func_wrapper

    def get(self, name: str) -> Optional[HandlerInfo]:
        return self._handlers.get(name, None)

    def handlers_info(self) -> List[str]:
        return ["<%s is_async:%s>" % (k, v.is_async) for k, v in self._handlers.items()]


class Executor:
    def __init__(
        self,
        xxl_client: XXL,
        config: ExecutorConfig,
        *,
        handler: Optional[JobHandler] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        logger_factory: Optional[LogBase] = None,
        successed_callback: Optional[Callable] = None,
        failed_callback: Optional[Callable] = None,
    ) -> None:
        """Executor runtime that maps XXL trigger semantics onto asyncio."""

        self.xxl_client = xxl_client
        self.config = config

        self.handler: JobHandler = handler or JobHandler()
        self.loop = loop or asyncio.get_event_loop()
        self.tasks: Dict[int, XXLTask] = {}
        self.queue: Dict[int, asyncio.Queue[RunData]] = defaultdict(
            lambda: asyncio.Queue(maxsize=self.config.task_queue_length)
        )
        self._job_log_ids: Dict[int, set[int]] = defaultdict(set)
        self._cover_replacements: Dict[int, RunData] = {}
        # Isolate scheduling decisions per jobId so unrelated jobs do not block
        # each other, while still keeping each jobId's state transitions atomic.
        self._job_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.thread_pool = ThreadPoolExecutor(
            max_workers=self.config.max_workers,
            thread_name_prefix="pyxxl_pool",
        )
        self.logger_factory = logger_factory or DiskLog(self.config.log_local_dir)
        self.successed_callback = successed_callback or (lambda: 1)
        self.failed_callback = failed_callback or (lambda x: 1)
        self.callback_manager = CallbackManager(
            self.xxl_client,
            self.loop,
            self.executor_logger,
            retry_interval=self.config.http_retry_duration,
            store_dir=Path(self.config.log_local_dir).joinpath(".callback_failures"),
        )
        self.loop.set_default_executor(self.thread_pool)

    @property
    def executor_logger(self) -> logging.Logger:
        return self.config.executor_logger

    def _get_job_lock(self, job_id: int) -> asyncio.Lock:
        """Return the per-job scheduling lock."""
        return self._job_locks[job_id]

    def _get_job_log_ids(self, job_id: int) -> set[int]:
        return self._job_log_ids[job_id]

    def _register_log_id(self, job_id: int, log_id: int) -> None:
        self._get_job_log_ids(job_id).add(log_id)

    def _release_log_id(self, job_id: int, log_id: int) -> None:
        log_ids = self._job_log_ids.get(job_id)
        if not log_ids:
            return
        log_ids.discard(log_id)
        if not log_ids:
            self._job_log_ids.pop(job_id, None)

    def _pop_cover_replacement(self, job_id: int) -> Optional[RunData]:
        return self._cover_replacements.pop(job_id, None)

    async def start_callback_manager(self) -> None:
        await self.callback_manager.start()

    def _create_task(self, data: RunData) -> XXLTask:
        """Create the asyncio task wrapper and attach cleanup hooks."""
        task_info = XXLTask(None, data)
        task = self.loop.create_task(self._run(task_info), name=f"{data.jobId}_{data.logId}")
        task_info.task = task
        task.add_done_callback(
            lambda finished_task, task_info=task_info: self._on_executor_task_done(task_info, finished_task)
        )
        return task_info

    def _on_executor_task_done(self, task_info: XXLTask, finished_task: asyncio.Task) -> None:
        if not finished_task.cancelled() or task_info.started or task_info.cleaned:
            return

        # A task can be cancelled before _run() flips started=True, for example
        # when COVER_EARLY replaces it immediately after scheduling.
        cleanup_task = self.loop.create_task(
            self._cleanup_task(task_info, push_cancel_callback=True),
            name=f"cleanup_{task_info.data.jobId}_{task_info.data.logId}",
        )
        _spawn_task(cleanup_task)

    async def _handle_discard_later(self, data: RunData) -> str:
        """处理DISCARD_LATER策略：丢弃后来的任务"""
        raise error.JobDuplicateError("The same job [%s] is already executing and this has been discarded." % data)

    async def _handle_cover_early(self, data: RunData) -> str:
        """Match Java COVER_EARLY semantics: only the latest trigger should run."""
        reason = "block strategy effect：{}".format(executorBlockStrategy.COVER_EARLY.value)
        msg = "Job {} BlockStrategy is COVER_EARLY, logId {} replaced.".format(data.jobId, data.logId)
        self.executor_logger.warning(msg)
        # Java drains queued triggers as well; they should receive explicit failed
        # callbacks instead of silently disappearing.
        dropped_tasks = self._drain_pending_queue_locked(data.jobId)
        replacement = self._pop_cover_replacement(data.jobId)
        if replacement is not None:
            self._release_log_id(data.jobId, replacement.logId)
            dropped_tasks.append(replacement)

        self._register_log_id(data.jobId, data.logId)
        current_task = self.tasks.get(data.jobId)
        if current_task is not None:
            self._cover_replacements[data.jobId] = data
            current_task.cancel_reason = reason
            current_task.cancel()
        else:
            self.tasks[data.jobId] = self._create_task(data)

        await self._push_failed_queued_tasks(dropped_tasks, reason)
        return msg

    async def _handle_serial_execution(self, data: RunData) -> str:
        """Queue follow-up triggers for the same jobId."""
        queue = self.get_queue(data.jobId)
        if queue.full():
            msg = "Job {job_id} is SERIAL, queue length more than {maxsize}. Job {job} discard!".format(
                job_id=data.jobId, job=data, maxsize=queue.maxsize
            )
            self.executor_logger.error(msg)
            raise error.JobDuplicateError(msg)
        else:
            msg = "job {job_id} is in queue, logId {log_id} ranked {ranked}th [max={maxsize}]...".format(
                job_id=data.jobId, log_id=data.logId, ranked=queue.qsize() + 1, maxsize=queue.maxsize
            )
            self.executor_logger.info(msg)
            await queue.put(data)
            self._register_log_id(data.jobId, data.logId)
            return msg

    async def run_job(self, data: RunData) -> str:
        handler_obj = self.handler.get(data.executorHandler)
        if not handler_obj:
            self.executor_logger.warning("handler %s not found." % data.executorHandler)
            raise error.JobNotFoundError("handler %s not found." % data.executorHandler)

        job_lock = self._get_job_lock(data.jobId)
        async with job_lock:
            # Keep logId reserved until cleanup finishes so duplicate dispatches
            # for the same execution are rejected.
            if data.logId in self._get_job_log_ids(data.jobId):
                raise error.JobDuplicateError("repeate trigger job, logId:%s" % data.logId)

            current_task = self.tasks.get(data.jobId)
            queue = self.get_queue(data.jobId)

            if not current_task and queue.empty() and data.jobId not in self._cover_replacements:
                self._register_log_id(data.jobId, data.logId)
                self.tasks[data.jobId] = self._create_task(data)
                return "Running"

            # 任务冲突，根据阻塞策略处理
            self.executor_logger.warning("jobId {} is running. current_task={}".format(data.jobId, current_task))

            if data.executorBlockStrategy == executorBlockStrategy.DISCARD_LATER.value:
                return await self._handle_discard_later(data)
            elif data.executorBlockStrategy == executorBlockStrategy.COVER_EARLY.value:
                return await self._handle_cover_early(data)
            elif data.executorBlockStrategy == executorBlockStrategy.SERIAL_EXECUTION.value:
                return await self._handle_serial_execution(data)
            else:
                raise error.JobParamsError(
                    "unknown executorBlockStrategy [%s]." % data.executorBlockStrategy,
                    executorBlockStrategy=data.executorBlockStrategy,
                )

    async def cancel_job(
        self, job_id: int, include_queue: bool = True, reason: str = "scheduling center kill job."
    ) -> None:
        await asyncio.sleep(0.01)  # delay for pytest
        self.executor_logger.warning("start kill job: job_id={}".format(job_id))

        job_lock = self._get_job_lock(job_id)
        task_to_cancel = None
        queued_tasks: List[RunData] = []

        async with job_lock:
            if include_queue:
                queued_tasks.extend(self._drain_pending_queue_locked(job_id))
                replacement = self._pop_cover_replacement(job_id)
                if replacement is not None:
                    self._release_log_id(job_id, replacement.logId)
                    queued_tasks.append(replacement)

            task_to_cancel = self.tasks.get(job_id, None)
            if task_to_cancel:
                task_to_cancel.cancel_reason = reason
                task_to_cancel.cancel()

        await self._push_failed_queued_tasks(queued_tasks, reason)

        # Wait outside the per-job lock because task cleanup also needs that lock.
        if task_to_cancel:
            try:
                await task_to_cancel.task
            except asyncio.CancelledError:
                self.executor_logger.warning("Job %s cancelled." % job_id)

    async def is_running(self, job_id: int) -> bool:
        return job_id in self.tasks

    async def is_running_or_has_queue(self, job_id: int) -> bool:
        # BUSYOVER/idleBeat semantics in Java consider both the active thread and
        # its trigger queue. COVER_EARLY replacement is also a pending trigger.
        queue = self.queue.get(job_id)
        return (
            job_id in self.tasks
            or (queue is not None and not queue.empty())
            or job_id in self._cover_replacements
        )

    def _callback_timestamp(self, data: RunData) -> int:
        return data.logDateTime or int(time.time() * 1000)

    def _cancel_message_for_task(self, task_info: XXLTask) -> str:
        if task_info.cancel_reason:
            return "{} [job running, killed.]".format(task_info.cancel_reason)
        return "CancelledError"

    def _drain_pending_queue_locked(self, job_id: int) -> List[RunData]:
        queue = self.get_queue(job_id)
        drained: List[RunData] = []
        while not queue.empty():
            data = queue.get_nowait()
            self.executor_logger.warning("Discard jobId {} from queue, data: {}".format(job_id, data))
            drained.append(data)
            self._release_log_id(job_id, data.logId)
            queue.task_done()
        return drained

    async def _push_failed_queued_tasks(self, pending_tasks: List[RunData], reason: str) -> None:
        for data in pending_tasks:
            await self._push_callback(
                data.logId,
                self._callback_timestamp(data),
                code=500,
                msg="{} [job not executed, in the job queue, killed.]".format(reason),
            )

    async def _cleanup_task(self, task_info: XXLTask, push_cancel_callback: bool = False) -> None:
        if task_info.cleaned:
            return

        task_info.cleaned = True
        data = task_info.data
        if push_cancel_callback:
            # This path is used for tasks cancelled before _run() could push its
            # own completion callback.
            await self._push_callback(
                data.logId,
                self._callback_timestamp(data),
                code=500,
                msg=self._cancel_message_for_task(task_info),
            )
            self.failed_callback("cancelled")

        job_lock = self._get_job_lock(data.jobId)
        async with job_lock:
            await self._finish(task_info)

    async def _run(self, task_info: XXLTask) -> None:
        task_info.started = True
        data = task_info.data
        handler = self.handler.get(data.executorHandler)
        assert handler
        g.set_xxl_run_data(data)

        with new_logger(self.logger_factory, data.logId) as task_logger:
            callback_timestamp = self._callback_timestamp(data)
            try:
                task_logger.info("Start job jobId=%s logId=%s [%s]" % (data.jobId, data.logId, data))
                timeout = data.executorTimeout or self.config.task_timeout
                result = await handler.start(timeout)
                task_logger.info("Job finished jobId=%s logId=%s" % (data.jobId, data.logId))
                await self._push_callback(data.logId, callback_timestamp, code=200, msg=result)
                self.successed_callback()
            except asyncio.CancelledError as e:
                task_logger.info(e, exc_info=True)
                await self._push_callback(
                    data.logId,
                    callback_timestamp,
                    code=500,
                    msg=self._cancel_message_for_task(task_info),
                )
                self.failed_callback("cancelled")
            except asyncio.exceptions.TimeoutError as e:
                # Sync handlers still run inside the thread pool after timeout.
                # This remains a gap versus Java interrupt-based cancellation.
                task_logger.warning(e, exc_info=True)
                await self._push_callback(data.logId, callback_timestamp, code=500, msg="TimeoutError")
                self.failed_callback("timeout")
            except Exception as err:  # pylint: disable=broad-except
                task_logger.exception(err, exc_info=True)
                await self._push_callback(data.logId, callback_timestamp, code=500, msg=str(err))
                self.failed_callback("exception")
            finally:
                await self._cleanup_task(task_info)

    async def _push_callback(self, log_id: int, timestamp: int, code: int, msg: Optional[str] = None) -> None:
        await self.callback_manager.enqueue(
            CallbackRequest(log_id=log_id, timestamp=timestamp, code=code, msg=msg)
        )

    async def _finish(self, finish_task: XXLTask) -> None:
        job_id = finish_task.data.jobId
        current_task = self.tasks.get(job_id)
        if current_task is finish_task:
            self.tasks.pop(job_id, None)
        self.executor_logger.info("Finish task %s", finish_task)
        self._release_log_id(job_id, finish_task.data.logId)

        if current_task is not finish_task:
            return

        # COVER_EARLY replacement should run before ordinary queued SERIAL work so
        # the latest trigger wins deterministically.
        replacement = self._pop_cover_replacement(job_id)
        if replacement is not None:
            self.executor_logger.info("Start cover replacement jobId=%s logId=%s", job_id, replacement.logId)
            self.tasks[job_id] = self._create_task(replacement)
            return

        queue = self.get_queue(job_id)
        if not queue.empty():
            data = queue.get_nowait()
            self.executor_logger.info(
                "Get data from queue jobId={}, after queueSize={}, data={}".format(job_id, queue.qsize(), data)
            )
            self.tasks[job_id] = self._create_task(data)
            queue.task_done()

    async def shutdown(self) -> None:
        """Cancel running and queued work immediately."""
        await asyncio.sleep(0.01)  # sleep for pytest
        job_ids = set(self.tasks.keys()) | set(self.queue.keys()) | set(self._cover_replacements.keys())
        await asyncio.gather(
            *(self.cancel_job(job_id, include_queue=True, reason="executor shutdown.") for job_id in job_ids)
        )

    async def graceful_close(self, timeout: int = 60) -> None:
        """Wait for in-flight work, then flush pending callbacks within timeout."""
        await asyncio.sleep(0.01)  # sleep for pytest
        start = self.loop.time()

        async def _graceful_close() -> None:
            while (
                len(self.tasks) > 0
                or any(i.qsize() > 0 for i in self.queue.values())
                or len(self._cover_replacements) > 0
            ):
                pending_tasks = [i.task for i in self.tasks.values() if i.task is not None]
                if pending_tasks:
                    await asyncio.wait(pending_tasks)
                await asyncio.sleep(0.05)

        await asyncio.wait_for(_graceful_close(), timeout=timeout)
        remaining = max(timeout - (self.loop.time() - start), 0.01)
        await self.stop_callback_manager(timeout=remaining)

    async def stop_callback_manager(self, timeout: Optional[float] = None, close: bool = False) -> bool:
        return await self.callback_manager.stop(timeout, close=close)

    def reset_handler(self, handler: Optional[JobHandler] = None) -> None:
        self.handler = handler or JobHandler()

    def get_queue(self, job_id: int) -> asyncio.Queue[RunData]:
        return self.queue[job_id]
