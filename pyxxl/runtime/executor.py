from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Dict, List, Optional

from pyxxl import error
from pyxxl.config import ExecutorConfig
from pyxxl.context import g
from pyxxl.logger import DiskLog, LogBase, new_logger
from pyxxl.model import ExecutorBlockStrategy, RunData
from pyxxl.protocol.admin_client import XXL
from pyxxl.runtime.background import keep_asyncio_task
from pyxxl.runtime.callbacks import CallbackManager, CallbackRequest
from pyxxl.runtime.handlers import JobHandler
from pyxxl.runtime.models import XXLTask


class Executor:
    """XXL-JOB Python 执行器核心运行时。"""

    def __init__(
        self,
        xxl_client: XXL,
        config: ExecutorConfig,
        *,
        handler: Optional[JobHandler] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        logger_factory: Optional[LogBase] = None,
        succeeded_callback: Optional[Callable[[], None]] = None,
        failed_callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.xxl_client = xxl_client
        self.config = config
        self.handler = handler or JobHandler()
        self.loop = loop or asyncio.get_event_loop()

        self.tasks: Dict[int, XXLTask] = {}
        self.queue: Dict[int, asyncio.Queue[RunData]] = defaultdict(
            lambda: asyncio.Queue(maxsize=self.config.task_queue_length)
        )
        self._job_log_ids: Dict[int, set[int]] = defaultdict(set)
        self._cover_replacements: Dict[int, RunData] = {}
        self._job_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

        self.thread_pool = ThreadPoolExecutor(
            max_workers=self.config.max_workers,
            thread_name_prefix="pyxxl_pool",
        )
        self.logger_factory = logger_factory or DiskLog(self.config.log_local_dir)
        self.succeeded_callback = succeeded_callback or (lambda: None)
        self.failed_callback = failed_callback or (lambda reason: None)
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
        """返回单个 `jobId` 对应的调度锁，避免不同任务互相阻塞。"""

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
        """创建协程任务并挂上清理钩子。"""

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

        cleanup_task = self.loop.create_task(
            self._cleanup_task(task_info, push_cancel_callback=True),
            name=f"cleanup_{task_info.data.jobId}_{task_info.data.logId}",
        )
        keep_asyncio_task(cleanup_task)

    async def _handle_discard_later(self, data: RunData) -> str:
        raise error.JobDuplicateError("The same job [%s] is already executing and this has been discarded." % data)

    async def _handle_cover_early(self, data: RunData) -> str:
        """对齐 Java 的 `COVER_EARLY`：只保留最新一次触发。"""

        reason = f"block strategy effect：{ExecutorBlockStrategy.COVER_EARLY.value}"
        msg = "Job {} BlockStrategy is COVER_EARLY, logId {} replaced.".format(data.jobId, data.logId)
        self.executor_logger.warning(msg)

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
        """对齐 Java 的 `SERIAL_EXECUTION`：相同 `jobId` 进入本地队列。"""

        queue = self.get_queue(data.jobId)
        if queue.full():
            msg = "Job {job_id} is SERIAL, queue length more than {maxsize}. Job {job} discard!".format(
                job_id=data.jobId,
                job=data,
                maxsize=queue.maxsize,
            )
            self.executor_logger.error(msg)
            raise error.JobDuplicateError(msg)

        msg = "job {job_id} is in queue, logId {log_id} ranked {ranked}th [max={maxsize}]...".format(
            job_id=data.jobId,
            log_id=data.logId,
            ranked=queue.qsize() + 1,
            maxsize=queue.maxsize,
        )
        self.executor_logger.info(msg)
        await queue.put(data)
        self._register_log_id(data.jobId, data.logId)
        return msg

    async def run_job(self, data: RunData) -> str:
        handler_obj = self.handler.get(data.executorHandler)
        if handler_obj is None:
            self.executor_logger.warning("handler %s not found.", data.executorHandler)
            raise error.JobNotFoundError("handler %s not found." % data.executorHandler)

        job_lock = self._get_job_lock(data.jobId)
        async with job_lock:
            if data.logId in self._get_job_log_ids(data.jobId):
                raise error.JobDuplicateError("repeate trigger job, logId:%s" % data.logId)

            current_task = self.tasks.get(data.jobId)
            queue = self.get_queue(data.jobId)

            if not current_task and queue.empty() and data.jobId not in self._cover_replacements:
                self._register_log_id(data.jobId, data.logId)
                self.tasks[data.jobId] = self._create_task(data)
                return "Running"

            self.executor_logger.warning("jobId %s is running. current_task=%s", data.jobId, current_task)

            if data.executorBlockStrategy == ExecutorBlockStrategy.DISCARD_LATER.value:
                return await self._handle_discard_later(data)
            if data.executorBlockStrategy == ExecutorBlockStrategy.COVER_EARLY.value:
                return await self._handle_cover_early(data)
            if data.executorBlockStrategy == ExecutorBlockStrategy.SERIAL_EXECUTION.value:
                return await self._handle_serial_execution(data)

            raise error.JobParamsError(
                "unknown executorBlockStrategy [%s]." % data.executorBlockStrategy,
                executorBlockStrategy=data.executorBlockStrategy,
            )

    async def cancel_job(
        self,
        job_id: int,
        include_queue: bool = True,
        reason: str = "scheduling center kill job.",
    ) -> None:
        await asyncio.sleep(0.01)
        self.executor_logger.warning("start kill job: job_id=%s", job_id)

        job_lock = self._get_job_lock(job_id)
        task_to_cancel: Optional[XXLTask] = None
        queued_tasks: List[RunData] = []

        async with job_lock:
            if include_queue:
                queued_tasks.extend(self._drain_pending_queue_locked(job_id))
                replacement = self._pop_cover_replacement(job_id)
                if replacement is not None:
                    self._release_log_id(job_id, replacement.logId)
                    queued_tasks.append(replacement)

            task_to_cancel = self.tasks.get(job_id)
            if task_to_cancel:
                task_to_cancel.cancel_reason = reason
                task_to_cancel.cancel()

        await self._push_failed_queued_tasks(queued_tasks, reason)

        if task_to_cancel:
            try:
                assert task_to_cancel.task is not None
                await task_to_cancel.task
            except asyncio.CancelledError:
                self.executor_logger.warning("Job %s cancelled.", job_id)

    async def is_running(self, job_id: int) -> bool:
        return job_id in self.tasks

    async def is_running_or_has_queue(self, job_id: int) -> bool:
        """对齐 Java `idleBeat` 的 busy 语义：运行中、排队中、待替换都算忙。"""

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
            self.executor_logger.warning("Discard jobId %s from queue, data: %s", job_id, data)
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
        assert handler is not None
        g.set_xxl_run_data(data)

        with new_logger(self.logger_factory, data.logId) as task_logger:
            callback_timestamp = self._callback_timestamp(data)
            try:
                task_logger.info("Start job jobId=%s logId=%s [%s]", data.jobId, data.logId, data)
                timeout = data.executorTimeout or self.config.task_timeout
                result = await handler.start(timeout, data, self.config)
                task_logger.info("Job finished jobId=%s logId=%s", data.jobId, data.logId)
                await self._push_callback(data.logId, callback_timestamp, code=200, msg=result)
                self.succeeded_callback()
            except asyncio.CancelledError as err:
                task_logger.info(err, exc_info=True)
                await self._push_callback(
                    data.logId,
                    callback_timestamp,
                    code=500,
                    msg=self._cancel_message_for_task(task_info),
                )
                self.failed_callback("cancelled")
            except asyncio.TimeoutError as err:
                task_logger.warning(err, exc_info=True)
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

        replacement = self._pop_cover_replacement(job_id)
        if replacement is not None:
            self.executor_logger.info("Start cover replacement jobId=%s logId=%s", job_id, replacement.logId)
            self.tasks[job_id] = self._create_task(replacement)
            return

        queue = self.get_queue(job_id)
        if not queue.empty():
            data = queue.get_nowait()
            self.executor_logger.info(
                "Get data from queue jobId=%s, after queueSize=%s, data=%s",
                job_id,
                queue.qsize(),
                data,
            )
            self.tasks[job_id] = self._create_task(data)
            queue.task_done()

    async def shutdown(self) -> None:
        """立即取消所有运行中和排队中的任务。"""

        await asyncio.sleep(0.01)
        job_ids = set(self.tasks.keys()) | set(self.queue.keys()) | set(self._cover_replacements.keys())
        await asyncio.gather(
            *(self.cancel_job(job_id, include_queue=True, reason="executor shutdown.") for job_id in job_ids)
        )

    async def graceful_close(self, timeout: int = 60) -> None:
        """等待运行中任务结束，并在剩余时间里尽量把 callback 队列刷完。"""

        await asyncio.sleep(0.01)
        start = self.loop.time()

        async def _graceful_close() -> None:
            while (
                len(self.tasks) > 0
                or any(queue.qsize() > 0 for queue in self.queue.values())
                or len(self._cover_replacements) > 0
            ):
                pending_tasks = [item.task for item in self.tasks.values() if item.task is not None]
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
