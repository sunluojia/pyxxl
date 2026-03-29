from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import MutableSet, Optional
from uuid import uuid4

from pyxxl.protocol.admin_client import XXL


@dataclass
class CallbackRequest:
    """待投递到 admin 的 callback 请求。"""

    log_id: int
    timestamp: int
    code: int
    msg: Optional[str] = None
    attempts: int = 0
    persisted_path: Optional[str] = None


class CallbackManager:
    """对齐 Java `TriggerCallbackThread` 的 callback 队列、重试和补偿逻辑。"""

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
            return CallbackRequest(
                log_id=payload["log_id"],
                timestamp=payload["timestamp"],
                code=payload["code"],
                msg=payload.get("msg"),
                attempts=payload.get("attempts", 0),
                persisted_path=path.as_posix(),
            )
        except (KeyError, TypeError, json.JSONDecodeError, OSError) as err:
            self.logger.warning("Load persisted callback failed path=%s error=%s", path, err)
            return None

    async def _persist_buffered_requests(self) -> None:
        while not self._queue.empty():
            request = self._queue.get_nowait()
            self._queue.task_done()
            await self._persist_request(request)

    async def _persist_request(self, request: CallbackRequest) -> None:
        if request.persisted_path:
            path = Path(request.persisted_path)
        else:
            path = self.store_dir / f"callback-{request.log_id}-{request.timestamp}-{uuid4().hex}.json"
            request.persisted_path = path.as_posix()

        try:
            path.write_text(json.dumps(asdict(request), ensure_ascii=False), encoding="utf-8")
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
