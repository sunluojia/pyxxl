import asyncio
from typing import MutableSet

_BACKGROUND_TASKS: MutableSet[asyncio.Task] = set()


def keep_asyncio_task(task: asyncio.Task) -> None:
    """保存后台任务引用，避免 fire-and-forget 任务被过早回收。"""

    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
