import asyncio
from typing import Any, Optional

from pyxxl.model import RunData


class XXLTask:
    """单个调度触发在执行器中的运行态对象。"""

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
