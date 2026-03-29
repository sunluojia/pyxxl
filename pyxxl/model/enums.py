from enum import Enum


class ExecutorBlockStrategy(Enum):
    """XXL-JOB 调度中心下发的阻塞策略。"""

    SERIAL_EXECUTION = "SERIAL_EXECUTION"
    DISCARD_LATER = "DISCARD_LATER"
    COVER_EARLY = "COVER_EARLY"


class HandlerRunMode(Enum):
    """Python 任务处理函数的执行模式。"""

    ASYNC = "async"
    THREAD = "thread"
    PROCESS = "process"
