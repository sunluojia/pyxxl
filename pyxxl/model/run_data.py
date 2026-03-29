from dataclasses import dataclass, fields
from typing import Optional


@dataclass(frozen=True)
class RunData:
    """调度中心传入执行器的任务载荷。"""

    jobId: int
    logId: int
    executorHandler: str
    executorBlockStrategy: str

    executorParams: Optional[str] = None
    executorTimeout: Optional[int] = None
    logDateTime: Optional[int] = None
    glueType: Optional[str] = None
    glueSource: Optional[str] = None
    glueUpdatetime: Optional[int] = None
    broadcastIndex: Optional[int] = None
    broadcastTotal: Optional[int] = None

    @classmethod
    def from_dict(cls, data: dict) -> "RunData":
        """反序列化调度请求，并忽略不同版本 admin 多出来的字段。"""

        class_fields = {field.name for field in fields(cls)}
        return cls(**{key: value for key, value in data.items() if key in class_fields})
