import os
from typing import Any, Dict, Optional

from pyxxl.error import XXLClientError
from pyxxl.main import PyxxlRunner
from pyxxl.utils import try_import
from pyxxl.xxl_client import XXL, JsonType, Response


class MokeXXL(XXL):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback_result: Dict[int, Any] = {}
        self.callback_attempts: Dict[int, int] = {}
        self.callback_failures: Dict[int, int] = {}
        self.callback_messages: Dict[int, Any] = {}
        self.callback_timestamps: Dict[int, int] = {}

    async def callback(self, log_id: int, timestamp: int, code: int = 200, msg: Optional[str] = None) -> None:
        self.callback_attempts[log_id] = self.callback_attempts.get(log_id, 0) + 1
        remaining_failures = self.callback_failures.get(log_id, 0)
        if remaining_failures > 0:
            self.callback_failures[log_id] = remaining_failures - 1
            raise XXLClientError(f"mock callback failure for logId={log_id}")
        self.callback_result[log_id] = code
        self.callback_messages[log_id] = msg
        self.callback_timestamps[log_id] = timestamp

    async def _post(self, path: str, payload: JsonType, retry_times: Optional[int] = None) -> Response:
        return Response(code=200)

    def clear_result(self) -> None:
        self.callback_result = {}
        self.callback_attempts = {}
        self.callback_failures = {}
        self.callback_messages = {}
        self.callback_timestamps = {}

    def set_callback_failures(self, log_id: int, times: int) -> None:
        self.callback_failures[log_id] = times


class MokePyxxlRunner(PyxxlRunner):
    def _get_xxl_clint(self) -> MokeXXL:
        return MokeXXL(self.config.admin_baseurls, token=self.config.access_token)


REDIS_TEST_URI = os.environ.get("REDIS_TEST_URI", "redis://localhost")
INSTALL_REDIS = bool(try_import("redis"))
