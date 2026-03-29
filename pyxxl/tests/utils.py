import os
from typing import Any, Dict, Optional

from pyxxl.error import XXLClientError
from pyxxl import JsonType, PyxxlRunner, Response, XXL
from pyxxl.utils import try_import


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
    def _get_xxl_client(self) -> MokeXXL:
        return MokeXXL(self.config.admin_baseurls, token=self.config.access_token)


REDIS_TEST_URI = os.environ.get("REDIS_TEST_URI", "redis://localhost")
INSTALL_REDIS = bool(try_import("redis"))


def _check_redis_server() -> bool:
    if not INSTALL_REDIS:
        return False

    redis = try_import("redis")
    assert redis is not None
    try:
        client = redis.Redis.from_url(REDIS_TEST_URI)
        client.ping()
        return True
    except redis.RedisError:
        return False


REDIS_SERVER_AVAILABLE = _check_redis_server()
ENABLE_REDIS_TESTS = INSTALL_REDIS and REDIS_SERVER_AVAILABLE
