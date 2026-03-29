import asyncio
import logging
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import aiohttp

from pyxxl.error import XXLClientError
from pyxxl.log import xxl_client_logger

JsonType = Union[None, int, str, bool, List[Any], Dict[Any, Any]]


class Response:
    """xxl-job-admin JSON 响应的轻量封装。"""

    def __init__(self, code: int, msg: Optional[str] = None, **kwargs: Any) -> None:
        self.code = code
        self.msg = msg

    @property
    def ok(self) -> bool:
        return self.code == 200


class XXL:
    """执行器访问 xxl-job-admin OpenAPI 的客户端。"""

    def __init__(
        self,
        admin_url: Union[str, List[str]],
        token: Optional[str] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        retry_times: int = 1,
        retry_duration: int = 5,
        http_timeout: int = 10,
        session: Optional[aiohttp.ClientSession] = None,
        logger: Optional[logging.Logger] = None,
        **kwargs: Any,
    ) -> None:
        self.loop = loop or asyncio.get_event_loop()
        kwargs["loop"] = self.loop

        self.admin_urls = self._normalize_admin_urls(admin_url)
        self._relative_url_path = urlparse(self.admin_urls[0]).path
        self._use_relative_url = session is not None and len(self.admin_urls) == 1

        if session is None:
            self.conn = aiohttp.TCPConnector(**kwargs)
            session = aiohttp.ClientSession(
                connector=self.conn,
                trust_env=True,
            )

        self.session = session
        self.retry_times = retry_times
        self.retry_duration = retry_duration
        self.headers = {"XXL-JOB-ACCESS-TOKEN": token, "XXL-RPC-ACCESS-TOKEN": token} if token else {}
        self.logger = logger or xxl_client_logger
        self.http_timeout = http_timeout

    def _normalize_admin_urls(self, admin_url: Union[str, List[str]]) -> List[str]:
        raw_urls = admin_url.split(",") if isinstance(admin_url, str) else admin_url
        normalized_urls = [self._normalize_admin_url(url) for url in raw_urls if url.strip()]
        if not normalized_urls:
            raise ValueError("admin_url must like http://localhost:8080/xxl-job-admin/api/")
        return normalized_urls

    def _normalize_admin_url(self, admin_url: str) -> str:
        """兼容用户直接拷贝的根地址、`/api` 地址和 `/api/` 地址。"""

        parsed = urlparse(admin_url.strip())
        if not parsed.scheme.startswith("http"):
            raise ValueError("admin_url must like http://localhost:8080/xxl-job-admin/api/")

        path = parsed.path.rstrip("/")
        if path.endswith("/api"):
            normalized_path = f"{path}/"
        else:
            normalized_path = f"{path}/api/" if path else "/api/"
        return parsed._replace(path=normalized_path).geturl()

    async def registry(self, key: str, value: str) -> bool:
        payload = {"registryGroup": "EXECUTOR", "registryKey": key, "registryValue": value}
        try:
            await self._post("registry", payload, retry_times=5)
            return True
        except XXLClientError as err:
            self.logger.error("Registry executor failed. %s", err.message)
        return False

    async def registryRemove(self, key: str, value: str) -> None:
        payload = {"registryGroup": "EXECUTOR", "registryKey": key, "registryValue": value}
        await self._post("registryRemove", payload, retry_times=3)
        self.logger.info("RegistryRemove successful. %s", payload)

    async def callback(self, log_id: int, timestamp: int, code: int = 200, msg: Optional[str] = None) -> None:
        """
        上报任务回调结果。

        这里保留 `executeResult`，用于兼容仍然读取旧字段的 2.x admin 版本。
        """

        payload = [
            {
                "logId": log_id,
                "logDateTim": timestamp,
                "handleCode": code,
                "handleMsg": msg,
                "executeResult": {"code": code, "msg": msg},
            }
        ]
        await self._post("callback", payload)
        self.logger.debug("Callback successful. %s", payload)

    async def _post_once(self, admin_url: str, path: str, payload: JsonType) -> Response:
        request_url = self._relative_url_path + path if self._use_relative_url else admin_url + path
        async with self.session.post(request_url, json=payload, headers=self.headers, timeout=self.http_timeout) as response:
            if response.status == 200:
                response_data = Response(**(await response.json()))
                if not response_data.ok:
                    raise XXLClientError(response_data.msg or "")
                return response_data
            raise XXLClientError(await response.text())

    async def _post(self, path: str, payload: JsonType, retry_times: Optional[int] = None) -> Response:
        self.logger.debug("post to xxl-job admins=%s path=%s payload=%s", self.admin_urls, path, payload)
        retry_times = retry_times or self.retry_times
        last_error: Optional[XXLClientError] = None

        for times in range(1, retry_times + 1):
            had_retryable_error = False
            for admin_url in self.admin_urls:
                try:
                    return await self._post_once(admin_url, path, payload)
                except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as err:
                    had_retryable_error = True
                    last_error = XXLClientError(f"{admin_url} {err}")
                    self.logger.warning(
                        "Connection error address=%s attempt=%s/%s retry_in=%ss error=%s",
                        admin_url,
                        times,
                        retry_times,
                        self.retry_duration,
                        err,
                    )
                except XXLClientError as err:
                    last_error = XXLClientError(f"{admin_url} {err.message}")
                    self.logger.warning("Request failed address=%s path=%s error=%s", admin_url, path, err.message)

            if had_retryable_error and times < retry_times:
                await asyncio.sleep(self.retry_duration)
                continue
            break

        raise last_error or XXLClientError("Connection error after retry times 0")

    async def close(self) -> None:
        await self.session.close()
        self.logger.info("http session is closed.")
