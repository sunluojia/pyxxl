import inspect
import logging
import os
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional, get_origin
from urllib.parse import urlparse

from pyxxl.log import executor_logger, setting_logger
from pyxxl.utils import get_network_ip, setup_logging


@dataclass
class ExecutorConfig:
    """
    Python XXL-JOB 执行器配置。

    如果安装了 `python-dotenv`，默认会尝试从 `.env` 文件读取同名配置项。
    环境变量优先级高于构造函数传参，例如 `access_token` 会优先读取
    `access_token` / `ACCESS_TOKEN`。
    """

    xxl_admin_baseurl: str
    """xxl-job-admin 的地址，支持单地址、逗号分隔多地址，以及根路径或 `/api` 路径。"""

    executor_app_name: str
    """执行器在 xxl-job-admin 中配置的 `AppName`，必须保持一致。"""

    access_token: Optional[str] = None
    """调度中心和执行器之间的访问令牌。"""

    executor_url: str = field(default="")
    """
    执行器向 admin 上报的可访问地址。

    默认值为 `http://{executor_listen_host}:{executor_listen_port}`。
    如果执行器前面还有 Nginx、SLB 或端口映射，应填写 admin 真正可访问的地址。
    """

    executor_listen_port: int = 9999
    """执行器 HTTP 服务监听端口。"""

    executor_listen_host: str = ""
    """执行器 HTTP 服务监听地址；为空时自动取本机网卡地址。"""

    executor_log_path: str = "pyxxl.log"
    """执行器自身日志输出文件。"""

    executor_logger: logging.Logger = field(default=None)  # type: ignore[assignment]
    """执行器内部日志对象。任务日志仍然走 `logger` 子系统。"""

    max_workers: int = 30
    """同步任务所使用的线程池大小。"""

    task_timeout: int = 60 * 10
    """任务默认超时时间，单位秒；若调度中心下发 `executorTimeout` 则优先使用后者。"""

    task_queue_length: int = 30
    """单个 `jobId` 的本地排队长度。"""

    graceful_close: bool = False
    """是否在关闭时等待运行中任务自然结束。"""

    graceful_timeout: int = 60 * 5
    """优雅关闭最长等待时间，单位秒。"""

    log_target: Literal["disk", "redis"] = "disk"
    """任务日志存储后端。"""

    log_local_dir: str = "logs"
    """磁盘日志目录。"""

    log_redis_uri: str = ""
    """Redis 日志后端连接地址。"""

    log_expired_days: float = 14
    """任务日志保留天数。"""

    log_clean_interval: int = 3600
    """扫描并清理过期日志的时间间隔，单位秒。"""

    http_retry_times: int = 3
    """访问 admin 的 HTTP 重试次数。"""

    http_retry_duration: int = 5
    """访问 admin 的 HTTP 重试间隔，单位秒。"""

    http_timeout: int = 30
    """访问 admin 的 HTTP 超时时间，单位秒。"""

    dotenv_try: bool = True
    """是否尝试从 `.env` 文件读取配置。"""

    dotenv_path: Optional[str] = None
    """`.env` 文件路径，默认为当前目录下的 `.env`。"""

    debug: bool = False
    """是否启用调试级别日志。"""

    def __post_init__(self) -> None:
        setup_logging(self.executor_log_path, __name__, level=logging.DEBUG)
        if self.dotenv_try:
            self._try_load_from_dotenv()

        self._validate_admin_baseurl()
        self._validate_executor_app_name()
        self._validate_logger_target()

        if not self.executor_listen_host:
            self.executor_listen_host = get_network_ip()

        if not self.executor_url:
            self.executor_url = f"http://{self.executor_listen_host}:{self.executor_listen_port}"

        if self.executor_logger is None:
            self.executor_logger = executor_logger
            setup_logging(
                self.executor_log_path,
                executor_logger.name,
                level=logging.DEBUG if self.debug else logging.INFO,
            )

        setting_logger.debug("init config: %s", asdict(self))

    def _try_load_from_dotenv(self) -> None:
        try:
            from dotenv import load_dotenv

            load_dotenv(self.dotenv_path)
        except ImportError:  # pragma: no cover
            pass

        for parameter in inspect.signature(ExecutorConfig).parameters.values():
            env_val = os.getenv(parameter.name) or os.getenv(parameter.name.upper())
            if env_val is None:
                continue

            setting_logger.info("Get [%s] config from env.", parameter.name)
            real_value: Any = env_val
            if parameter.annotation is bool:
                real_value = env_val in ["true", "True"]
            elif get_origin(parameter.annotation) is None:
                real_value = parameter.annotation(env_val)
            setattr(self, parameter.name, real_value)

    def _validate_admin_baseurl(self) -> None:
        admin_urls = [self._normalize_admin_baseurl(url) for url in self._split_admin_baseurls(self.xxl_admin_baseurl)]
        if not admin_urls:
            raise ValueError("admin_url must like http://localhost:8080/xxl-job-admin/api/")
        self.xxl_admin_baseurl = ",".join(admin_urls)

    def _split_admin_baseurls(self, raw_value: str) -> list[str]:
        return [url.strip() for url in raw_value.split(",") if url.strip()]

    def _normalize_admin_baseurl(self, raw_value: str) -> str:
        """把 admin 地址统一规范到 `/api/` 结尾，减少运行时分支判断。"""

        admin_url = urlparse(raw_value.strip())
        if not admin_url.scheme.startswith("http"):
            raise ValueError("admin_url must like http://localhost:8080/xxl-job-admin/api/")

        path = admin_url.path.rstrip("/")
        if path.endswith("/api"):
            normalized_path = f"{path}/"
        else:
            normalized_path = f"{path}/api/" if path else "/api/"
        return admin_url._replace(path=normalized_path).geturl()

    def _validate_executor_app_name(self) -> None:
        if not self.executor_app_name:
            raise ValueError("executor_app_name is required.")

    def _validate_logger_target(self) -> None:
        if self.log_target == "disk" and not self.log_local_dir:
            raise ValueError("log_target 'disk' config item 'log_local_dir' is necessary.")

        if self.log_target == "redis" and not self.log_redis_uri:
            raise ValueError("log_target 'redis' config item 'log_redis_uri' is necessary.")

    @property
    def executor_baseurl(self) -> str:
        """返回上报给 admin 的执行器访问地址。"""

        return self.executor_url

    @property
    def admin_baseurls(self) -> list[str]:
        """按故障转移顺序返回已规范化的 admin 地址列表。"""

        return self._split_admin_baseurls(self.xxl_admin_baseurl)
