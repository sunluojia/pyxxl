import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, is_dataclass
from typing import Any

from aiohttp import web
from prometheus_client import Counter, Gauge, Info
from prometheus_client.exposition import _bake_output
from prometheus_client.registry import REGISTRY

from pyxxl.context import g
from pyxxl.runtime import Executor

FAILED_COUNTER = Counter("failed", "task failed number.", ["jobId", "reason"])
SUCCESS_COUNTER = Counter("success", "task success number.", ["jobId"])

RUNNING_TASKS = Gauge("running_tasks", "running tasks")
QUEUE_TASKS = Gauge("queue_tasks", "queue_tasks", ["jobId"])
ASYNCIO_TASKS_TOTAL = Gauge("asyncio_tasks_total", "ASYNCIO_TASKS_TOTAL")

RUNNING_TASK_INFO = Info("running_task", "running task info", ["pk"])
QUEUE_TASKS_INFO = Info("queue_task", "queue task info", ["pk"])
THREAD_POOL_INFO = Info("executor_thread_pool", "executor_thread_pool")

routes = web.RouteTableDef()


def success() -> None:
    """记录任务成功次数。"""

    SUCCESS_COUNTER.labels(g.xxl_run_data.jobId).inc(1)


def failed(reason: str) -> None:
    """按失败原因记录任务失败次数。"""

    FAILED_COUNTER.labels(g.xxl_run_data.jobId, reason).inc(1)


def as_str_dict(obj: Any) -> dict:
    """Prometheus `Info` 只接受字符串值，这里统一做一次转换。"""

    if is_dataclass(obj):
        obj = asdict(obj)  # type: ignore[arg-type]
    return {key: str(value) for key, value in obj.items()}


def _get_thread_pool_info(pool: ThreadPoolExecutor) -> dict:
    """导出线程池即时状态，便于观察同步任务拥塞情况。"""

    data = {
        "wait_qsize": str(pool._work_queue.qsize()),
        "current_threads": str(len(pool._threads)),
        "max_workers": str(pool._max_workers),
        "idle_threads": str(pool._idle_semaphore._value),  # type: ignore[attr-defined]
    }
    return data


@routes.get("/metrics")
async def metrics(request: web.Request) -> web.Response:
    """实时构造 Prometheus 指标输出。"""

    RUNNING_TASK_INFO.clear()
    QUEUE_TASKS_INFO.clear()
    ASYNCIO_TASKS_TOTAL.set(len(asyncio.all_tasks()))

    executor: Executor = request.app["pyxxl_state"].executor
    RUNNING_TASKS.set(len(executor.tasks))

    for key, value in executor.tasks.items():
        RUNNING_TASK_INFO.labels(key).info(as_str_dict(value.data))

    for key, queue in executor.queue.items():
        QUEUE_TASKS.labels(key).set(queue.qsize())
        QUEUE_TASKS_INFO.labels(key).info({"detail": str(queue)})

    THREAD_POOL_INFO.info(_get_thread_pool_info(executor.thread_pool))

    _, headers, output = _bake_output(REGISTRY, "", "", request.query, True)
    return web.Response(body=output, headers=headers)


def mount_app(app: web.Application) -> None:
    """把 `/metrics` 路由挂到 aiohttp 应用上。"""

    app.add_routes(routes)
