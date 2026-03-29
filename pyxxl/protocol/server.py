import logging
from typing import TYPE_CHECKING, Optional

from aiohttp import web

from pyxxl import error
from pyxxl.model import RunData
from pyxxl.runtime import Executor
from pyxxl.utils import try_import

if TYPE_CHECKING:
    from pyxxl.logger import LogBase


routes = web.RouteTableDef()
ACCESS_TOKEN_HEADER = "XXL-JOB-ACCESS-TOKEN"


def app_logger(request: web.Request) -> logging.Logger:
    return request.app["pyxxl_state"].executor_logger


def app_executor(request: web.Request) -> Executor:
    return request.app["pyxxl_state"].executor


def validate_access_token(request: web.Request) -> Optional[web.Response]:
    """把 token 校验失败也包装成 XXL 约定的 JSON 响应。"""

    access_token = app_executor(request).config.access_token
    if access_token and request.headers.get(ACCESS_TOKEN_HEADER) != access_token:
        app_logger(request).warning("Invalid access token for %s", request.path)
        return web.json_response({"code": 500, "msg": "The access token is wrong."})
    return None


@routes.post("/beat")
async def beat(request: web.Request) -> web.Response:
    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response

    app_logger(request).debug("beat")
    return web.json_response({"code": 200, "msg": None})


@routes.post("/idleBeat")
async def idle_beat(request: web.Request) -> web.Response:
    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response

    data = await request.json()
    job_id = data["jobId"]
    app_logger(request).debug("idleBeat: %s", data)
    if await app_executor(request).is_running_or_has_queue(job_id):
        return web.json_response({"code": 500, "msg": "job thread is running or has trigger queue."})
    return web.json_response({"code": 200, "msg": None})


@routes.post("/run")
async def run(request: web.Request) -> web.Response:
    """处理调度中心发来的执行请求。"""

    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response

    data = await request.json()
    run_data = RunData.from_dict(data)
    app_logger(request).info("Get task request. jobId=%s logId=%s [%s]", run_data.jobId, run_data.logId, run_data)
    try:
        msg = await app_executor(request).run_job(run_data)
    except error.JobDuplicateError as err:
        return web.json_response({"code": 500, "msg": err.message})
    except error.JobNotFoundError as err:
        return web.json_response({"code": 500, "msg": err.message})

    return web.json_response({"code": 200, "msg": msg})


@routes.post("/kill")
async def kill(request: web.Request) -> web.Response:
    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response

    data = await request.json()
    await app_executor(request).cancel_job(data["jobId"], include_queue=True)
    return web.json_response({"code": 200, "msg": None})


@routes.post("/log")
async def log(request: web.Request) -> web.Response:
    """按 xxl-job-admin 需要的格式返回分页日志。"""

    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response

    data = await request.json()
    app_logger(request).debug("get log request %s", data)
    task_log: LogBase = request.app["pyxxl_state"].task_log
    response = {
        "code": 200,
        "msg": None,
        "content": await task_log.get_logs(data),
    }
    response["data"] = response["content"]
    return web.json_response(response)


def create_app() -> web.Application:
    """创建 aiohttp 应用，并按需挂载 `/metrics`。"""

    app = web.Application()
    app.add_routes(routes)
    if try_import("prometheus_client"):
        from pyxxl.monitoring import mount_app

        mount_app(app)
    return app
