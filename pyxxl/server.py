import logging
from typing import TYPE_CHECKING, Optional

from aiohttp import web

from pyxxl import error
from pyxxl.executor import Executor
from pyxxl.schema import RunData
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
    """Return an XXL-style auth failure instead of a framework exception."""
    access_token = app_executor(request).config.access_token
    if access_token and request.headers.get(ACCESS_TOKEN_HEADER) != access_token:
        app_logger(request).warning("Invalid access token for %s", request.path)
        return web.json_response(dict(code=500, msg="The access token is wrong."))
    return None


@routes.post("/beat")
async def beat(request: web.Request) -> web.Response:
    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response
    app_logger(request).debug("beat")
    return web.json_response(dict(code=200, msg=None))


@routes.post("/idleBeat")
async def idle_beat(request: web.Request) -> web.Response:
    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response
    data = await request.json()
    job_id = data["jobId"]
    app_logger(request).debug("idleBeat: %s" % data)
    # Java idleBeat reports busy when the job thread is running or when queued
    # triggers already exist for the same jobId.
    if await app_executor(request).is_running_or_has_queue(job_id):
        return web.json_response(dict(code=500, msg="job thread is running or has trigger queue."))
    return web.json_response(dict(code=200, msg=None))


@routes.post("/run")
async def run(request: web.Request) -> web.Response:
    """Handle executor openapi run requests from xxl-job-admin.

    {
    "jobId":1,                             // 任务ID
    "executorHandler":"demoJobHandler",    // 任务标识
    "executorParams":"demoJobHandler",     // 任务参数
    "executorBlockStrategy":"COVER_EARLY", // 任务阻塞策略，可选值参考 com.xxl.job.core.enums.ExecutorBlockStrategyEnum
    "executorTimeout":0,                   // 任务超时时间，单位秒，大于零时生效
    "logId":1,                             // 本次调度日志ID
    "logDateTime":1586629003729,           // 本次调度日志时间
    "glueType":"BEAN",                     // 任务模式，可选值参考 com.xxl.job.core.glue.GlueTypeEnum
    "glueSource":"xxx",                    // GLUE脚本代码
    "glueUpdatetime":1586629003727,        // GLUE脚本更新时间，用于判定脚本是否变更以及是否需要刷新
    "broadcastIndex":0,                    // 分片参数：当前分片
    "broadcastTotal":0                     // 分片参数：总分片
    }
    """
    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response
    data = await request.json()
    run_data = RunData.from_dict(data)
    app_logger(request).info("Get task request. jobId=%s logId=%s [%s]" % (run_data.jobId, run_data.logId, run_data))
    try:
        msg = await app_executor(request).run_job(run_data)
    except error.JobDuplicateError as e:
        return web.json_response(dict(code=500, msg=e.message))
    except error.JobNotFoundError as e:
        return web.json_response(dict(code=500, msg=e.message))

    return web.json_response(dict(code=200, msg=msg))


@routes.post("/kill")
async def kill(request: web.Request) -> web.Response:
    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response
    data = await request.json()
    await app_executor(request).cancel_job(data["jobId"], include_queue=True)
    return web.json_response(dict(code=200, msg=None))


@routes.post("/log")
async def log(request: web.Request) -> web.Response:
    """Return paged task logs in both pre-3.3 and 3.3+ response shapes.

        {
        "logDateTim":0,     // 本次调度日志时间
        "logId":0,          // 本次调度日志ID
        "fromLineNum":0     // 日志开始行号，滚动加载日志
    }
    """
    invalid_response = validate_access_token(request)
    if invalid_response is not None:
        return invalid_response
    data = await request.json()
    app_logger(request).debug("get log request %s" % data)
    task_log: LogBase = request.app["pyxxl_state"].task_log
    response = {
        "code": 200,
        "msg": None,
        "content": await task_log.get_logs(data),
    }
    response["data"] = response["content"]  # v3.3.0 changed response format, keep both keys for compatibility.
    return web.json_response(response)


def create_app() -> web.Application:
    """Create the aiohttp app with optional Prometheus endpoint mounting."""
    app = web.Application()
    app.add_routes(routes)
    if try_import("prometheus_client"):
        from pyxxl.prometheus import mount_app

        mount_app(app)

    return app
