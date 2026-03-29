import asyncio
import time

import pytest
from aiohttp.test_utils import TestClient
from pytest_aiohttp.plugin import AiohttpClient

from pyxxl import ExecutorConfig
from pyxxl.schema import RunData
from pyxxl.tests.conftest import GLOBAL_CONFIG
from pyxxl.tests.utils import MokePyxxlRunner


async def send_demoJobHandler(cli: TestClient, headers=None, **kwargs):
    job_data = {
        "jobId": int(time.time() * 1000),
        "executorHandler": "demoJobHandler",
        "executorParams": "demoJobHandler",
        "executorBlockStrategy": "COVER_EARLY",
        "executorTimeout": 0,
        "logId": int(time.time() * 1000),
        "logDateTime": 1586629003729,
        "glueType": "BEAN",
        "glueSource": "xxx",
        "glueUpdatetime": 1586629003727,
        "broadcastIndex": 0,
        "broadcastTotal": 0,
    }
    job_data.update(kwargs)
    resp = await cli.post("/run", json=job_data, headers=headers)
    return resp, job_data["jobId"]


@pytest.fixture
async def cli_with_token(aiohttp_client: AiohttpClient) -> TestClient:
    config = ExecutorConfig(**GLOBAL_CONFIG, access_token="token-test")
    runner = MokePyxxlRunner(config)

    @runner.register(name="demoJobHandler")
    async def test_task() -> None:
        await asyncio.sleep(0.01)

    return await aiohttp_client(runner.create_server_app())


@pytest.mark.asyncio
async def test_run(cli: TestClient):
    resp, _ = await send_demoJobHandler(cli, executorBlockStrategy="DISCARD_LATER", jobId=100)
    assert resp.status == 200
    assert await resp.json() == {"code": 200, "msg": "Running"}
    # error
    resp, _ = await send_demoJobHandler(cli, executorBlockStrategy="DISCARD_LATER", jobId=100)
    assert resp.status == 200
    response_dict = await resp.json()
    assert response_dict["code"] == 500
    assert "already executing" in response_dict["msg"]


@pytest.mark.asyncio
async def test_run_not_found(cli: TestClient):
    resp, _ = await send_demoJobHandler(cli, executorHandler="test_run_not_found")
    assert resp.status == 200
    response_dict = await resp.json()
    assert response_dict["code"] == 500
    assert "not found" in response_dict["msg"]


@pytest.mark.asyncio
async def test_beat(cli: TestClient):
    resp = await cli.post("/beat")
    assert resp.status == 200
    assert await resp.json() == {"code": 200, "msg": None}


@pytest.mark.asyncio
async def test_idle_beat(cli: TestClient):
    resp = await cli.post("/idleBeat", json={"jobId": 1})
    assert resp.status == 200
    assert await resp.json() == {"code": 200, "msg": None}

    resp, jobId = await send_demoJobHandler(cli, jobId=300, executorBlockStrategy="SERIAL_EXECUTION", logId=301)
    resp, jobId = await send_demoJobHandler(cli, jobId=300, executorBlockStrategy="SERIAL_EXECUTION", logId=302)
    resp = await cli.post("/idleBeat", json={"jobId": jobId})
    response_data = await resp.json()
    assert response_data["code"] == 500
    assert response_data["msg"] == "job thread is running or has trigger queue."

    executor = cli.server.app["pyxxl_state"].executor
    queue_only_job_id = 333
    # Queue-only state should still be reported as busy to match Java idleBeat.
    await executor.get_queue(queue_only_job_id).put(
        RunData(
            jobId=queue_only_job_id,
            logId=334,
            executorHandler="demoJobHandler",
            executorBlockStrategy="SERIAL_EXECUTION",
        )
    )
    resp = await cli.post("/idleBeat", json={"jobId": queue_only_job_id})
    assert await resp.json() == {"code": 500, "msg": "job thread is running or has trigger queue."}
    await executor.cancel_job(jobId, include_queue=True, reason="pytest cleanup.")
    await executor.cancel_job(queue_only_job_id, include_queue=True, reason="pytest cleanup.")
    await executor.stop_callback_manager(timeout=5)


@pytest.mark.asyncio
async def test_kill(cli: TestClient):
    resp, jobId = await send_demoJobHandler(cli)
    resp = await cli.post("/kill", json={"jobId": jobId})
    assert await resp.json() == {"code": 200, "msg": None}


@pytest.mark.asyncio
async def test_run_duplicate_log_id(cli: TestClient):
    # The executor endpoint should reject duplicate triggers before a second run
    # is created for the same logId.
    job_payload = {
        "jobId": 401,
        "executorHandler": "demoJobHandler",
        "executorParams": "demoJobHandler",
        "executorBlockStrategy": "SERIAL_EXECUTION",
        "executorTimeout": 0,
        "logId": 402,
        "logDateTime": 1586629003729,
        "glueType": "BEAN",
        "glueSource": "xxx",
        "glueUpdatetime": 1586629003727,
        "broadcastIndex": 0,
        "broadcastTotal": 0,
    }

    resp = await cli.post("/run", json=job_payload)
    assert await resp.json() == {"code": 200, "msg": "Running"}

    resp = await cli.post("/run", json=job_payload)
    response_data = await resp.json()
    assert response_data["code"] == 500
    assert "repeate trigger job" in response_data["msg"]

    executor = cli.server.app["pyxxl_state"].executor
    await executor.cancel_job(job_payload["jobId"], include_queue=True, reason="pytest cleanup.")
    await executor.stop_callback_manager(timeout=5)


@pytest.mark.asyncio
async def test_log(cli: TestClient):
    resp, jobId = await send_demoJobHandler(cli)
    resp = await cli.post(
        "/log",
        json={
            "logId": jobId,
            "fromLineNum": 1,
        },
    )
    assert (await resp.json())["code"] == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "payload"),
    [
        ("/beat", None),
        ("/idleBeat", {"jobId": 1}),
        (
            "/run",
            {
                "jobId": 1,
                "executorHandler": "demoJobHandler",
                "executorParams": "demoJobHandler",
                "executorBlockStrategy": "DISCARD_LATER",
                "executorTimeout": 0,
                "logId": 1,
                "logDateTime": 1586629003729,
                "glueType": "BEAN",
                "glueSource": "xxx",
                "glueUpdatetime": 1586629003727,
                "broadcastIndex": 0,
                "broadcastTotal": 0,
            },
        ),
        ("/kill", {"jobId": 1}),
        ("/log", {"logId": 1, "fromLineNum": 1}),
    ],
)
async def test_access_token_rejected(cli_with_token: TestClient, path: str, payload):
    # Executor routes return XXL-style JSON failures instead of HTTP 401/403 so
    # admin keeps its expected parsing behavior.
    resp = await cli_with_token.post(path, json=payload)
    assert resp.status == 200
    assert await resp.json() == {"code": 500, "msg": "The access token is wrong."}

    resp = await cli_with_token.post(path, json=payload, headers={"XXL-JOB-ACCESS-TOKEN": "wrong-token"})
    assert resp.status == 200
    assert await resp.json() == {"code": 500, "msg": "The access token is wrong."}


@pytest.mark.asyncio
async def test_access_token_accepted(cli_with_token: TestClient):
    headers = {"XXL-JOB-ACCESS-TOKEN": "token-test"}

    resp = await cli_with_token.post("/beat", headers=headers)
    assert resp.status == 200
    assert await resp.json() == {"code": 200, "msg": None}

    resp, _ = await send_demoJobHandler(
        cli_with_token,
        headers=headers,
        executorBlockStrategy="DISCARD_LATER",
        jobId=101,
    )
    assert resp.status == 200
    assert await resp.json() == {"code": 200, "msg": "Running"}
