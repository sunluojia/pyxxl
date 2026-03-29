import asyncio

import pytest
from aiohttp.test_utils import TestClient

from pyxxl import ExecutorConfig
from pyxxl.tests.conftest import GLOBAL_CONFIG
from pyxxl.tests.utils import MokePyxxlRunner
from pyxxl.utils import try_import

from .test_server import send_demoJobHandler


@pytest.mark.asyncio
@pytest.mark.skipif(not try_import("prometheus_client"), reason="不存在prometheus_client")
async def test_metrics(cli: TestClient):
    for _ in range(3):
        await send_demoJobHandler(cli, jobId=630)
        await send_demoJobHandler(cli, jobId=631, executorHandler="demoJobHandlerSync")
        await asyncio.sleep(0.01)
    await asyncio.sleep(1)
    resp = await cli.get("/metrics")
    assert resp.status == 200
    assert "python_gc_objects_collected_total" in await resp.text()


@pytest.mark.asyncio
@pytest.mark.skipif(not try_import("prometheus_client"), reason="不存在prometheus_client")
async def test_metrics_counters(aiohttp_client) -> None:
    # The Prometheus wrapper must chain user execution outcomes into counters.
    config = ExecutorConfig(**GLOBAL_CONFIG)
    runner = MokePyxxlRunner(config)

    @runner.register(name="demoJobHandler")
    async def success_task() -> None:
        await asyncio.sleep(0.01)

    @runner.register(name="demoJobHandlerError")
    async def failed_task() -> None:
        raise ValueError("metrics failure")

    cli = await aiohttp_client(runner.create_server_app())
    await send_demoJobHandler(cli, jobId=7700, logId=7701)
    await send_demoJobHandler(cli, jobId=7702, logId=7703, executorHandler="demoJobHandlerError")
    await asyncio.sleep(0.2)

    resp = await cli.get("/metrics")
    body = await resp.text()
    assert 'success_total{jobId="7700"} 1.0' in body
    assert 'failed_total{jobId="7702",reason="exception"} 1.0' in body
