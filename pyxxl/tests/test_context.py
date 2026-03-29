import pytest

from pyxxl import Executor, ExecutorBlockStrategy, RunData, g


@pytest.mark.asyncio
async def test_runner_callback(executor: Executor):
    executor.reset_handler()
    executor.xxl_client.clear_result()

    @executor.handler.register
    async def test_ctx():
        logId = g.xxl_run_data.logId
        assert logId == 1

    @executor.handler.register
    def test_ctx_sync():
        logId = g.xxl_run_data.logId
        assert logId == 1

    for handler in ["test_ctx", "test_ctx_sync"]:
        data = RunData.from_dict(
            dict(
                logId=1,
                jobId=11,
                executorHandler=handler,
                executorBlockStrategy=ExecutorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
        await executor.run_job(data)
        await executor.graceful_close()
        assert executor.xxl_client.callback_result.get(1) == 200
