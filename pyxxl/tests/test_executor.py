import asyncio
import time
from pathlib import Path
from typing import Iterator

import pytest

from pyxxl import CallbackRequest, Executor, ExecutorBlockStrategy, ExecutorConfig, JobHandler, RunData, g
from pyxxl.error import JobDuplicateError, JobNotFoundError, JobParamsError
from pyxxl.tests.conftest import GLOBAL_CONFIG
from pyxxl.tests.utils import MokeXXL

executorBlockStrategy = ExecutorBlockStrategy

job_handler = JobHandler()
TASK_SLEEP_SECONDS = 2


@job_handler.register
async def pytest_executor_async():
    await asyncio.sleep(TASK_SLEEP_SECONDS)
    return "成功30"


@job_handler.register
def pytest_executor_sync():
    time.sleep(TASK_SLEEP_SECONDS)
    return "成功30"


@job_handler.register
async def pytest_executor_error():
    assert 1 == 2


@job_handler.register_process(name="pytest_executor_process")
def pytest_executor_process() -> str:
    return f"process:{g.xxl_run_data.executorParams}"


@job_handler.register_process(name="pytest_executor_process_blocking")
def pytest_executor_process_blocking() -> str:
    while not g.cancel_event.is_set():
        time.sleep(0.2)
    return "process-stopped"


HANDLER_NAMES = [
    "pytest_executor_async",
    "pytest_executor_sync",
]


@pytest.mark.asyncio
async def test_runner_not_found(executor: Executor, job_id: int, log_id: int):
    executor.reset_handler(job_handler)
    with pytest.raises(JobNotFoundError):
        await executor.run_job(
            RunData.from_dict(
                dict(
                    logId=log_id,
                    jobId=job_id,
                    executorHandler="not_found",
                    executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
                    errorTest="errorTest",
                )
            )
        )
    await executor.graceful_close()


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_callback(executor: Executor, handler_name: str):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    data = RunData.from_dict(
        dict(
            logId=1,
            jobId=11,
            executorHandler=handler_name,
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )
    await executor.run_job(data)
    data = RunData.from_dict(
        dict(
            logId=2,
            jobId=12,
            executorHandler="pytest_executor_error",
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )
    await executor.run_job(data)
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(1) == 200
    assert executor.xxl_client.callback_result.get(2) == 500
    assert executor.xxl_client.callback_result.get(3) is None


@pytest.mark.asyncio
async def test_runner_callback_retry(executor: Executor, job_id: int, log_id: int):
    # callback 应在后台重试，并在 admin 恢复后最终成功。
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    executor.callback_manager.retry_interval = 0.3
    executor.xxl_client.set_callback_failures(log_id, 1)

    await executor.run_job(
        RunData(
            logId=log_id,
            jobId=job_id,
            executorHandler="pytest_executor_async",
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )

    await asyncio.sleep(TASK_SLEEP_SECONDS + 0.1)
    assert not await executor.is_running(job_id)
    assert executor.xxl_client.callback_result.get(log_id) is None

    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(log_id) == 200
    assert executor.xxl_client.callback_attempts.get(log_id) == 2


@pytest.mark.asyncio
async def test_runner_callback_replay_from_persisted_file(tmp_path: Path, log_id: int):
    # 持久化 callback 记录必须能跨进程重启恢复，对齐 Java 的补偿语义。
    config = ExecutorConfig(**GLOBAL_CONFIG, log_local_dir=tmp_path.as_posix())
    callback_store = tmp_path.joinpath(".callback_failures")

    first_client = MokeXXL("http://localhost:8080/xxl-job-admin/api/")
    first_executor = Executor(first_client, config, handler=job_handler)
    await first_executor.start_callback_manager()
    first_executor.callback_manager.retry_interval = 30
    first_client.set_callback_failures(log_id, 1)

    await first_executor.callback_manager.enqueue(
        CallbackRequest(log_id=log_id, timestamp=123456789, code=200, msg="persist-me")
    )
    await asyncio.sleep(0.1)
    await first_executor.stop_callback_manager(timeout=0.01, close=True)
    await first_executor.xxl_client.close()

    persisted_files = list(callback_store.glob("callback-*.json"))
    assert len(persisted_files) == 1

    second_client = MokeXXL("http://localhost:8080/xxl-job-admin/api/")
    second_executor = Executor(second_client, config, handler=job_handler)
    await second_executor.start_callback_manager()
    await second_executor.stop_callback_manager(timeout=5)

    assert second_client.callback_result.get(log_id) == 200
    assert second_client.callback_messages.get(log_id) == "persist-me"
    assert not list(callback_store.glob("callback-*.json"))
    await second_executor.stop_callback_manager(timeout=1, close=True)
    await second_executor.xxl_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_cancel(executor: Executor, handler_name: str):
    executor.reset_handler(job_handler)
    cancel_job_id, ok_job_id = 1100, 1200
    cancel_log_id, ok_log_id = 1100, 1200
    base_data = dict(
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    await executor.run_job(RunData(logId=cancel_log_id, jobId=cancel_job_id, **base_data))
    await executor.run_job(RunData(logId=ok_log_id, jobId=ok_job_id, **base_data))

    await executor.cancel_job(cancel_job_id, include_queue=False)
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(cancel_job_id) == 500
    assert executor.xxl_client.callback_result.get(ok_job_id) == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_cancel_include_queue(
    executor: Executor, handler_name: str, job_id: int, log_id_iter: Iterator[int]
):
    executor.reset_handler(job_handler)
    base_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    cancel_log_id, queue_log_id = next(log_id_iter), next(log_id_iter)
    await executor.run_job(RunData(logId=cancel_log_id, **base_data))
    await executor.run_job(RunData(logId=queue_log_id, **base_data))
    await executor.cancel_job(job_id, include_queue=True)
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(cancel_log_id) == 500
    assert executor.xxl_client.callback_result.get(queue_log_id) == 500
    assert "job not executed" in executor.xxl_client.callback_messages.get(queue_log_id)


@pytest.mark.asyncio
async def test_runner_shutdown_include_queue_callback(executor: Executor, job_id: int, log_id_iter: Iterator[int]):
    # shutdown 要把状态机彻底收口：运行中和排队中的任务都必须收到终态 callback。
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    running_log_id, queue_log_id = next(log_id_iter), next(log_id_iter)
    base_data = dict(
        jobId=job_id,
        executorHandler=HANDLER_NAMES[0],
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )

    await executor.run_job(RunData(logId=running_log_id, **base_data))
    await executor.run_job(RunData(logId=queue_log_id, **base_data))

    await executor.shutdown()
    await executor.stop_callback_manager(timeout=10)

    assert executor.xxl_client.callback_result.get(running_log_id) == 500
    assert executor.xxl_client.callback_result.get(queue_log_id) == 500
    assert "executor shutdown." in executor.xxl_client.callback_messages.get(running_log_id)
    assert "job not executed" in executor.xxl_client.callback_messages.get(queue_log_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_SERIAL_EXECUTION(executor: Executor, job_id: int, handler_name: str, log_id_iter: Iterator[int]):
    executor.xxl_client.clear_result()
    executor.reset_handler(job_handler)
    run_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    queue_size = 3
    log_ids = [next(log_id_iter) for _ in range(queue_size)]
    for log_id in log_ids:
        await executor.run_job(RunData(logId=log_id, **run_data))

    assert executor.queue.get(job_id).qsize() == queue_size - 1
    await executor.graceful_close(10)
    assert executor.queue.get(job_id).qsize() == 0
    assert executor.xxl_client.callback_result.get(log_id) == 200

    # 队列长度达到上限后应拒绝继续入队
    for _ in range(executor.config.task_queue_length + 1):
        await executor.run_job(RunData(logId=next(log_id_iter), **run_data))

    with pytest.raises(JobDuplicateError, match="discard"):
        await executor.run_job(RunData(logId=next(log_id_iter), **run_data))

    await executor.shutdown()


@pytest.mark.asyncio
async def test_runner_duplicate_log_id_dedup(executor: Executor, job_id: int, log_id: int):
    # 同一个 jobId + logId 必须视为同一次执行并直接拒绝。
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    run_data = RunData(
        logId=log_id,
        jobId=job_id,
        executorHandler=HANDLER_NAMES[0],
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )

    await executor.run_job(run_data)
    with pytest.raises(JobDuplicateError, match="repeate trigger job"):
        await executor.run_job(run_data)

    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(log_id) == 200
    assert executor.xxl_client.callback_attempts.get(log_id) == 1


@pytest.mark.asyncio
async def test_runner_process_mode_success(executor: Executor, job_id: int, log_id: int):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()

    await executor.run_job(
        RunData(
            logId=log_id,
            jobId=job_id,
            executorHandler="pytest_executor_process",
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            executorParams="process-ok",
        )
    )

    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(log_id) == 200
    assert executor.xxl_client.callback_messages.get(log_id) == "process:process-ok"


@pytest.mark.asyncio
async def test_runner_process_mode_timeout(executor: Executor, job_id: int, log_id: int):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()

    await executor.run_job(
        RunData(
            logId=log_id,
            jobId=job_id,
            executorHandler="pytest_executor_process_blocking",
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            executorTimeout=1,
        )
    )

    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(log_id) == 500
    assert executor.xxl_client.callback_messages.get(log_id) == "TimeoutError"


@pytest.mark.asyncio
async def test_runner_process_mode_cancel(executor: Executor, job_id: int, log_id: int):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()

    await executor.run_job(
        RunData(
            logId=log_id,
            jobId=job_id,
            executorHandler="pytest_executor_process_blocking",
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )
    await asyncio.sleep(0.2)

    await executor.cancel_job(job_id, include_queue=False)
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(log_id) == 500
    assert "scheduling center kill job." in executor.xxl_client.callback_messages.get(log_id)


@pytest.mark.asyncio
async def test_is_running_or_has_queue(executor: Executor, job_id: int, log_id: int):
    # idleBeat/BUSYOVER 语义必须感知队列，而不只是感知运行中任务。
    executor.reset_handler(job_handler)
    queued = RunData(
        logId=log_id,
        jobId=job_id,
        executorHandler=HANDLER_NAMES[0],
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )

    await executor.get_queue(job_id).put(queued)
    assert not await executor.is_running(job_id)
    assert await executor.is_running_or_has_queue(job_id)

    await executor.cancel_job(job_id, include_queue=True, reason="pytest cleanup.")
    await executor.stop_callback_manager(timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_DISCARD_LATER(executor: Executor, job_id: int, handler_name: str, log_id_iter: Iterator[int]):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    run_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
    )
    ok_log_id, duplicate_log_id = next(log_id_iter), next(log_id_iter)
    await executor.run_job(RunData(logId=ok_log_id, **run_data))
    with pytest.raises(JobDuplicateError):
        await executor.run_job(RunData(logId=duplicate_log_id, **run_data))
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(ok_log_id) == 200
    assert executor.xxl_client.callback_result.get(duplicate_log_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_COVER_EARLY(executor: Executor, job_id: int, handler_name: str, log_id_iter: Iterator[int]):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    run_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
    )
    ok_log_id, error_log_id = next(log_id_iter), next(log_id_iter)
    await executor.run_job(RunData(logId=error_log_id, **run_data))
    await executor.run_job(RunData(logId=ok_log_id, **run_data))
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(ok_log_id) == 200
    assert executor.xxl_client.callback_result.get(error_log_id) == 500


@pytest.mark.asyncio
async def test_runner_cover_early_replaces_queue(executor: Executor, job_id: int, log_id_iter: Iterator[int]):
    # COVER_EARLY 应取消当前运行、清空排队任务，并只保留最后一次触发作为替换项。
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    running_log_id, queued_log_id, cover_log_id = [next(log_id_iter) for _ in range(3)]

    await executor.run_job(
        RunData(
            logId=running_log_id,
            jobId=job_id,
            executorHandler="pytest_executor_async",
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )
    await executor.run_job(
        RunData(
            logId=queued_log_id,
            jobId=job_id,
            executorHandler="pytest_executor_async",
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )
    await executor.run_job(
        RunData(
            logId=cover_log_id,
            jobId=job_id,
            executorHandler="pytest_executor_async",
            executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
        )
    )

    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(running_log_id) == 500
    assert executor.xxl_client.callback_result.get(queued_log_id) == 500
    assert executor.xxl_client.callback_result.get(cover_log_id) == 200
    assert "block strategy effect" in executor.xxl_client.callback_messages.get(running_log_id)
    assert "job not executed" in executor.xxl_client.callback_messages.get(queued_log_id)


@pytest.mark.asyncio
async def test_runner_cover_early_only_latest_runs(executor: Executor, job_id: int, log_id_iter: Iterator[int]):
    # 多次 COVER_EARLY 触发最终应折叠成最后一次执行。
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    first_log_id, middle_log_id, last_log_id = [next(log_id_iter) for _ in range(3)]

    await executor.run_job(
        RunData(
            logId=first_log_id,
            jobId=job_id,
            executorHandler="pytest_executor_async",
            executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
        )
    )
    await executor.run_job(
        RunData(
            logId=middle_log_id,
            jobId=job_id,
            executorHandler="pytest_executor_async",
            executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
        )
    )
    await executor.run_job(
        RunData(
            logId=last_log_id,
            jobId=job_id,
            executorHandler="pytest_executor_async",
            executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
        )
    )

    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(first_log_id) == 500
    assert executor.xxl_client.callback_result.get(middle_log_id) == 500
    assert executor.xxl_client.callback_result.get(last_log_id) == 200
    assert "block strategy effect" in executor.xxl_client.callback_messages.get(first_log_id)
    assert "job not executed" in executor.xxl_client.callback_messages.get(middle_log_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_OTHER(executor: Executor, job_id: int, handler_name: str, log_id_iter: Iterator[int]):
    executor.reset_handler(job_handler)
    with pytest.raises(JobParamsError, match="unknown executorBlockStrategy"):
        for _ in range(2):
            await executor.run_job(
                RunData(
                    logId=next(log_id_iter),
                    jobId=job_id,
                    executorHandler=handler_name,
                    executorBlockStrategy="OTHER",
                )
            )
    executor.xxl_client.clear_result()


@pytest.mark.asyncio
async def test_sync_timeout(executor: Executor, job_id: int, log_id: int):
    sync_handler = JobHandler()

    @sync_handler.register(name="pytest_executor_sync")
    def pytest_executor_sync():
        while not g.cancel_event.is_set():
            time.sleep(1)

    executor.reset_handler(sync_handler)
    await executor.run_job(
        RunData(
            logId=log_id,
            jobId=job_id,
            executorHandler="pytest_executor_sync",
            executorBlockStrategy="OTHER",
            executorTimeout=2,
        )
    )
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(log_id) == 500


@pytest.mark.asyncio
async def test_many_jobs_running(executor: Executor, job_id: int, log_id_iter: Iterator[int]):
    """不同 jobId 应在各自的锁内独立调度。"""
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()

    task1 = RunData(
        logId=next(log_id_iter),
        jobId=job_id,
        executorHandler=HANDLER_NAMES[0],
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    task2 = RunData(
        logId=next(log_id_iter),
        jobId=job_id + 10086,
        executorHandler=HANDLER_NAMES[1],
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    start = time.time()
    await asyncio.gather(
        executor.run_job(task1),
        executor.run_job(task2),
    )
    await executor.graceful_close(20)
    duration = time.time() - start
    print(duration)
    assert duration < TASK_SLEEP_SECONDS * 2, f"任务执行时间过长，可能存在串行执行的情况，duration={duration}"

    assert executor.xxl_client.callback_result.get(task1.logId) == 200
    assert executor.xxl_client.callback_result.get(task2.logId) == 200
