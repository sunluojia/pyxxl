import pytest

from pyxxl import Executor, ExecutorConfig, HandlerRunMode
from pyxxl.error import JobRegisterError
from pyxxl.tests.conftest import GLOBAL_CONFIG
from pyxxl.tests.utils import MokePyxxlRunner


async def top_level_async_handler() -> str:
    return "async"


def top_level_thread_handler() -> str:
    return "thread"


def top_level_process_handler() -> str:
    return "process"


# pylint: disable=function-redefined
@pytest.mark.asyncio
async def test_hander_error(executor: Executor):
    executor.reset_handler()
    with pytest.raises(JobRegisterError):

        @executor.handler.register
        def test_dup_error(): ...

        @executor.handler.register
        def test_dup_error():  # noqa: F811
            ...


# pylint: disable=function-redefined
@pytest.mark.asyncio
async def test_hander(executor: Executor):
    executor.reset_handler()

    @executor.handler.register
    def test_hander1(): ...

    @executor.handler.register(replace=True)
    async def test_hander1():  # noqa: F811
        ...

    @executor.handler.register(name="test_hander1_dup")
    def test_hander1():  # noqa: F811
        ...


@pytest.mark.asyncio
async def test_handler_register_modes(executor: Executor):
    executor.reset_handler()

    executor.handler.register("async_job", "async")(top_level_async_handler)
    executor.handler.register(name="thread_job", mode="thread")(top_level_thread_handler)
    executor.handler.register("process_job", "process")(top_level_process_handler)

    assert executor.handler.get("async_job").mode == HandlerRunMode.ASYNC
    assert executor.handler.get("thread_job").mode == HandlerRunMode.THREAD
    assert executor.handler.get("process_job").mode == HandlerRunMode.PROCESS


@pytest.mark.asyncio
async def test_handler_register_process_reject_local_function(executor: Executor):
    executor.reset_handler()

    with pytest.raises(JobRegisterError, match="top-level importable function"):

        @executor.handler.register_process(name="local_process")
        def local_process() -> str:
            return "nope"


def test_runner_register_shortcuts() -> None:
    runner = MokePyxxlRunner(ExecutorConfig(**GLOBAL_CONFIG))
    runner.register_thread(name="runner_thread")(top_level_thread_handler)
    runner.register_process(name="runner_process")(top_level_process_handler)

    assert runner.handler.get("runner_thread").mode == HandlerRunMode.THREAD
    assert runner.handler.get("runner_process").mode == HandlerRunMode.PROCESS
