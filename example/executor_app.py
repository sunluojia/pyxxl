import asyncio
import time

from pyxxl import ExecutorConfig, PyxxlRunner, g

# 如果 xxl-admin 可以直接访问执行器所在机器，可以不显式填写 executor_url。
config = ExecutorConfig(
    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
    executor_app_name="xxl-job-executor-sample",
    executor_listen_host="127.0.0.1",  # 监听地址默认会自动探测，这里为了本地调试显式指定。
    debug=True,
)

app = PyxxlRunner(config)


@app.register(name="demoJobHandler")
async def test_task():
    # 任务执行期上下文统一从 g 获取。
    g.logger.info("get executor params: %s" % g.xxl_run_data.executorParams)
    for i in range(10):
        g.logger.warning("test logger %s" % i)
    await asyncio.sleep(5)
    return "成功..."


@app.register(name="asyncTask")
async def test_task3():
    await asyncio.sleep(3)
    return "成功3"


@app.register(name="syncThreadTask", mode="thread")
def test_task4():
    # 如果要在 xxl-admin 上看到任务日志，请使用 g.logger。
    n = 1
    g.logger.info("Job %s get executor params: %s" % (g.xxl_run_data.jobId, g.xxl_run_data.executorParams))
    # 线程池任务需要协作式取消，循环内务必检查 g.cancel_event。
    while n <= 10 and not g.cancel_event.is_set():
        # 如果不需要在 admin 查看日志，也可以使用你自己的 logger。
        g.logger.info(
            "log to {} logger test_task4.{},params:{}".format(
                g.xxl_run_data.jobId,
                n,
                g.xxl_run_data.executorParams,
            )
        )
        time.sleep(2)
        n += 1
    return "成功3"


@app.register(name="processTask", mode="process")
def test_task5():
    g.logger.info("process job start, params=%s", g.xxl_run_data.executorParams)
    for _ in range(10):
        if g.cancel_event.is_set():
            g.logger.warning("process job cancelled")
            return "cancelled"
        time.sleep(1)
    return "成功5"


if __name__ == "__main__":
    app.run_executor()
