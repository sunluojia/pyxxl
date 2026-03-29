# XXL-JOB 的 Python 执行器

<p align="center">
<a href="https://pypi.org/project/pyxxl" target="_blank">
    <img src="https://img.shields.io/pypi/v/pyxxl?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/pyxxl" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/pyxxl.svg?color=%2334D058" alt="Supported Python versions">
</a>
<a href="https://pypi.org/project/pyxxl" target="_blank">
    <img src="https://img.shields.io/codecov/c/github/fcfangcc/pyxxl?color=%2334D058" alt="Coverage">
</a>
</p>

`pyxxl` 的定位是“Python 版 XXL-JOB 执行器”。

它负责把 Python 任务函数注册成 `xxl-job-admin` 可调度的 handler，让你们现有的爬虫、采集、ETL、同步脚本统一接入 XXL-JOB 管理。

它不是调度中心，也不负责任务 CRUD 页面和业务编排。这部分仍然应该交给 `xxl-job-admin` 或你们自己的管理后台。

实现方式是对接 XXL-JOB executor 协议，提供与 Java 官方执行器同类的 OpenAPI：

- `/beat`
- `/idleBeat`
- `/run`
- `/kill`
- `/log`

## 当前能力

- 执行器自动注册到 `xxl-job-admin`
- 使用装饰器注册任务函数，接入方式接近 Flask/FastAPI
- 支持 `async`、线程池同步任务、子进程任务三种运行模式
- 支持 `SERIAL_EXECUTION`、`DISCARD_LATER`、`COVER_EARLY`
- 支持 access token 校验
- 支持多 admin 地址顺序 failover
- 支持 callback 异步发送、失败持久化补偿、启动恢复
- 支持 `disk` / `redis` 两种任务日志后端
- 支持 Prometheus `/metrics`
- 支持 kill / shutdown 时对运行中和排队中的任务补发结果
- 支持从 `.env` 读取配置
- 已按“高层入口 / 协议层 / 运行时 / 模型 / 配置”完成内部目录拆分

## 已经测试过的XXL-JOB版本

**3.3.2**,**2.4.0**,**2.3.0**,**2.2.0**

如遇到不兼容的情况请issue告诉我XXL-JOB版本和对应的问题我会尽量适配

## 安装

```shell
pip install pyxxl
```

可选扩展：

```shell
# Redis 日志后端
pip install "pyxxl[redis]"

# 从 .env 加载配置
pip install "pyxxl[dotenv]"

# Prometheus 指标
pip install "pyxxl[metrics]"

# 安装全部扩展
pip install "pyxxl[all]"
```

要求：

- Python `>= 3.9`
- 已有可访问的 `xxl-job-admin`

## 快速开始

```python
import asyncio
import time

from pyxxl import ExecutorConfig, PyxxlRunner, g

config = ExecutorConfig(
    # 支持单地址、逗号分隔多地址，支持填写根地址或 /api 地址
    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin",
    executor_app_name="crawler-python-executor",
    access_token="same-token-as-admin",
    executor_listen_host="127.0.0.1",
    executor_listen_port=9999,
    log_local_dir="logs",
    debug=True,
)

app = PyxxlRunner(config)

@app.register(name="demoJobHandler")
async def demo_job():
    g.logger.info("params=%s", g.xxl_run_data.executorParams)
    await asyncio.sleep(1)
    return "ok"

@app.register(name="syncThreadTask", mode="thread")
def sync_job():
    # 线程池任务无法像 Java 一样被强杀，需要自行检查取消信号。
    for _ in range(3):
        if g.cancel_event.is_set():
            return "cancelled"
        g.logger.info("thread task is running")
        time.sleep(1)
    return "done"

@app.register(name="browserTask", mode="process")
def browser_job():
    for _ in range(10):
        if g.cancel_event.is_set():
            g.logger.warning("process task cancelled")
            return "cancelled"
        time.sleep(1)
    return "ok"


if __name__ == "__main__":
    app.run_executor()
```

启动后，在 `xxl-job-admin` 中创建任务，把 `JobHandler` 填成上面注册的名字，例如：

- `demoJobHandler`
- `syncThreadTask`
- `browserTask`

完整示例见 `example/executor_app.py`。

## 最终推荐的对外 API

普通业务侧建议只用顶层导出，不要依赖内部目录：

```python
from pyxxl import (
    ExecutorBlockStrategy,
    ExecutorConfig,
    HandlerRunMode,
    JobHandler,
    PyxxlRunner,
    RunData,
    g,
)
```

最常用的只有三个：

- `ExecutorConfig`：执行器配置
- `PyxxlRunner`：高层入口，负责启动 HTTP 服务、注册循环、关闭流程
- `g`：运行期上下文，任务里可以读取 `RunData`、日志对象和取消事件

如果你在做框架二开，还可以直接使用：

- `RunData`
- `HandlerRunMode`
- `ExecutorBlockStrategy`
- `JobHandler`
- `Executor`
- `CallbackManager`

## 任务注册方式

当前推荐统一使用一个装饰器，通过 `mode` 指定执行模式：

```python
@app.register
async def default_async_job():
    return "ok"

@app.register(name="syncJob", mode="thread")
def sync_job():
    return "ok"

@app.register(name="processJob", mode="process")
def process_job():
    return "ok"
```

也保留了三个便捷入口：

```python
@app.register_async(name="asyncJob")
async def async_job():
    return "ok"

@app.register_thread(name="threadJob")
def thread_job():
    return "ok"

@app.register_process(name="processJob")
def process_job():
    return "ok"
```

三种模式的选择建议：

- `async`
  适合绝大多数 IO 型爬虫、HTTP 采集、数据库读写、消息处理任务，优先推荐。
- `thread`
  适合短小同步函数，或者你能主动检查 `g.cancel_event` 的阻塞逻辑。
- `process`
  适合浏览器驱动、长阻塞采集、第三方库无法协作取消的重任务。

## 关键配置项

`ExecutorConfig` 的重要字段：

- `xxl_admin_baseurl`
  支持单地址或逗号分隔多地址，支持根路径或 `/api` 路径。
- `executor_app_name`
  必须与 `xxl-job-admin` 中的执行器 `AppName` 完全一致。
- `access_token`
  如果 admin 配了 token，这里必须一致。
- `executor_url`
  上报给 admin 的外部访问地址；如果前面还有 Nginx / 网关 / 端口映射，要填真实外部地址。
- `executor_listen_host` / `executor_listen_port`
  本地 HTTP 服务监听地址。
- `max_workers`
  同步线程池大小。
- `task_timeout`
  默认任务超时秒数；若 admin 下发 `executorTimeout`，以后者为准。
- `task_queue_length`
  单个 `jobId` 的本地排队长度。
- `log_target`
  任务日志后端，支持 `disk` / `redis`。
- `log_local_dir`
  磁盘日志目录。
- `log_redis_uri`
  Redis 日志后端连接地址。
- `graceful_close` / `graceful_timeout`
  控制关闭时是否等待任务自然结束。
- `http_retry_times` / `http_retry_duration` / `http_timeout`
  admin 请求重试和超时配置。
- `dotenv_try` / `dotenv_path`
  是否尝试从 `.env` 读取配置。

如果 `executor_url` 不填，默认会使用：

```text
http://{executor_listen_host}:{executor_listen_port}
```

## 运行期上下文

任务函数里可以通过 `g` 读取当前调度数据：

```python
from pyxxl import g


@app.register(name="demoJobHandler")
async def demo_job():
    run_data = g.xxl_run_data
    g.logger.info("jobId=%s", run_data.jobId)
    g.logger.info("logId=%s", run_data.logId)
    g.logger.info("executorParams=%s", run_data.executorParams)
    g.logger.info("broadcast=%s/%s", run_data.broadcastIndex, run_data.broadcastTotal)
    return "ok"
```

`RunData` 目前兼容的核心字段包括：

- `jobId`
- `logId`
- `executorHandler`
- `executorBlockStrategy`
- `executorParams`
- `executorTimeout`
- `broadcastIndex`
- `broadcastTotal`

## 同步线程池任务注意事项

Python 的线程池任务不能像 Java `JobThread + interrupt` 一样真正强制中断。

这意味着：

- `thread` 模式更依赖任务自身协作取消
- 长时间阻塞的同步代码不适合放在线程池里
- 真正需要强隔离和更强中断能力的任务，应该优先使用 `process`

推荐写法：

```python
@app.register(name="syncLoopJob", mode="thread")
def sync_loop_job():
    while not g.cancel_event.is_set():
        # 你的同步逻辑
        time.sleep(1)
    return "cancelled"
```

不推荐写法：

```python
@app.register(name="badSyncJob", mode="thread")
def bad_sync_job():
    while True:
        time.sleep(3)
```

上面这种线程会一直占住线程池，`timeout` 和 `kill` 都无法像 Java 一样立即生效。

## Metrics

安装 metrics 扩展后会自动暴露 Prometheus 指标：

```shell
pip install "pyxxl[metrics]"
```

访问地址：

```text
http://{executor_listen_host}:{executor_listen_port}/metrics
```

目前主要包括：

- 成功任务计数
- 失败任务计数
- 当前运行中任务数
- 当前排队任务数
- 线程池状态指标

## 多 admin / callback / 日志

运行期行为要点：

- 多 admin 不是广播，而是按配置顺序依次 failover
- 任务结束后先进入 callback 队列，再由后台 worker 发送给 admin
- callback 失败会写入本地补偿文件，并在启动时恢复重放
- `/log` 会兼容 admin 新旧版本的字段差异
- Redis 日志后端在 Redis 不可用时会降级返回空日志，而不是直接让接口 500

## 当前内部目录结构

对业务方来说，主要使用顶层 API 即可；如果你要继续二开，可以从下面的结构读起：

```text
pyxxl/
  __init__.py            # 顶层公共 API
  app/                   # 高层启动入口
  config/                # 配置模型与配置校验
  context/               # 运行期上下文 g
  logger/                # 任务日志后端
  model/                 # 枚举与 RunData 等数据模型
  monitoring/            # Prometheus 指标
  protocol/              # admin client 与 executor HTTP 协议层
  runtime/               # handler 注册、调度状态、callback、进程执行模型
  log.py                 # 执行器内部日志
  error.py               # 业务异常定义
  utils.py               # 通用工具
```

关键文件入口：

- `pyxxl/__init__.py`
- `pyxxl/app/runner.py`
- `pyxxl/config/executor.py`
- `pyxxl/protocol/server.py`
- `pyxxl/protocol/admin_client.py`
- `pyxxl/runtime/executor.py`
- `pyxxl/runtime/handlers.py`

## 文档

本地文档：

- 使用文档：`docs/XXL_JOB_PYTHON_EXECUTOR_USAGE.md`
- Java 官方对照与学习文档：`docs/JAVA_PYTHON_EXECUTOR_COMPARISON_AND_STUDY_GUIDE.md`
- Java 对齐检查清单：`docs/PYTHON_EXECUTOR_PARITY_CHECKLIST.md`
- API 文档：`docs/docs/apis/`

在线文档：

- <https://fcfangcc.github.io/pyxxl/latest/>

## 开发调试

启动本地调度中心：

```shell
./init_dev_env.sh
```

调度中心地址：

```text
http://127.0.0.1:8080/xxl-job-admin/
```

默认账号：

```text
admin / 123456
```

启动执行器示例：

```shell
uv sync --all-extras
py example/executor_app.py
```

## 已知差异与限制

- XXL-JOB 管理台展示的执行时间仍然可能显示成 callback 时间，这是 admin 侧解析问题，不是 Python 执行器单独造成的。
- 线程池同步任务无法做到 Java 那种真正强制中断。
- 当前磁盘日志目录结构还没有完全复刻 Java 官方实现。
- 任务 CRUD、cron 修改、发布暂停恢复不属于 executor 协议能力，应由 admin 侧实现。

## 其他

- 访问 `xxl-job-admin` 接口时支持读取代理环境变量，例如 `HTTP_PROXY`
- `0.3.0` 是最后一个支持 Python `3.8` 的版本，当前版本要求 Python `>= 3.9`

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=fcfangcc/pyxxl&type=Date)](https://www.star-history.com/#fcfangcc/pyxxl&Date)

## 开发人员
下面是开发人员如何快捷的搭建开发调试环境

### 启动xxl的调度中心

```shell
./init_dev_env.sh
```

http://127.0.0.1:8080/xxl-job-admin/

admin/123456

### 启动执行器

```shell
uv sync --all-extras
# 修改 example/executor_app.py 中的配置后启动
py example/executor_app.py
```
