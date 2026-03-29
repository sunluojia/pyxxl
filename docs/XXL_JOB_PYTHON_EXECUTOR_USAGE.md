# Python XXL-JOB 执行器使用文档

## 1. 文档目标

这份文档面向两类人：

- 需要把 Python 任务接入现有 `xxl-job-admin` 的开发人员
- 需要把公司爬虫任务统一纳入 XXL-JOB 管理的维护人员

当前这个 Python 执行器已经补齐了大部分和 Java 官方执行器相关的核心行为，重点包括：

- 执行器 OpenAPI：`/beat`、`/idleBeat`、`/run`、`/kill`、`/log`
- 入站 `XXL-JOB-ACCESS-TOKEN` 校验
- `SERIAL_EXECUTION`、`DISCARD_LATER`、`COVER_EARLY`
- `logId` 去重
- kill / shutdown 对运行中任务和排队任务补发失败 callback
- callback 异步队列、失败重试、本地补偿文件、启动恢复
- 多 admin 地址 failover
- metrics 成功/失败计数、运行中任务和队列指标

如果你需要从 Java 官方源码反向理解当前 Python 实现，或者准备继续二开，请再读：

- `docs/JAVA_PYTHON_EXECUTOR_COMPARISON_AND_STUDY_GUIDE.md`

仍然和 Java 官方实现存在的主要差异：

- Python 同步任务本质上跑在线程池里，不能像 Java `JobThread + interrupt` 一样真正强杀
- 调度中心的任务 CRUD 仍然应该由 `xxl-job-admin` 或你们自己的 admin API 管理，不建议由 Python 直接写库
- 磁盘日志目录结构和 Java 官方日志目录还没有完全统一

---

## 2. 安装

最小安装：

```powershell
py -m pip install pyxxl
```

常见扩展：

```powershell
# Redis 日志
py -m pip install "pyxxl[redis]"

# .env 配置加载
py -m pip install "pyxxl[dotenv]"

# Prometheus metrics
py -m pip install "pyxxl[metrics]"

# 开发环境
py -m pip install -e ".[dev,dotenv,metrics,redis]"
```

---

## 3. 启动前准备

需要先准备一个可访问的 `xxl-job-admin`。

典型地址：

- `http://127.0.0.1:8080/xxl-job-admin`

Python 执行器现在支持以下 admin 配置写法：

- admin 根地址：`http://127.0.0.1:8080/xxl-job-admin`
- `/api` 地址：`http://127.0.0.1:8080/xxl-job-admin/api`
- `/api/` 地址：`http://127.0.0.1:8080/xxl-job-admin/api/`
- 多地址：`http://10.0.0.1:8080/xxl-job-admin,http://10.0.0.2:8080/xxl-job-admin`

内部会自动归一化成 `/api/` 地址，并在请求时按顺序依次尝试。

---

## 4. 最小可运行示例

```python
import asyncio
import json

from pyxxl import ExecutorConfig, PyxxlRunner
from pyxxl.ctx import g


config = ExecutorConfig(
    xxl_admin_baseurl="http://127.0.0.1:8080/xxl-job-admin",
    executor_app_name="crawler-python-executor",
    access_token="same-token-as-admin",
    executor_listen_host="0.0.0.0",
    executor_listen_port=9999,
    log_local_dir="logs",
    graceful_close=True,
    graceful_timeout=300,
    debug=True,
)

app = PyxxlRunner(config)


@app.register(name="demoJobHandler")
async def demo_job() -> str:
    params = g.xxl_run_data.executorParams or "{}"
    payload = json.loads(params)
    g.logger.info("payload=%s", payload)
    await asyncio.sleep(1)
    return "ok"


if __name__ == "__main__":
    app.run_executor()
```

启动：

```powershell
py example/executor_app.py
```

---

## 5. 配置说明

最关键的配置项如下。

### `xxl_admin_baseurl`

作用：

- 执行器注册
- 任务结果 callback
- registry remove

建议：

- 单 admin：填写一个根地址
- 多 admin：逗号分隔多个地址

示例：

```python
xxl_admin_baseurl="http://10.0.0.1:8080/xxl-job-admin,http://10.0.0.2:8080/xxl-job-admin"
```

### `executor_app_name`

必须和 `xxl-job-admin` 上的执行器 AppName 完全一致，否则任务不会路由到这个执行器。

### `access_token`

如果 admin 配了 token，Python 执行器也必须配置相同 token。

这个 token 同时用于：

- Python -> admin 的请求头
- admin -> Python 执行器的请求头校验

### `executor_url`

这是暴露给 admin 的地址，不一定等于本地监听地址。

常见场景：

- 本地监听：`0.0.0.0:9999`
- 外部通过 Nginx 暴露：`http://scheduler.company.com/python-executor`

这时应该配置：

```python
executor_listen_host="0.0.0.0"
executor_listen_port=9999
executor_url="http://scheduler.company.com/python-executor"
```

### `task_timeout`

默认任务超时时间。调度中心如果传了 `executorTimeout`，则优先用调度参数。

### `task_queue_length`

`SERIAL_EXECUTION` 的单机排队长度上限。超过后会拒绝新任务。

### `graceful_close` / `graceful_timeout`

用于优雅停机。

建议线上开启：

```python
graceful_close=True
graceful_timeout=300
```

### `log_target`

可选：

- `disk`
- `redis`

建议默认用 `disk`，部署和排障更简单。

---

## 6. 在 XXL-JOB Admin 上如何配置任务

### JobHandler

admin 上配置的 `JobHandler` 要和 Python 代码里 `@app.register(name="...")` 的名字一致。

例如：

```python
@app.register(name="demoJobHandler")
async def demo_job():
    ...
```

那 admin 上就要填：

- `JobHandler = demoJobHandler`

### 阻塞策略建议

#### `SERIAL_EXECUTION`

适合：

- 同一站点同一资源不允许并发
- 需要严格串行的增量同步任务

#### `DISCARD_LATER`

适合：

- 周期很短，但重复跑没有意义
- 上一个任务还没结束时，后一个任务直接丢弃

#### `COVER_EARLY`

适合：

- “只保留最新一次”的任务
- 例如最新榜单刷新、最新配置同步、最新回刷窗口

当前 Python 实现已经对齐到“旧任务停止，新任务接管”的核心语义，并且连续多次触发只保留最后一次 replacement。

### 路由策略建议

对于 Python 执行器集群，常见建议如下：

- `FAILOVER`：希望自动跳过故障节点时使用
- `BUSYOVER`：希望跳过繁忙节点时使用
- `SHARDING_BROADCAST`：需要分片广播时使用

`idleBeat` 现在已经覆盖“运行中或队列非空都算忙碌”的 Java 语义。

---

## 7. 推荐的爬虫接入方式

如果目标是“方便公司爬虫接入”，不建议把每个爬虫函数都直接暴露成一个 XXL-JOB handler。更稳妥的做法是固定少量 handler，再由 handler 内部分发。

建议保留少量固定入口：

- `crawler_dispatch`
- `crawler_maintenance`
- `crawler_backfill`

推荐把 `executorParams` 统一成 JSON：

```json
{
  "task_code": "jd_goods_daily",
  "task_version": "2026-03-01",
  "env": "prod",
  "args": {
    "shop_id": 123,
    "date": "2026-03-29"
  },
  "resources": {
    "proxy_pool": "default",
    "browser": false
  }
}
```

对应的 Python 入口建议：

```python
import json

from pyxxl.ctx import g


TASKS = {}


@app.register(name="crawler_dispatch")
async def crawler_dispatch() -> str:
    payload = json.loads(g.xxl_run_data.executorParams or "{}")
    task_code = payload["task_code"]
    handler = TASKS[task_code]
    return await handler(payload)
```

这样做的好处：

- admin 层任务模板更稳定
- Python 内部可以自由做任务版本切换
- 便于统一埋点、审计、参数校验、资源控制

---

## 8. 同步任务注意事项

这是 Python 和 Java 官方执行器最不一样、也是最容易踩坑的地方。

Java 的 `interrupt + toStop` 虽然也不完美，但长期实践里对阻塞任务的处理比 Python 线程池更成熟。

Python 当前行为：

- `async def` 任务最推荐
- `def` 任务会被放进线程池
- 取消或超时时，只能通过 `g.cancel_event` 协作式退出
- 如果同步任务内部死循环、阻塞系统调用、卡浏览器驱动，线程不会被真正强杀

正确示例：

```python
import time

from pyxxl.ctx import g


@app.register(name="sync_demo")
def sync_demo() -> str:
    while not g.cancel_event.is_set():
        time.sleep(1)
    return "cancelled safely"
```

不建议直接把以下任务扔进同步线程池：

- Playwright
- Selenium
- 浏览器驱动任务
- 长时间不可中断的阻塞采集

这类任务后续建议单独做“子进程执行模型”。

---

## 9. 运行期行为说明

### callback

任务执行结束后，结果不会直接在任务协程里同步发送给 admin，而是：

1. 先进入 callback 队列
2. 后台 worker 异步发送
3. 失败时本地持久化到 `.callback_failures`
4. 下次启动时自动重放

这和 Java `TriggerCallbackThread` 的思路保持一致。

### kill / shutdown

当前行为：

- 运行中的任务会回调失败
- 队列中尚未执行的任务也会回调失败
- `COVER_EARLY` 被替换掉的旧任务和排队任务也会有明确结果

### 多 admin

当前行为：

- 按配置顺序依次尝试
- 任一成功就算成功
- 适合 admin 主备或多节点 failover

它不是“广播到所有 admin”，而是“顺序 failover”。

---

## 10. Metrics

安装：

```powershell
py -m pip install "pyxxl[metrics]"
```

地址：

```text
http://{executor_listen_host}:{executor_listen_port}/metrics
```

目前可见的主要指标包括：

- 成功任务计数
- 失败任务计数
- 当前运行中任务数
- 当前队列任务数
- 线程池信息

---

## 11. 常见故障排查

### admin 调度不到 Python 执行器

优先检查：

- `executor_app_name` 是否一致
- `executor_url` 是否是 admin 可直连地址
- `access_token` 是否一致
- 执行器是否持续注册成功

### callback 丢失或 admin 看不到结果

优先检查：

- `logs/.callback_failures` 下是否有补偿文件
- admin 地址是否至少有一个可达
- 执行器退出时是否走了正常 shutdown

### `/log` 或 Redis 日志报错

优先检查：

- Redis 是否真的启动
- `log_target="redis"` 时 `log_redis_uri` 是否正确

如果没有强需求，建议优先用 `disk`。

---

## 12. 和 Java 官方执行器对齐情况

当前可以认为：

- 协议层已经足够接近 Java 官方执行器
- 常用调度语义已经基本对齐
- 对 Python 任务的管理已经可以稳定接入现有 `xxl-job-admin`

但如果要做到“公司爬虫可长期稳定大规模接入”，下一阶段建议继续做：

1. 固定 `crawler_dispatch` 入口和参数 schema
2. 浏览器/驱动类任务改子进程执行
3. 在 `xxl-job-admin` 侧补内部管理 API，而不是让 Python 直接操作 admin 库表
4. 继续增强日志、生命周期、可观测性
