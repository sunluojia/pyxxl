# XXL-JOB Python Executor 最终差异清单

更新时间：`2026-03-29`

这份文档不是“待办列表”，而是当前版本的最终对齐快照。

目标只有一个：直接对照官方 Java executor，判断当前 Python 执行器到底已经对齐到什么程度，还差什么，以及哪些东西根本不应该继续往 executor 里堆。

---

## 1. 结论先行

如果你的目标是：

- 保留现有 `xxl-job-admin`
- 把 Python 任务，尤其是公司爬虫任务，统一接入 XXL-JOB 调度
- 做一个长期可维护的 Python executor

那么当前 `pyxxl` 已经可以视为“可用的正式基线”，而不是 demo。

当前结论：

- executor 协议层已经和 Java 官方核心语义基本对齐
- 运行时层已经补齐了最关键的 block strategy、callback、kill、queue、logId 去重
- 对典型 Python 任务，尤其 `async` 和 `process` 模式，已经接近“和 Java executor 几乎一样能用”
- 剩余差异主要集中在执行模型、日志目录结构、callback 批量策略、registry 生命周期细节、GLUE/script 支持

一句话判断：

- 现在已经足够接公司爬虫
- 不需要再去“重写一套 Python admin”
- 如果还要继续追平 Java，后续投入应放在运维细节和日志/生命周期，而不是协议主链路

---

## 2. 本次对照基线

### 2.1 Java 官方源码

本次差异判断基于直接读取以下官方源码：

- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/executor/XxlJobExecutor.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/openapi/impl/ExecutorBizImpl.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/JobThread.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/TriggerCallbackThread.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/ExecutorRegistryThread.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/log/XxlJobFileAppender.java`

补充参考：

- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/server/EmbedServer.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/constant/Const.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/constant/ExecutorBlockStrategyEnum.java`

### 2.2 Python 当前实现

当前 Python executor 的核心实现位置：

- `E:/code/python/pyxxl/pyxxl/app/runner.py`
- `E:/code/python/pyxxl/pyxxl/config/executor.py`
- `E:/code/python/pyxxl/pyxxl/context/runtime.py`
- `E:/code/python/pyxxl/pyxxl/model/run_data.py`
- `E:/code/python/pyxxl/pyxxl/protocol/server.py`
- `E:/code/python/pyxxl/pyxxl/protocol/admin_client.py`
- `E:/code/python/pyxxl/pyxxl/runtime/executor.py`
- `E:/code/python/pyxxl/pyxxl/runtime/handlers.py`
- `E:/code/python/pyxxl/pyxxl/runtime/callbacks.py`
- `E:/code/python/pyxxl/pyxxl/logger/disk.py`
- `E:/code/python/pyxxl/pyxxl/logger/redis.py`

---

## 3. 已对齐的核心能力

下面这些可以视为“当前已基本对齐 Java 官方 executor 的核心能力”。

| 能力 | Java 官方基线 | Python 当前实现 | 结论 |
| --- | --- | --- | --- |
| `/beat` | `ExecutorBizImpl.beat()` | `pyxxl/protocol/server.py` | 已对齐 |
| `/idleBeat` 忙碌判断 | `JobThread.isRunningOrHasQueue()` | `Executor.is_running_or_has_queue()` | 已对齐 |
| `/run` 基础调度流程 | `ExecutorBizImpl.run()` | `server.py + runtime/executor.py` | 已对齐 |
| `/kill` | `ExecutorBizImpl.kill()` | `server.py + Executor.cancel_job()` | 已对齐 |
| `/log` 接口协议 | `ExecutorBizImpl.log()` | `server.py + logger/*` | 已对齐协议 |
| access token 校验 | `EmbedServer` / `Const.XXL_JOB_ACCESS_TOKEN` | `validate_access_token()` | 已对齐 |
| `SERIAL_EXECUTION` | `JobThread.pushTriggerQueue()` | `Executor._handle_serial_execution()` | 已对齐 |
| `DISCARD_LATER` | `ExecutorBizImpl.run()` | `Executor._handle_discard_later()` | 已对齐 |
| `COVER_EARLY` | `ExecutorBizImpl.run() + registJobThread()` | `Executor._handle_cover_early()` | 已对齐核心语义 |
| `logId` 去重 | `JobThread.triggerLogIdSet` | `_job_log_ids` | 已对齐 |
| kill 后运行中任务失败 callback | `JobThread finally` | `_cleanup_task()` / `_run()` | 已对齐 |
| kill 后排队任务失败 callback | `JobThread` 清空队列 | `_push_failed_queued_tasks()` | 已对齐 |
| callback 异步补偿 | `TriggerCallbackThread` | `CallbackManager` | 已对齐核心能力 |
| callback 启动恢复 | `retryFailCallbackFile()` | `_replay_persisted_requests()` | 已对齐 |
| admin 多地址 failover | Java 逐个尝试 `AdminBiz` | `XXL._post()` 逐地址尝试 | 已对齐 |
| registry / registryRemove | `ExecutorRegistryThread` | `_register_task()` + `registryRemove()` | 已对齐基础能力 |
| 任务运行上下文 | `XxlJobContext / XxlJobHelper` | `g + RunData` | 已对齐核心用法 |
| Prometheus 指标 | Java 无原生同名模块 | `monitoring/prometheus.py` | Python 额外增强 |

补充说明：

- Python 当前不仅支持 `async`，还补了 `process` 模式。这不是 Java 官方的等价实现，但对 Python 爬虫场景非常有价值。
- 这也是为什么当前版本更适合作为“面向 Python 任务的 executor”，而不是机械复刻 Java 类结构。

---

## 4. 最后一轮剩余差异

下面这些是截至当前版本，仍然和 Java 官方 executor 不完全等价的地方。

### 4.1 执行模型与中断语义仍然不等价

优先级：`P1`

Java 官方：

- 每个 `jobId` 对应一个 `JobThread`
- `JobThread.toStop()` + `interrupt()` 共同参与停止
- 超时场景下，`JobThread` 还会启动单独的 `futureThread` 并在超时后 `futureThread.interrupt()`

关键事实：

- Java 也不是真正意义上的“硬杀”
- `JobThread` 源码自己就写明了：`interrupt` 只对 `wait/join/sleep` 这类阻塞状态有效，不会无条件终止任意运行中的线程

Python 当前：

- `async` 模式用 `asyncio.Task`
- `thread` 模式进入线程池，靠 `g.cancel_event` 协作取消
- `process` 模式使用子进程，是当前最接近“强隔离”的方案

真实差异不是“Java 能强杀、Python 不能”，而是：

- Java 的 `JobThread` 是专属线程模型，中断语义更集中
- Python 的 `thread` 模式跑在线程池里，控制力更弱
- Python 的 `process` 模式反而是一个更务实的补强方案

对业务的影响：

- `async` 任务基本没有问题
- 浏览器驱动、长阻塞采集、第三方同步库任务，应该优先使用 `process`
- 不应再试图把“线程池模式完全做到和 Java 一样”作为目标

建议：

- 保持当前 `thread/process` 双模式
- 文档和示例继续明确：重任务默认 `process`
- 不建议继续投入做“线程池硬中断”

### 4.2 Java 的 GLUE / Script / `@XxlJob(init, destroy)`，Python 还没有

优先级：`P2`

Java 官方：

- `ExecutorBizImpl.run()` 支持 `BEAN`、`GLUE_GROOVY`、脚本类型
- `XxlJobExecutor.registryJobHandler()` 支持 `@XxlJob(value, init, destroy)`
- handler 生命周期里有 `init()` / `destroy()`

Python 当前：

- 只支持显式注册 Python 函数
- 注册方式是 `@app.register(...)`
- 没有 GLUE 动态脚本执行
- 没有 `init/destroy` 生命周期钩子扫描
- 没有自动扫描包并注册 handler

这是不是缺陷，要看目标：

- 对“公司爬虫接入 XXL-JOB”来说，不是核心缺陷
- 对“完全复刻 Java executor 生态”来说，这是明确差异

建议：

- 当前阶段不要优先补 GLUE/script
- 如果后面确实需要，可单独做一个“Python 发现层”
- 但这层应该是可选增强，不要污染当前简洁的 decorator API

### 4.3 registry 生命周期细节还没完全按 Java 线程模型实现

优先级：`P1`

Java 官方：

- `ExecutorRegistryThread` 使用独立线程
- 心跳周期使用 `Const.BEAT_TIMEOUT`，默认 `30s`
- `toStop()` 时 `interrupt + join`
- `registryRemove` 在 registry 线程退出时统一执行

Python 当前：

- `PyxxlRunner._register_task()` 使用 `asyncio` 后台任务
- 当前周期是 `10s`
- 退出时先取消任务，再在 cleanup 中主动调用 `registryRemove`
- 没有显式的 registry 状态对象和失败退避状态

当前实现是可用的，但和 Java 仍有差异：

- 心跳节奏不同
- 停止顺序不完全同构
- 注册状态的可观测性更弱

建议：

- 把 registry 周期改成可配置，并默认贴近 Java 的 `30s`
- 增加 registry 成功/失败时间戳、最近错误、连续失败次数
- 为 registry 单独暴露 metrics 或运行态状态

### 4.4 callback 仍未完全对齐 Java 的“批量 + 日志回写”语义

优先级：`P1`

Java 官方：

- `TriggerCallbackThread` 先 `take()` 一个，再 `drainTo()` 批量发送
- 失败补偿放到 `callbacklogs/`
- callback 成功、失败、异常会写入对应任务日志文件

Python 当前：

- `CallbackManager` 以单个 `CallbackRequest` 为单位处理
- 失败补偿写到 `logs/.callback_failures/*.json`
- 会重放、会重试，但不会像 Java 那样把 callback 生命周期回写到对应任务日志

影响：

- 功能上已经够用
- 但高并发回调下，Java 的批量发送更省请求
- Java 的任务日志里能更清楚看到 callback 成功/失败痕迹，Python 目前这块可观测性较弱

建议：

- 增加可选批量 callback 投递
- 在任务日志里补充 callback 成功/失败摘要
- 保留当前 JSON 持久化格式即可，不必强行完全复刻 Java 的文件命名

### 4.5 磁盘日志目录结构和清理策略还没有对齐 Java

优先级：`P1`

Java 官方：

- 日志目录结构：`logBasePath/yyyy-MM-dd/{logId}.log`
- 还维护 `gluesource/`、`callbacklogs/`
- `readLog()` 按文件逐行读取
- 日志清理按“日期目录”删除，且 `logRetentionDays >= 3` 才生效

Python 当前：

- 任务日志文件名：`logs/pyxxl-{logId}.log`
- callback 补偿目录：`logs/.callback_failures/`
- 没有 `yyyy-MM-dd` 日期目录
- 当前清理逻辑按文件创建时间扫描
- 单次日志查询有 `1000` 行上限

当前协议兼容性没有问题，但仍有差异：

- 和 Java 的日志落盘目录不一致
- 管理台侧排障时，不方便和 Java executor 按同一磁盘结构看问题
- 长日志翻页效率与 Java 也不是同一种策略

建议：

- 如果要继续追平，优先把磁盘日志改成 `yyyy-MM-dd/{logId}.log`
- callback 补偿目录也可改成显式 `callbacklogs/`
- 这项对外 API 不影响，只影响内部实现和运维体验

### 4.6 Java 的常驻 `JobThread` 与空闲回收语义，Python 没有完全等价结构

优先级：`P2`

Java 官方：

- 每个 `jobId` 会持有一个 `JobThread`
- 线程空闲轮询超过阈值后，会主动从 `jobThreadRepository` 移除

Python 当前：

- 没有常驻线程对象
- 当前是“运行中的 task + 按 `jobId` 分队列 + lock + replacement 状态”
- 更贴近 Python 运行时，但不是 Java 的同构模型

这项差异是否值得补，要看收益：

- 从业务效果看，不是核心问题
- 从调试和和 Java 一一映射的角度看，这是结构差异

建议：

- 不建议为了类结构相似而强行引入常驻 `JobThread` 式对象
- 当前状态机已经足够清晰

### 4.7 Java 的配置面更完整，Python 仍是“业务友好优先”

优先级：`P2`

Java 官方额外有这些语义：

- `enabled=false` 可直接关闭 executor 初始化
- `appname` 为空可关闭自动注册
- `address / ip / port` 三者语义区分更细

Python 当前：

- 更偏向“启动就工作”的配置风格
- 通过 `executor_url` 和 `executor_listen_host/port` 已覆盖大多数场景
- 但还没有完全同名、同语义地复刻 Java 配置面

结论：

- 这是配置风格差异，不是主链路缺陷

---

## 5. 哪些事情根本不属于 executor

这部分必须明确，不然很容易越做越偏。

不属于 Python executor 的范围：

- 任务 CRUD
- cron 修改
- 发布、暂停、恢复
- 任务分组管理
- 直接写 `xxl-job-admin` 数据库
- “丢失任务补单”这类 admin 侧兜底逻辑

这些都应该留在：

- `xxl-job-admin`
- 或你们公司自己加在 admin 侧的内部 API / service 层

不要把这些能力堆进 Python executor。

executor 应该只负责：

- 接收调度
- 管理本地运行时
- 记录日志
- 回调结果

---

## 6. 面向公司爬虫接入的建议优先级

如果目标是“现在就让爬虫任务稳定接入 XXL-JOB”，建议按下面的顺序做。

### 6.1 现在就够用的部分

以下能力已经足够支撑接入：

- `async` handler
- `process` 模式重任务
- block strategy
- callback 补偿
- access token
- 多 admin failover
- `/log` 查看任务日志

### 6.2 真要继续追平，建议只做这三项

建议优先继续做的只有：

1. 日志目录对齐 Java
   把磁盘日志改成 `yyyy-MM-dd/{logId}.log`
2. callback 批量发送 + callback 日志回写
   提升高并发场景的可观测性
3. registry 生命周期可观测
   周期、状态、失败次数、退避策略更清晰

### 6.3 当前不建议优先做的项

以下项不建议现在投入：

- GLUE_GROOVY / script 执行
- 自动扫描包并注册 handler
- 模拟 Java 常驻 `JobThread`
- 把 Python 线程池模式强行做成“像 Java 一样可中断”
- 在 executor 里直接加任务管理 API

---

## 7. 最终判断

截至当前版本，可以这样评价：

- 对 `xxl-job-admin` 来说，这已经是一个合格的 Python executor
- 对 Python 业务方来说，顶层 API 已经足够简洁稳定
- 对爬虫任务来说，只要把重任务放到 `process` 模式，整体效果已经非常接近 Java 官方 executor

仍然不完全等价的地方主要是：

- Java 的 `JobThread` / `GLUE` / `Script` / `init-destroy` 生命周期体系
- callback 的批量与日志回写
- registry 和日志目录的内部实现细节

所以现在最准确的说法不是：

- “已经 100% 复刻 Java executor”

而是：

- “已经完成 Python executor 的核心对齐，剩下的是少数内部机制和运维细节差异”

如果你后面还要继续学这个框架，建议配合阅读：

- `docs/JAVA_PYTHON_EXECUTOR_COMPARISON_AND_STUDY_GUIDE.md`
- `docs/XXL_JOB_PYTHON_EXECUTOR_USAGE.md`

