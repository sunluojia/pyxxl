# Java XXL-JOB 与 Python 执行器对照学习文档

## 1. 文档目标

这份文档解决三个问题：

- Java 官方 XXL-JOB 执行器的核心运行机制是什么
- 当前 Python 执行器分别用哪些文件和结构去对齐这些机制
- 如果后续要继续二开，尤其是服务公司爬虫任务，应该优先改哪里、不该轻易改哪里

这份文档不重复安装步骤。安装和直接接入方式请看：

- `docs/XXL_JOB_PYTHON_EXECUTOR_USAGE.md`
- `docs/PYTHON_EXECUTOR_PARITY_CHECKLIST.md`

---

## 2. 先建立正确心智模型

XXL-JOB 里真正要分清的是三层：

1. `xxl-job-admin`
   作用：任务管理、调度触发、查看执行结果和日志。
2. executor 协议层
   作用：对外暴露 `/beat`、`/idleBeat`、`/run`、`/kill`、`/log`，并对 admin 发 `registry`、`callback`、`registryRemove`。
3. executor 运行时层
   作用：真正管理 handler、排队、取消、日志、回调、生命周期。

Java 官方版本这三层拆得更明显。

Python 当前版本虽然文件更少，但本质上也已经拆成了这三层：

- 协议入口：`pyxxl/protocol/server.py`
- 运行时核心：`pyxxl/runtime/executor.py`
- 启动与生命周期：`pyxxl/app/runner.py`
- admin 客户端：`pyxxl/protocol/admin_client.py`
- 配置：`pyxxl/config/executor.py`
- 日志：`pyxxl/logger/*`
- 指标：`pyxxl/monitoring/prometheus.py`

---

## 3. Java 与 Python 核心映射

| Java 官方 | 作用 | Python 当前对应 | 说明 |
| --- | --- | --- | --- |
| `XxlJobExecutor` | 执行器总入口，启动日志、admin 客户端、callback 线程、embed server | `pyxxl/app/runner.py` 的 `PyxxlRunner`，配合 `ExecutorConfig` 和 `Executor` | Python 把启动装配拆到了 runner 和 config |
| `EmbedServer` | 暴露 executor OpenAPI | `pyxxl/protocol/server.py` | Java 用 embed server，Python 用 aiohttp |
| `ExecutorBizImpl` | `/beat` `/idleBeat` `/run` `/kill` `/log` 的业务实现 | `pyxxl/protocol/server.py` + `pyxxl/runtime/executor.py` | Python 把协议解析和运行时逻辑拆开了 |
| `JobThread` | 每个 `jobId` 的运行线程、队列、去重、阻塞策略 | `pyxxl/runtime/executor.py` | Python 没有完全等价的 Thread，对应的是 `tasks + queue + _job_locks + _job_log_ids + _cover_replacements` |
| `TriggerCallbackThread` | callback 队列、批量回调、失败补偿、启动恢复 | `pyxxl/runtime/callbacks.py` 里的 `CallbackManager` | 核心语义已对齐 |
| `ExecutorRegistryThread` | 周期注册、停止时注销 | `pyxxl/app/runner.py` 的 `_register_task()` + `pyxxl/protocol/admin_client.py` | Python 仍是定时循环注册 |
| `XxlJobFileAppender` | 任务日志落盘 | `pyxxl/logger/disk.py` | 目录结构还没完全对齐 Java |
| `JobLogFileCleanThread` | 清理过期日志 | `LogBase.expired_loop()` + `DiskLog.expired_once()` | Python 已有清理循环 |
| `AdminBiz` | executor 调 admin 的 client | `pyxxl/protocol/admin_client.py` | Python 额外补了多 admin failover |
| `XxlJobContext` / `XxlJobHelper` | 任务上下文、日志、分片参数 | `pyxxl.g` + `RunData` | Python 用上下文对象 `g` 暴露运行参数 |

如果你只记最关键的 5 个映射：

- Java `EmbedServer` <-> Python `server.py`
- Java `ExecutorBizImpl` <-> Python `server.py + executor.py`
- Java `JobThread` <-> Python `Executor` 的每个 `jobId` 状态机
- Java `TriggerCallbackThread` <-> Python `CallbackManager`
- Java `ExecutorRegistryThread` <-> Python `PyxxlRunner._register_task()`

---

## 4. 推荐阅读顺序

### 4.1 先读 Python，再回头看 Java

推荐顺序：

1. `example/executor_app.py`
2. `pyxxl/config/executor.py`
3. `pyxxl/app/runner.py`
4. `pyxxl/protocol/server.py`
5. `pyxxl/runtime/executor.py`
6. `pyxxl/protocol/admin_client.py`
7. `pyxxl/logger/disk.py`
8. `pyxxl/monitoring/prometheus.py`
9. `pyxxl/tests/test_executor.py`
10. `pyxxl/tests/api/test_server.py`

### 4.2 再读 Java 官方

推荐顺序：

1. `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/executor/XxlJobExecutor.java`
2. `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/server/EmbedServer.java`
3. `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/openapi/impl/ExecutorBizImpl.java`
4. `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/JobThread.java`
5. `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/TriggerCallbackThread.java`
6. `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/ExecutorRegistryThread.java`

---

## 5. 一次完整任务是怎么跑起来的

### 5.1 启动阶段

Java：

1. `XxlJobExecutor.start()`
2. 初始化日志路径
3. 初始化 admin client 列表
4. 启动日志清理线程
5. 启动 callback 线程
6. 启动 executor server

Python：

1. `PyxxlRunner.run_executor()`
2. `create_server_app()`
3. `_cleanup_ctx()` 里创建 `XXL`、`Executor`、日志后端
4. 启动 `callback_manager`
5. 启动日志清理任务
6. 启动注册循环 `_register_task()`
7. aiohttp 暴露 `/beat` `/idleBeat` `/run` `/kill` `/log`

### 5.2 调度触发阶段

admin 调 Python executor 的 `/run`：

1. `server.py` 校验 token
2. 请求体转成 `RunData`
3. 调 `Executor.run_job()`
4. `Executor` 根据 `jobId`、`logId`、block strategy 做调度决策
5. 真正执行 handler
6. 任务结束后把结果写入 `CallbackManager`

### 5.3 回调阶段

Java：

- `JobThread` 结束时 push 到 `TriggerCallbackThread`
- callback 失败会落文件
- 下次启动会重放失败 callback

Python：

- `_run()` 结束后调用 `_push_callback()`
- `CallbackManager` 后台 worker 异步发送
- 失败时持久化到 `logs/.callback_failures`
- 下次启动 `start()` 先重放持久化 callback

### 5.4 关停阶段

Java：

1. stop embed server
2. 等优雅关闭窗口
3. 中断全部 job thread
4. 等 thread 结束
5. 停 callback thread
6. 注销 executor

Python：

1. cancel register task、log clean task
2. `registryRemove`
3. `graceful_close()` 或 `shutdown()`
4. flush / stop callback manager
5. close admin client session

---

## 6. 最关键的运行时差异

### 6.1 Java 是 `JobThread`，Python 是 `asyncio task + thread pool`

Java：

- 每个 `jobId` 对应一个 `JobThread`
- 线程内有自己的触发队列
- 可以 `interrupt`

Python：

- 当前运行中的任务放在 `Executor.tasks`
- 同一 `jobId` 的排队任务放在 `Executor.queue[jobId]`
- 并发控制靠 `_job_locks`
- 同步 handler 会丢到线程池

### 6.2 为什么 Python 不能简单说“复刻 Java 执行器就行”

因为最难复刻的不是 HTTP 协议，而是执行模型。

协议层很好复刻：

- `/beat`
- `/idleBeat`
- `/run`
- `/kill`
- `/log`
- `registry`
- `callback`
- `registryRemove`

这些现在基本都已经有了。

真正难的是：

- 同步阻塞任务如何真正 kill
- 超时后如何回收执行资源
- 浏览器驱动、Selenium、Playwright 这类任务怎么安全终止

这就是为什么后面要单独做“子进程执行模型”。

---

## 7. 当前已经基本对齐 Java 的部分

下面这些已经可以认为核心语义比较接近 Java：

- executor OpenAPI：`/beat`、`/idleBeat`、`/run`、`/kill`、`/log`
- 入站 `XXL-JOB-ACCESS-TOKEN` 校验
- `SERIAL_EXECUTION`
- `DISCARD_LATER`
- `COVER_EARLY`
- `logId` 去重
- kill / shutdown 时对运行中任务补失败 callback
- kill / shutdown 时对队列任务补失败 callback
- callback 异步发送
- callback 失败持久化与启动恢复
- 多 admin 顺序 failover
- metrics 成功/失败计数

---

## 8. 当前仍然不等价的地方

### 8.1 同步任务不能像 Java 那样真正中断

当前 Python：

- `async def` 最好
- `def` 会进线程池
- 超时和取消依赖 `g.cancel_event`
- 如果任务自己不响应取消，线程不会真的被强杀

这是最大差距。

### 8.2 registry 生命周期还能继续收紧

当前 Python 的 `_register_task()` 已经在持续注册，语义接近 `ExecutorRegistryThread`。

但如果继续优化，还可以加强：

- 停止顺序更严格
- registry 状态可观测
- 注册失败退避策略更细

### 8.3 日志还没完全对齐 Java 的目录和读取方式

当前 Python：

- `disk` 和 `redis` 两种后端
- `/log` 已兼容新旧响应字段

剩余差异：

- 磁盘日志目录结构
- 长日志分页效率
- Redis 日志目前仍是同步 client

### 8.4 admin 内部任务管理不属于 executor 协议

Java 官方 `/api/*` 只解决 executor 协议，不负责任务 CRUD。

所以如果你们公司想做：

- 自动建任务
- 改 cron
- 发布版本
- 暂停恢复

正确位置不在 Python executor，而在 `xxl-job-admin` 的 service/controller 层增加内部 API。

---

## 9. 看 Python 代码时，应该如何理解每个文件

### `pyxxl/config/executor.py`

配置项全貌、admin URL 归一化、executor 对外地址计算。

### `pyxxl/app/runner.py`

runner 如何组装 client、executor、log、metrics，以及如何启动和关闭。

### `pyxxl/protocol/server.py`

executor 对 admin 的协议面，包括 token 校验和 5 个 OpenAPI。

### `pyxxl/runtime/executor.py`

这是最核心的文件：handler 注册、每个 `jobId` 的调度状态、阻塞策略、回调补偿、取消和关停。

### `pyxxl/protocol/admin_client.py`

admin 交互客户端：`registry`、`callback`、`registryRemove`、多 admin failover、HTTP 重试。

### `pyxxl/logger/*`

任务日志的存储和 `/log` 读取数据来源。

### `pyxxl/monitoring/prometheus.py`

运行时指标和 Prometheus 暴露。

---

## 10. 建议你怎么学这个框架

### 第一阶段：先会用

目标：

- 能自己写一个 handler
- 能让 admin 调到 Python
- 能看日志和 callback

建议动作：

1. 跑 `example/executor_app.py`
2. 在 admin 上手动建一个 `demoJobHandler`
3. 看 `/run` 到 `_run()` 到 `callback` 的日志链路

### 第二阶段：理解协议层

目标：

- 明白 admin 发什么、executor 回什么
- 明白为什么格式不能乱改

建议动作：

1. 读 `server.py`
2. 读 Java `EmbedServer.java` 和 `ExecutorBizImpl.java`
3. 对照 `/beat` `/idleBeat` `/run` `/kill` `/log`

### 第三阶段：理解运行时层

目标：

- 明白一个 `jobId` 是怎么排队、怎么取消、怎么替换的

建议动作：

1. 读 `executor.py`
2. 重点看 `run_job()`、`cancel_job()`、`_run()`、`_finish()`
3. 再看 `tests/test_executor.py`

### 第四阶段：理解和 Java 的真正差距

目标：

- 明白为什么爬虫重任务不能只靠线程池

建议动作：

1. 读 Java `JobThread.java`
2. 再回头看 Python 同步任务的 thread pool 行为
3. 想清楚为什么下一步必须是 process model

---

## 11. 给公司爬虫二开时，正确的改造方向

这一节描述的是“建议如何使用这个执行器去承载爬虫任务”，不是说执行器本身要内置 crawler 业务框架。

如果目标是给公司爬虫长期接入，我建议按这个顺序推进。

### 11.1 固定入口 handler

不要给每个爬虫都注册一个 Python 函数名。

建议固定少量入口：

- `crawler_dispatch`
- `crawler_backfill`
- `crawler_maintenance`

### 11.2 统一 `executorParams` 为 JSON schema

不要传随意字符串。

建议至少包含：

- `task_code`
- `task_version`
- `env`
- `args`
- `resources`

### 11.3 浏览器类任务做子进程执行模型

适用任务：

- Playwright
- Selenium
- Chrome/Edge 驱动
- 长阻塞采集

父进程负责：

- XXL 协议
- callback
- registry
- 日志聚合

子进程负责：

- 真正执行爬虫
- 接受 kill 信号
- 回传状态和产物

### 11.4 不要让 Python 直接改 admin 库表

如果要自动创建和更新任务，应在 `xxl-job-admin` 里补公司内部 API。

---

## 12. 哪些地方可以大胆重构，哪些地方别乱动

### 可以重构

- 把 `executor.py` 拆成 `callback_manager.py`、`task_state.py`、`scheduler.py`
- 把 registry loop 单独抽成 `registry_manager.py`
- 把同步/异步/子进程执行器拆成不同 runner
- 加更强的 metrics 和 tracing

### 不建议轻易改

- `/beat` `/idleBeat` `/run` `/kill` `/log` 的请求和响应格式
- `XXL-JOB-ACCESS-TOKEN` 校验方式
- `idleBeat` 的 queue-aware 语义
- `logId` 去重
- `COVER_EARLY` 只保留最后 replacement 的语义
- callback 持久化恢复链路
- `registry` / `callback` / `registryRemove` 的顺序 failover 语义

这些地方一旦改坏，会直接影响和 admin 的兼容。

---

## 13. 调试时该看哪里

### admin 调不到 executor

先看：

- `setting.py` 里的 `executor_baseurl`
- `main.py` 里的 `_register_task()`
- `xxl_client.py` 里的 `registry()`

### 任务明明触发了但没执行

先看：

- `server.py` 的 `/run`
- `executor.py` 的 `run_job()`
- handler 名称是否注册成功

### 任务执行完但 admin 没结果

先看：

- `executor.py` 的 `_push_callback()`
- `CallbackManager`
- `logs/.callback_failures`
- `xxl_client.py` 的 `callback()`

### kill 不生效

先判断任务类型：

- `async def` 任务：看协程是否正确响应取消
- `def` 任务：看是否检查了 `g.cancel_event`
- 浏览器/驱动任务：默认就不该继续放在线程池里

---

## 14. 最后给你的结论

如果你的目标只是“让 Python 任务接入现有 XXL-JOB 调度”，当前这套已经够用，而且核心协议层和常用执行语义已经很接近 Java 官方执行器。

如果你的目标是“作为公司爬虫执行底座长期演进”，重点已经不再是继续补 executor 基础接口，而是：

1. 固定 `crawler_dispatch` 入口
2. 统一参数 schema
3. 做子进程执行模型
4. 在 admin 侧补内部管理 API
5. 继续增强生命周期、日志和可观测性

一句话概括：

- Java 官方执行器的协议层，这个 Python 版本已经补得差不多了
- 真正决定能不能服务爬虫规模化落地的，是接下来执行模型和接入层的设计
