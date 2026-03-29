# XXL-JOB Python Executor 对齐清单

适用目标：

- 保留现有 `xxl-job-admin` 作为调度中心和运维界面
- 让 Python 执行器对齐官方 Java 执行器的核心协议与关键行为
- 优先满足“公司爬虫任务接入 XXL-JOB 调度”的落地需求

不在本清单第一阶段范围内的事项：

- 重写 `xxl-job-admin`
- 在 Python 侧完整复刻 Java 的 GLUE Groovy/脚本编辑体验
- 做独立于 XXL-JOB 的 URL 级爬虫调度系统

---

## 1. 结论先行

当前仓库里的 Python 实现已经具备“能接 XXL-JOB”的基础能力，但离“可作为公司级 Python 执行器长期使用”还差一层关键行为对齐。

核心判断：

- 方向是对的：就是要复刻 Java 官方执行器
- 但不能只复刻接口名，还要复刻关键行为
- 第一阶段应聚焦“官方执行器等价”
- 第二阶段再叠加“爬虫任务接入友好层”

建议执行顺序：

1. 先做 P0：协议与稳定性对齐
2. 再做 P1：可运维性和性能
3. 最后做 P2：面向爬虫业务的接入抽象

配套学习文档见：

- `docs/JAVA_PYTHON_EXECUTOR_COMPARISON_AND_STUDY_GUIDE.md`

---

## 2. 代码基线

### 2.1 Java 官方基线

执行器核心参考文件：

- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/server/EmbedServer.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/openapi/impl/ExecutorBizImpl.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/executor/XxlJobExecutor.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/JobThread.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/ExecutorRegistryThread.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/TriggerCallbackThread.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/constant/Const.java`
- `E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/constant/ExecutorBlockStrategyEnum.java`

管理端任务模型参考文件：

- `E:/code/java/xxl-job/xxl-job-admin/src/main/java/com/xxl/job/admin/controller/biz/JobInfoController.java`
- `E:/code/java/xxl-job/xxl-job-admin/src/main/java/com/xxl/job/admin/service/impl/XxlJobServiceImpl.java`
- `E:/code/java/xxl-job/xxl-job-admin/src/main/java/com/xxl/job/admin/model/XxlJobInfo.java`
- `E:/code/java/xxl-job/xxl-job-admin/src/main/java/com/xxl/job/admin/model/XxlJobGroup.java`
- `E:/code/java/xxl-job/xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/trigger/JobTrigger.java`
- `E:/code/java/xxl-job/xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/route/ExecutorRouteStrategyEnum.java`
- `E:/code/java/xxl-job/xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/route/strategy/ExecutorRouteBusyover.java`
- `E:/code/java/xxl-job/xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/openapi/OpenApiController.java`

### 2.2 Python 当前实现

- `E:/code/python/pyxxl/pyxxl/server.py`
- `E:/code/python/pyxxl/pyxxl/executor.py`
- `E:/code/python/pyxxl/pyxxl/main.py`
- `E:/code/python/pyxxl/pyxxl/xxl_client.py`
- `E:/code/python/pyxxl/pyxxl/setting.py`
- `E:/code/python/pyxxl/pyxxl/schema.py`
- `E:/code/python/pyxxl/pyxxl/logger/disk.py`
- `E:/code/python/pyxxl/pyxxl/logger/redis.py`
- `E:/code/python/pyxxl/pyxxl/prometheus.py`

---

## 3. 第一阶段完成定义

第一阶段完成后，应该满足以下标准：

- Python 执行器可被 `xxl-job-admin` 正常注册、下线、调度、杀死、拉日志
- `ROUND`、`FAILOVER`、`BUSYOVER`、`SHARDING_BROADCAST` 等常用管理端路由策略在 Python 执行器上行为可预测
- 阻塞策略 `SERIAL_EXECUTION`、`DISCARD_LATER`、`COVER_EARLY` 行为与 Java 官方核心语义一致
- callback 不因 admin 短暂不可用而丢失
- 执行器入站请求有 token 校验
- 关键行为有自动化测试覆盖

---

## 4. 差距总览

### 已实现

- 执行器基础接口已实现：`/beat`、`/idleBeat`、`/run`、`/kill`、`/log`
- admin 侧调用已实现：`registry`、`registryRemove`、`callback`
- 支持三种阻塞策略
- 支持任务日志读取
- 支持 async handler 和 sync handler

### 主要缺口

- 多 admin、registry 生命周期与 Java 线程模型还未完全对齐
- 同步重任务取消能力较弱
- 日志实现与 Java 的按日期目录、长日志分页仍有差距
- 部分监控与生命周期增强项还未完成

---

## 5. P0 清单：必须先做

### P0-01 入站 access token 校验

目标：

- Python 执行器对 `/beat`、`/idleBeat`、`/run`、`/kill`、`/log` 的请求，必须校验 `XXL-JOB-ACCESS-TOKEN`

官方基线：

- Java 在 `EmbedServer.dispatchRequest` 中校验 token
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/server/EmbedServer.java`

当前现状：

- Python 仅在对 admin 发请求时携带 token
- 执行器服务端未校验任何 header
- 参考：`E:/code/python/pyxxl/pyxxl/xxl_client.py`
- 参考：`E:/code/python/pyxxl/pyxxl/server.py`

改造任务：

- [x] 在 `server.py` 增加统一鉴权中间件或公共校验函数
- [x] 对所有 executor 路由校验 `XXL-JOB-ACCESS-TOKEN`
- [x] 与 Java 行为保持一致：未通过时返回 `code=500` 风格失败结果，而不是抛框架默认异常
- [x] 增加无 token、错误 token、正确 token 的接口测试

验收标准：

- 未携带 token 时 admin 无法调度 Python executor
- token 正确时所有接口正常
- 错误 token 时返回格式与 XXL-JOB 风格兼容

---

### P0-02 callback 改为独立队列 + 重试补偿

目标：

- 任务执行结束后，不直接在执行协程中把 callback 当成最终动作
- 要有独立 callback 队列、后台发送、失败重试

官方基线：

- Java 使用 `TriggerCallbackThread`
- callback 失败会落盘，后续重试
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/TriggerCallbackThread.java`

当前现状：

- Python 在 `Executor._run()` 内直接 `await xxl_client.callback(...)`
- 一旦 admin 不可用，回调失败风险直接暴露到任务结束路径
- 参考：`E:/code/python/pyxxl/pyxxl/executor.py`

改造任务：

- [x] 新增 callback manager，维护异步队列
- [x] 任务结束时只负责 enqueue callback 请求
- [x] 独立后台任务发送 callback，并在失败后异步重试
- [x] callback 失败时记录本地补偿文件或 Redis 持久化补偿队列
- [x] 执行器启动后自动重放失败 callback
- [x] 停机时尽量 flush callback 队列

当前进度：

- 已完成内存队列 + 后台 worker + 失败重试 + `graceful_close` 阶段 flush
- 已完成本地补偿文件落盘与启动恢复重放
- 当前仍未做 Redis 版 callback 补偿队列，但按本清单目标，基于本地文件的补偿链路已经满足 `P0-02`

验收标准：

- admin 短暂不可达时，任务结果不会直接丢失
- admin 恢复后 callback 能补发成功
- callback 线程/协程异常不会影响任务执行主路径

---

### P0-03 `idleBeat` 语义对齐 Java

目标：

- `idleBeat(jobId)` 必须在“任务运行中”或“该 jobId 仍有触发排队”时都返回忙碌

官方基线：

- Java 的 `idleBeat` 调用 `jobThread.isRunningOrHasQueue()`
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/openapi/impl/ExecutorBizImpl.java`
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/JobThread.java`

当前现状：

- Python `idleBeat` 仅通过 `job_id in self.tasks` 判断
- 队列非空但当前刚切换状态时可能判断不准
- 参考：`E:/code/python/pyxxl/pyxxl/server.py`

改造任务：

- [x] 为 executor 新增 `is_running_or_has_queue(job_id)` 方法
- [x] `/idleBeat` 改为调用该方法
- [x] 增加针对 `BUSYOVER` 语义的测试用例

验收标准：

- 同一 `jobId` 有排队任务时，`idleBeat` 返回忙碌
- 管理端 `BUSYOVER` 路由不会错误选中繁忙节点

---

### P0-04 增加 `logId` 去重

目标：

- 相同 `jobId + logId` 的重复调度请求不能被重复执行

官方基线：

- Java 在 `JobThread.pushTriggerQueue()` 中维护 `triggerLogIdSet`
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/JobThread.java`

当前现状：

- Python 当前没有等价去重集合
- 同一调度日志可能被重复接受
- 参考：`E:/code/python/pyxxl/pyxxl/executor.py`

改造任务：

- [x] 增加每个 `jobId` 维度的 `logId` 去重结构
- [x] 任务执行前、入队前都做 dedupe
- [x] 完成或取消后清理 dedupe 记录
- [x] 增加重复触发测试

验收标准：

- 相同 `logId` 重复 `run` 不会触发两次执行
- 返回错误消息与 Java 行为尽量接近

---

### P0-05 kill 与停机时，对排队任务补发失败 callback

目标：

- 被 kill 的正在执行任务要回调失败
- 队列中尚未执行的任务也要回调失败

官方基线：

- Java 在 `JobThread` 停止后，会给队列中未执行的 trigger 全部推送失败 callback
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/JobThread.java`

当前现状：

- Python `cancel_job(include_queue=True)` 会直接丢弃队列任务，不回调
- 当前测试也是按“不回调”写的
- 参考：`E:/code/python/pyxxl/pyxxl/executor.py`

改造任务：

- [x] 取消运行中任务时，回调失败结果
- [x] 清空等待队列时，为每个被丢弃任务补发失败 callback
- [x] 修改测试用例，按官方语义断言

验收标准：

- 被 kill 后，admin 上每个调度日志都有最终状态
- 不会出现队列任务静默消失

---

### P0-06 `COVER_EARLY` 行为对齐

目标：

- `COVER_EARLY` 必须明确体现为“旧任务停止，新任务接管”

官方基线：

- Java 的实现是让旧 `JobThread` 退出，再建立新线程，并将新 trigger 入队
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/openapi/impl/ExecutorBizImpl.java`

当前现状：

- Python 当前是：先把新任务塞队列，再异步 cancel 旧任务
- 能工作，但时序不够明确，边界条件更多
- 参考：`E:/code/python/pyxxl/pyxxl/executor.py`

改造任务：

- [x] 明确 `COVER_EARLY` 的状态机
- [x] 确保旧任务被标记为失败，新任务一定获得执行机会
- [x] 避免竞态导致两次 `_finish`、队列错位或状态覆盖
- [x] 增加快速连续触发的并发测试

验收标准：

- `COVER_EARLY` 连续触发时只保留最新任务
- admin 端日志结果可解释，旧任务有失败结论，新任务成功执行

当前进度：

- 已改为“保留一个待接管 replacement，旧任务结束后再启动新任务”的明确状态机
- 已补齐 `asyncio.create_task()` 后、任务尚未真正进入 `_run()` 就被取消的清理路径
- 已增加“替换排队任务”“连续多次只保留最新一次”的自动化测试

---

### P0-07 支持多 admin 地址

目标：

- Python executor 能像 Java 一样配置多个 admin 地址

官方基线：

- Java `XxlJobExecutor.initAdminBizList()` 支持逗号分隔多地址
- 注册与 callback 都会依次尝试多个 admin
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/executor/XxlJobExecutor.java`

当前现状：

- Python `xxl_admin_baseurl` 是单字符串单地址
- 参考：`E:/code/python/pyxxl/pyxxl/setting.py`
- 参考：`E:/code/python/pyxxl/pyxxl/xxl_client.py`

改造任务：

- [x] 配置层支持 `xxl_admin_baseurls` 或兼容逗号分隔字符串
- [x] registry 支持多地址尝试
- [x] callback 支持多地址尝试
- [x] registryRemove 支持多地址尝试
- [x] 增加一个 admin 不可用、另一个可用的测试

验收标准：

- 任一 admin 可达时 executor 能继续完成注册和回调

当前进度：

- 已支持逗号分隔多 admin 地址，并兼容填写 admin 根地址或 `/api` 地址
- `registry`、`callback`、`registryRemove` 均按地址顺序依次尝试，任一成功即返回
- 已增加一个 admin 不可用、另一个可用的 failover 自动化测试

---

### P0-08 注册线程与下线流程对齐

目标：

- 注册心跳、停止注册、下线注销流程要稳定

官方基线：

- Java 使用独立 `ExecutorRegistryThread`
- 心跳周期使用 `Const.BEAT_TIMEOUT = 30`
- 停止时先中断线程，再 `registryRemove`
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/thread/ExecutorRegistryThread.java`
- 参考：`E:/code/java/xxl-job/xxl-job-core/src/main/java/com/xxl/job/core/constant/Const.java`

当前现状：

- Python 用 `_register_task()` 每 10 秒注册一次
- 停止阶段直接 `register_task.cancel()` 然后调用 `registryRemove`
- 参考：`E:/code/python/pyxxl/pyxxl/main.py`

改造任务：

- [ ] 将注册周期做成可配置，默认对齐 30s
- [ ] 明确“注册协程已停止”与“已执行下线”的顺序
- [ ] 异常情况下确保不会因为单次 `registryRemove` 失败而无日志
- [ ] 增加 stop/cleanup 测试

验收标准：

- 执行器关闭后 admin 中地址能及时下线
- 注册协程异常时可见日志充分

---

### P0-09 同步任务的隔离与取消能力

目标：

- 对公司爬虫这类阻塞型 Python 任务，至少要有可控的取消与超时语义

官方基线：

- Java 用线程模型，`interrupt + toStop` 虽然不完美，但有长期沉淀
- Python 阻塞任务更难中断，线程池方案风险更高

当前现状：

- Python sync handler 通过 `asyncio.to_thread()` 执行
- 超时或取消后，只能通过 `cancel_event` 让业务代码自行配合
- 若业务不配合，线程仍可能继续跑
- 参考：`E:/code/python/pyxxl/pyxxl/executor.py`

改造任务：

- [ ] 第一阶段最少保留现有线程模式，但明确文档限制
- [ ] 新增“进程执行模式”设计占位，作为 P1/P2 重点
- [ ] 为 sync handler 增加测试，验证超时后能正确上报失败
- [ ] 为爬虫任务预留 `process` 模式配置项

验收标准：

- 文档明确哪些任务可直接跑在线程池，哪些必须走子进程
- 不会误导业务方把浏览器/驱动类任务直接堆在线程池

---

### P0-10 修复 metrics 钩子 bug

目标：

- success/failed 计数真正生效

当前现状：

- `main.py` 中赋值的是 `_successed_callback` / `_failed_callback`
- `executor.py` 实际调用的是 `successed_callback` / `failed_callback`
- 参考：`E:/code/python/pyxxl/pyxxl/main.py`
- 参考：`E:/code/python/pyxxl/pyxxl/executor.py`

改造任务：

- [x] 修复属性名
- [x] 增加 metrics 行为测试，而不是只断言 `/metrics` 接口存在

验收标准：

- 成功执行后成功计数增加
- 失败/取消/超时后失败计数增加

当前进度：

- 已改为在 `main.Executor` 中显式注入 `successed_callback` / `failed_callback`
- 已增加 `/metrics` 下 success / failed counter 的行为测试

---

### P0-11 自动化测试补齐

目标：

- 第一阶段所有关键行为必须有测试，不靠手工 admin 点点点

测试重点：

- [x] token 鉴权
- [x] callback 补偿
- [x] idleBeat + queue 语义
- [x] logId 去重
- [x] kill queued task callback
- [x] multi-admin failover
- [x] block strategy 并发边界

说明：

- 当前环境已安装 `pytest`
- 测试命令暂定：

```powershell
py -m pip install -e .[dev,dotenv,metrics,redis]
py -m pytest -q
```

---

## 6. P1 清单：建议紧接着做

### P1-01 增强日志读取与兼容性

- [ ] 评估是否需要对齐 Java 的按日期分目录日志组织
- [ ] 优化磁盘日志中间行读取效率
- [ ] Redis 日志读取改为异步或后台线程，避免阻塞事件循环
- [ ] 长日志分页与 `fromLineNum` 语义覆盖测试

### P1-02 IP 获取与网络配置增强

- [ ] 替换 `get_network_ip()` 的简化实现
- [ ] 支持更可靠的网卡/IP 选择策略
- [ ] 明确 `executor_url` 与 `executor_listen_host` 的职责

### P1-03 生命周期与关停增强

- [ ] `shutdown()` 时不应只清空内存队列，要保证状态闭环
- [ ] 线程池、日志任务、callback 任务、registry 任务退出顺序明确
- [ ] 增加长任务下的 graceful close 测试

### P1-04 协议兼容矩阵

- [ ] 明确支持的 XXL-JOB 版本范围
- [ ] 回归验证 2.x、3.x 的回调与日志格式差异
- [ ] 将兼容逻辑集中管理，避免散落在注释和条件判断里

### P1-05 可观测性增强

- [ ] metrics 增加 callback queue 长度、registry 状态、dropped task 数量
- [ ] executor 级别日志增加结构化字段
- [ ] 增加“当前运行任务”和“当前排队任务”的可观测数据

---

## 7. P2 清单：面向公司爬虫的接入层

这一层不是“XXL-JOB 执行器协议必须项”，但如果目标是“方便公司爬虫接入”，就应该尽快开始设计。

### P2-01 固定 handler：`crawler_dispatch`

建议：

- 不要让每个爬虫任务都在 XXL-JOB 上绑定一个 Python 函数名
- 建议保留少量固定 handler，例如：
  - `crawler_dispatch`
  - `crawler_maintenance`
  - `crawler_backfill`

### P2-02 任务参数标准化

建议 `executorParam` 不直接塞随意文本，统一成 JSON：

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

要做的事：

- [ ] 定义参数 schema
- [ ] 执行前校验
- [ ] 日志中打印标准化任务上下文

### P2-03 子进程执行模型

适用场景：

- Playwright / Selenium
- 长阻塞网络采集
- 依赖驱动和浏览器的爬虫
- 需要 kill 时真正终止的任务

要做的事：

- [ ] 设计 `process` 模式执行器
- [ ] 父进程负责 XXL-JOB 协议和回调
- [ ] 子进程负责真正执行爬虫
- [ ] 父子进程间传状态、日志、取消信号

### P2-04 产物与检查点

- [ ] 统一任务输出目录
- [ ] 支持 artifact 上报
- [ ] 支持 checkpoint / resume
- [ ] 失败后可定位到快照、请求样本、页面截图

### P2-05 与 admin 的内部管理 API

背景：

- 官方 `/api/*` OpenAPI 只给执行器用
- 不提供任务 CRUD
- 任务管理逻辑在 `JobInfoController` / `XxlJobServiceImpl`

建议：

- [ ] 在 `xxl-job-admin` 内增加公司内部 API
- [ ] 通过 service 层安全创建/更新爬虫任务
- [ ] 不要让 Python 直接写 admin 库表

建议新增能力：

- `upsert crawler job`
- `publish crawler version`
- `pause/resume crawler job`
- `manual trigger crawler job`
- `bind task_code -> xxl_job_info.id`

### P2-06 任务模板化

建议按爬虫类型沉淀模板：

- [ ] 周期增量模板
- [ ] 全量回刷模板
- [ ] 分片广播模板
- [ ] 单节点串行模板
- [ ] 故障恢复模板

---

## 8. 推荐实现顺序

### 里程碑 M1：先做到“可替代现有 pyxxl”

- [x] P0-01 token 校验
- [x] P0-02 callback 队列与补偿
- [x] P0-03 idleBeat 语义对齐
- [x] P0-04 logId 去重
- [x] P0-05 kill queued task callback
- [x] P0-06 COVER_EARLY 时序对齐
- [x] P0-07 multi-admin
- [x] P0-10 metrics bug 修复
- [x] P0-11 自动化测试

### 里程碑 M2：做到“公司可上线”

- [ ] P1-01 日志增强
- [ ] P1-02 网络配置增强
- [ ] P1-03 生命周期增强
- [ ] P1-04 协议兼容矩阵
- [ ] P1-05 可观测性增强

### 里程碑 M3：做到“爬虫接入友好”

- [ ] P2-01 固定 handler 方案
- [ ] P2-02 参数 schema
- [ ] P2-03 子进程执行模型
- [ ] P2-04 artifact/checkpoint
- [ ] P2-05 admin 内部 API
- [ ] P2-06 模板化接入

---

## 9. 建议的目录调整

建议在 Python 侧重构为：

```text
pyxxl/
  server.py
  client/
  protocol/
  runtime/
    executor.py
    callback_manager.py
    registry_manager.py
    task_state.py
  worker/
    async_runner.py
    thread_runner.py
    process_runner.py
  log/
  metrics/
  tests/
```

目的：

- 把“协议层”和“运行时层”拆开
- 后面做子进程模型时不需要推翻当前结构

---

## 10. 接手说明

如果上下文中断，新的执行者应按以下顺序继续：

1. 先读本文件
2. 再读 Java 官方以下文件：
   - `EmbedServer.java`
   - `ExecutorBizImpl.java`
   - `JobThread.java`
   - `TriggerCallbackThread.java`
   - `ExecutorRegistryThread.java`
3. 再读 Python 当前以下文件：
   - `server.py`
   - `executor.py`
   - `main.py`
   - `xxl_client.py`
4. 从 M1 开始，按编号顺序推进

推荐交接提示词：

```text
请根据 docs/PYTHON_EXECUTOR_PARITY_CHECKLIST.md 继续推进 M1。
先完成 P0-01 到 P0-03，修改代码并补测试。
```

---

## 11. 当前已确认的问题列表

- [x] Python 执行器服务端未校验 access token（已修复）
- [x] callback 无独立队列与失败补偿
- [x] `idleBeat` 仅检查 running，不检查 queue
- [x] 无 `logId` 去重
- [x] kill 队列任务时不回调失败
- [x] 单 admin 地址限制
- [x] sync 任务超时无法强制终止线程
- [x] metrics success/failed 钩子属性名写错
- [x] `OpenApiController` 不提供任务 CRUD，若要自动建任务应在 admin 侧补内部 API

---

## 12. 本文件维护规则

后续推进时请遵守：

- 每完成一项，将对应 checkbox 勾掉
- 若实现方案发生变化，先改本文件再改代码
- 若发现与官方 Java 行为不一致的新点，追加新编号，不要改旧编号含义
- 若决定某项不做，必须补“原因”和“替代方案”
