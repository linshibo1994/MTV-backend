# 视频远端任务可靠性最终审查修复 Spec

## 目标

消除视频 Provider 远端任务在 Celery worker 丢失、同步 soft/hard 信号、stale reclaim、远端明确失败与提交响应不确定时的重复付费或永久冻结风险，同时保持状态、积分与 outbox 单调。

## 约束

- 每项先补可重复红灯，再做最小实现并单独转绿。
- 不修改 lifecycle/scripts 相关文件，不提交，不回滚共享工作区改动。
- 不把 lease、密钥、完整远端响应或 URL 写入日志和业务 JSON。
- 未确认远端终态或 submit 是否被接受时必须 fail closed，不自动重提或释放冻结。
- 复用现有 Provider checkpoint、取消债务、version CAS 与独立事务 helper。
- 第 5 项先内省本地已安装 SDK；证据不足时只查询官方公开文档，不发送配置、ID 或凭据。

## 分项红灯与验收

### 1. checkpoint 消息 ACK

- 红灯：专用任务当前 `reject_on_worker_lost=False`、`acks_on_failure_or_timeout=True`。
- 验收：Task/Request 运行时属性为 `acks_late=True`、`reject_on_worker_lost=True`、`acks_on_failure_or_timeout=False`；DB 临时错误只产生受控 retry/reject，同 payload 保留；DB 已持久或明确冲突才正常返回。
- 限制：本子任务以 Request 语义集成为主，真实 Redis+prefork kill/hard-timeout 由主会话继续验证。

### 2. 同步 interrupt

- 红灯：同步 soft 先进入 `WorkerAsyncRuntime` 无界等待 cleanup，永久 finally 会阻塞 holder 恢复；等待还会吞后续 hard 信号。
- 验收：取消等待前调用 worker-scoped 同步 hook，未 durable 的 holder 立即尝试发布 checkpoint；已 durable 不重复。cleanup 有短 grace，第二个 `TimeLimitExceeded` 原样传播；broker 失败后同步入口仍可走独立 DB 恢复；Provider submit 仅一次。

### 3. stale 旧代际 orphan

- 红灯：A lease 过期、B reclaim 后，A checkpoint 冲突并被专用任务 ACK，远端 A 永久无人取消。
- 验收：reclaim 前持久化 A 代际；A 延迟 ID 进入有界、幂等、多记录 orphan cancellation debt，不覆盖 B 权威 ID；专用任务只在债务 durable/重复时 ACK；reconciler 分别消解 A orphan 与 B 当前 debt；提交未知可重放且不重复计费。

### 4. Provider 明确远端终态

- 红灯：OpenAI、Google、Bailian 及 Volcengine 明确 failed/cancelled/error 仍抛普通异常，worker 因取消 unsupported 保持 processing/frozen。
- 验收：新增 typed 远端终态异常，携 task ID、规范化非敏感状态；已确认终态直接安全 failed/release 并持久证据；网络、解析及未知状态仍保留 processing，不进入普通失败补偿。

### 5. submit 响应不确定

- 红灯：submit 请求可能已被服务端接受但客户端未取得 ID，stale/API retry 会再次 submit。
- 验收：有官方稳定 client token 能力时按 `job_id + attempt/lease generation` 组装；无法证明支持时，submit 前持久 `submit_intent`，响应不确定后持久 `submission_uncertain`，禁止 stale、自动与手动 retry，保留冻结并可观测；明确 submit 拒绝才允许 failed/release。

### 6. Volcengine 恢复 preflight

- 红灯：metadata 已有 provider task ID 时仍读取/编码参考媒体，存储故障阻断恢复。
- 验收：checkpoint 与恢复分支先于媒体 preflight；恢复路径不访问对象存储、不重新 submit。

## 最终验证

```bash
cd backend
.venv/bin/python -m pytest -q -ra <逐项专项>
.venv/bin/python -m pytest -q -ra tests/test_providers.py tests/test_video*.py tests/test_worker_async_runtime.py
.venv/bin/python -m pytest -q -ra
.venv/bin/ruff check app tests
.venv/bin/mypy app
git diff --check
```

## 进度记录

- 第 1 项完成：Task/Request ACK 属性、WorkerLost/TimeLimit 重排、retry 发布失败保留原消息专项 `5 passed`。
- 第 2 项完成：同步 soft interrupt 会在等待异步 cleanup 前快照 holder 并立即尝试 checkpoint；cleanup 使用短 grace，后续 hard limit 原样传播；broker 失败可切换一次性隔离数据库恢复，已 durable 时不重复提交。
- 第 3 项完成：stale reclaim 会先持久旧 lease 代际；迟到远端 ID 进入有界、幂等的 orphan cancellation debt，reconciler 独立消解旧代际与当前任务债务，并以 version CAS 防止覆盖新 attempt。
- 第 4 项完成：四家 Provider 对已确认的远端 failed/canceled 统一抛携任务 ID 与规范化终态的 typed error；worker 仅在 ID 权威一致时 failed/release，网络、解析与未知状态继续 fail closed。
- 第 5 项完成：本地 SDK 内省版本为 OpenAI `2.46.0`、Google GenAI `2.12.1`、Volcengine `5.0.41`，百炼使用直连 HTTP；均未找到可证明适用于视频 submit 的稳定服务端幂等 token，因此统一采用 durable `submit_intent` / `submission_uncertain`。OpenAI 与 Volcengine 的 SDK 默认会自动重试 2 次，生成客户端已显式设为 `max_retries=0`；Google 的 `retry_options=None` 与百炼 HTTPX 均只发送一次。未决提交会阻止 stale reclaim、底层 claim、API 新 attempt 与取消释放；明确 4xx 拒绝才写入 `submit_rejected` 并 failed/release。三家客户端 close 已统一为有界生命周期，普通关闭错误不再覆盖 typed error。
- 第 6 项完成：Volcengine 已有权威任务 ID 时先进入 checkpoint/恢复分支，跳过媒体尺寸预检、对象存储读取、编码和再次 submit。

## 验证记录

- 第 5/6 项与四家 Provider 提交顺序专项：`11 passed`。
- checkpoint 与 Provider 专项：`83 passed`。
- 全部视频专项：`422 passed, 15 skipped`。
- 全量 Pytest：`1297 passed, 57 skipped`。
- 全量 Ruff：通过。
- 全量 Mypy：`141 source files` 通过。
- `git diff --check`：通过。
- 跳过项均依赖未配置的 `MTV_TEST_POSTGRES_URL`；真实 PostgreSQL 并发语义、Redis+prefork worker lost/hard-timeout 仍需主会话或具备基础设施的环境补验。

## 独立复审补充

- OpenAI、Google、百炼客户端关闭必须有界；存在主异常时，普通 close 错误只记录非敏感异常类型，不得覆盖远端明确终态、明确 submit 拒绝、业务取消或 soft/hard time limit；不存在主异常时 close 错误继续传播。
- `asyncio.wait_for` 不是关闭硬上限：close 吞掉 `CancelledError` 时不得继续等待。关闭任务必须脱离当前 `WorkerAsyncRuntime` invocation scope；超时后只发取消、不再 await，pending task 登记为有界 orphan，并毒化所属运行时、禁止后续 invocation 复用。
- 百炼不得再由裸 `AsyncClient.__aexit__` 决定主异常；三家均复用同一关闭生命周期契约。
- 视频产物删除失败日志不得记录完整 `storage_key`，仅允许固定长度摘要。
- 状态：已完成。OpenAI、Google、百炼均已接入统一硬截止关闭；主异常优先级、控制流信号、无主异常关闭失败、吞取消 close、orphan 数量上界、运行时停止复用及日志脱敏均有回归测试。
- 补充精确测试：`15 passed, 66 deselected`；checkpoint 文件：`62 passed`；运行时文件：`18 passed, 1 skipped`。
- 补充视频专项：`437 passed, 15 skipped`；跳过项均依赖未配置的 `MTV_TEST_POSTGRES_URL`。
- 补充全量 Ruff、全量 Mypy（`142 source files`）与 `git diff --check`：通过。
- 全量 Pytest 首轮为 `1310 passed, 57 skipped, 8 failed`；8 个失败均位于并行修改中的媒体生命周期/时间线共享范围，由主会话在该范围稳定后重跑，不作为本补充项的通过结论。

## 第三轮复审补充

- poisoned runtime 处置：本地 Celery `5.6.3` 与 Billiard 源码表明，单独抛出 `Reject(requeue=True)` 只会由主进程拒绝消息，不会回收 prefork 子进程。六个视频 Celery 入口现统一捕获 `WorkerAsyncRuntimeStopping` 并抛出 `WorkerTerminate`，同时显式配置 `acks_late=True`、`reject_on_worker_lost=True`、`acks_on_failure_or_timeout=False`。子进程不发送 READY 即退出，主进程将未完成任务归一为 `WorkerLostError` 后执行 `reject(requeue=True)`，且补充新的 prefork 子进程。
- close orphan 上限：客户端 close 在创建 asyncio task 前，先通过进程级线程锁原子预留固定容量为 1 的 token。正常完成、响应取消、迟到完成及运行时放弃均幂等释放；24 个线程、24 个事件循环同时进入 close 时只允许 1 个 close task 启动。
- shutdown 硬截止：`WorkerAsyncRuntime` 显式登记 detached task；常规 drain 与 loop close 均排除这些任务。loop teardown 会取消但不等待拒绝取消的 detached close，执行放弃回调释放 orphan/token 后关闭循环，因此永久不释放的 close 也不能阻止运行时线程退出。
- 精确 poisoned/requeue、24 并发与永久 stubborn shutdown 回归：`14 passed`。
- 完整 runtime：`31 passed, 1 skipped`；完整 checkpoint：`63 passed`。
- 视频专项：`451 passed, 15 skipped`；跳过项均依赖未配置的 `MTV_TEST_POSTGRES_URL`。
- 全量 Ruff、全量 Mypy（`142 source files`）与 `git diff --check`：通过。
- 本轮未启动服务、容器或监听端口，也未执行真实 Redis+prefork kill；Task/Request 契约与本地依赖源码已覆盖 ACK/requeue/recycle 的控制流，真实 broker 投递时序仍属于基础设施环境的补验项。

## 第四轮复审补充

- Volcengine 生成与取消不再使用独立的 `asyncio.wait_for(client.close())`，均接入 `managed_video_client`，统一复用容量为 1 的跨线程 close token、detached task 登记、orphan 回收、runtime poisoning 与 shutdown 放弃回调。Ark close 适配器同时兼容同步返回和 awaitable 返回。
- 取消保持尽力语义：普通 close 失败或超时不会覆盖已经取得的远端取消结果；`asyncio.CancelledError`、`SoftTimeLimitExceeded` 与 `TimeLimitExceeded` 从取消请求、close 及外层入口原样传播，不再被通用异常分支转换为 `FAILED`。
- 旧实现红灯为 `4 failed`：生成/取消吞 cancellation 的 close 均等待约 `0.15s`，24 个跨事件循环 close 全部启动，且顽固 close 会使 `WorkerAsyncRuntime.shutdown()` 返回后运行时线程仍存活。统一生命周期实现后的核心回归为 `6 passed`，包含 soft/hard time limit 两个参数化用例。
- 完整 runtime：`32 passed, 1 skipped`；完整 checkpoint：`63 passed`；视频专项：`457 passed, 15 skipped`。跳过项均依赖未提供的 `MTV_TEST_POSTGRES_URL`。
- 本任务 scoped Ruff、全量 Mypy（`142 source files`）与 scoped `git diff --check` 通过。全量 Ruff 已运行，仅并行修改范围 `tests/test_media_upload_state_machine.py:3` 存在一项与本任务无关的 `I001`，未越界修改。
- 本轮未启动服务、容器、后台进程或监听端口，未访问真实 Provider、数据库或 broker。

## 第五轮复审补充

- 服务入口控制流：`_best_effort_cancel_remote_video_task` 不再把 Provider 抛出的 `asyncio.CancelledError`、`SoftTimeLimitExceeded` 或 `TimeLimitExceeded` 转换为 `VideoCancellationOutcome.FAILED`；三类控制流信号在通用异常降级前原样重抛，其他远端取消异常仍保持本地 canceled 优先和 retryable debt 语义。
- 全链路回归使用真实 Volcengine Provider，并让 `managed_video_client` 管理的 Ark close 分别抛出 soft/hard limit；helper 与外层 `cancel_video_job` 两组参数化测试均验证同一异常实例一路传播。外层信号发生前，本地 canceled 与 pending provider cancellation debt 已先提交，不会因 worker 时限中断丢失取消事实。
- 旧实现精确红灯为 `4 failed`，均显示 soft/hard 被 helper 捕获并记录为远端取消失败；最小服务层修复后为 `4 passed`。完整取消提交不确定性文件为 `21 passed, 2 skipped`，`test_video_generation.py -k cancel` 为 `34 passed`。
- 可靠性四文件为 `159 passed, 3 skipped, 1 failed`；视频专项（排除受保护的 `test_providers.py`）为 `426 passed, 15 skipped, 2 failed`。失败均为并行存储键规范化后，旧断言仍期望 `/generate_images/...` 前导斜杠，与本轮异常控制流无关，未越界修改。
- 全量 Ruff、全量 Mypy（`142 source files`）与 scoped `git diff --check` 通过；受保护 `test_providers.py` 未修改、未暂存。本轮未启动服务、容器、后台进程或监听端口。

## 第六轮复审补充

- Volcengine 恢复 `_close_client_safely(client)` 向后兼容入口，以兼容既有扩展和测试 monkeypatch；该方法只 `await self._close_client(client)`，不捕获异常、不持有超时常量，也不调用 `asyncio.wait_for`。同步/异步 Ark close 的调用适配仍由 `_close_client` 完成。
- 生成与取消的 close callback 均在 `managed_video_client` 内调用兼容入口，因此固定容量 token、detached task、orphan 登记、runtime poisoning、shutdown 放弃回调及吞取消 close 的硬截止均未下沉到兼容方法，旧 helper 无法绕过统一生命周期。
- 兼容 hook 红灯证明旧代码未调用该入口；修复后受保护 `test_providers.py` 单一回归、同步/异步委托、兼容 hook 吞取消硬截止、生成/取消硬截止与 runtime shutdown 共 `7 passed`。完整 streaming 为 `98 passed`，checkpoint 为 `63 passed`，runtime 为 `32 passed, 1 skipped`。
- 全量 Ruff、全量 Mypy（`142 source files`）与 scoped `git diff --check` 通过；受保护测试只读执行，未编辑、未暂存。本轮未启动服务、容器、后台进程或监听端口。
