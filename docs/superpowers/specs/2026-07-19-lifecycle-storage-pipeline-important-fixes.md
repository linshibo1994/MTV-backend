# 生命周期存储判权与 Pipeline 历史修复规格

## 目标

修复主体删除的 GenerationJob 外键失败、三个删除入口对共享对象的局部判权，
以及 Pipeline replay/retry 对不可能历史的接受问题。修改必须保留财务审计、幂等
投递和对象存储的事务边界，不通过关闭外键或捕获 IntegrityError 掩盖数据错误。

## 约束

- 不修改视频实现文件、现有媒体上传迁移或其他 Agent 的并行文件。
- 不新增或改写数据库迁移；本轮策略不需要改变外键或 API schema。
- direct subject、episode、script 删除必须复用同一个全局 storage-key 引用判权器。
- PostgreSQL 删除入口只沿用既有父资源锁；引用 writer 与 delete effect 必须对规范化 key
  按稳定顺序取得同一组事务级 advisory lock，不使用全表写屏障，也不跨 provider I/O
  持有数据库锁。
- SQLite 测试必须显式启用 `PRAGMA foreign_keys=ON` 验证真实外键行为。
- 每个问题先提交失败测试证据，再实现修复；本轮不提交 Git commit。

## A. 主体删除策略

### 产品证据

- `docs/Claude/02-功能规格文档.md` 将删除主体定义为“移除该主体/删除该主体行”。
- API `DELETE /scripts/{script_id}/asset-library/subjects/{subject_id}` 当前语义为
  “连带清理其资产条目”，没有归档或 `confirm` 参数。
- script/episode 删除的现有权威策略为：活动、待投递或可重试财务清理任务 409；
  终态任务先锁定并取消 outbox，再随业务资源硬删除；CreditReservation/CreditLog
  以无 FK 的 `source_id` 保留财务审计。

### 决策

- direct subject 沿用相同策略。`starting/processing` 图片任务，或
  `failed + error_type=financial_cleanup + retryable=true` 的任务返回明确 409，
  响应包含阻断任务数量。
- terminal GenerationJob 在 `Script -> Subject -> GenerationJob -> Outbox` 固定锁序中
  取消活动 outbox 后删除；CreditReservation、CreditLog 与已取消/终态 outbox 保留。
- AssetEntry 候选就是当前候选事实，仍按既有外键拓扑连带清理。
- API 将主体删除、成功审计和 commit 放在同一事务；任一步失败统一 rollback，
  不允许出现主体已删但审计缺失。

### 验收

- 开启 SQLite FK 的真实 API 回归先复现 terminal GenerationJob 导致 500/IntegrityError，
  修复后返回 200，主体、资产、terminal job 删除，outbox 已取消，credits 保留，审计存在。
- 活动任务和财务清理债务均返回 409，主体及所有依赖保持原样，不写成功审计。
- 人为制造审计失败时整笔删除回滚。

## B. 全局 storage-key 引用判权

### 候选与保留引用

候选 key 先通过 `validate_key_prefix` 规范化。全局保留引用扫描排除本事务会删除的
记录后，覆盖：

- `AssetEntry.storage_key`；
- `MediaUpload.temp_storage_key/final_storage_key`；
- `Episode.script_file_storage_key`；
- `GenerationJob.output_json/metrics`；
- `VideoGenerationJob.input_json/output_json/metrics`；
- `TimelineRenderJob.input_json/output_json/output_storage_key`；
- `StoryboardJob.detail`、`PipelineJob.output_json`；
- `ShotGroup.reference_frames/materials/audio_elements`；
- `CanvasNode.data` 与 `CanvasEdge.data`。

任何保留记录命中候选 identity，都保护该 key，不取消其必要 promote，也不写物理
delete。判权候选不是调用方初始集合，而是沿 promote 的 source/final 双向收敛所得的
完整闭包；活动 promote 的 final 被保护时，反向保护它仍需消费的 source，并继续多级
传播。删除上传拥有的 temp 只依据该上传是否仍需 promote：staging 且 final 被保护时
保留 temp；非 staging 的已失去业务所有权 temp 可清理。无引用 key 仍必须递归清理。

### 锁序与复杂度

- 入口先沿用 `Script -> Episode/Subject` 父锁。
- intent 阶段不再对引用表或 storage job 表取得全表锁；全表 barrier 为 no-op。普通 FK
  writer 与父行删除因此不会形成父行锁、子表 DML、全表锁之间的反向等待。
- 所有规范化 key 在 writer guard 与 delete effect 边界使用稳定排序的 PostgreSQL 事务级
  advisory lock；锁 ID 由 SHA-256 前 64 位映射为有符号整数。provider delete 只在
  `deleting` 已单独提交且数据库事务与 key lock 均释放后执行。
- promote 按稳定行序沿 source/final 与待删除 asset id 做有界闭包；
  查询分块 500，最多 10000 个 key、5000 行和 128 轮，超限明确返回冲突。
- 完整扩展候选必须重新执行同一 11 表全局引用扫描并保留调用方 exclusions；只取消
  final 最终可删除的 promote，delete intent 只来自最终闭包且使用稳定幂等键。
- 标量引用在数据库端使用 `IN` 预筛，PostgreSQL JSONB 使用参数化
  `jsonb_path_exists` 预筛；SQLite fallback 和 promote 查询均在执行前应用硬上限，禁止
  无界 `.all()` 物化。
- episode 不得再对每个 subject 重复扫描 GenerationJob；script 收集必须包含
  `GenerationJob.metrics`。

### 第六轮 durable delete 与引用写入协议

第五轮把 provider delete 放在数据库写屏障事务内，虽然关闭了 intent 与 effect 之间的
TOCTOU，却无法原子提交数据库和对象存储：对象已删后进程崩溃或数据库 commit 失败时，
数据库会回到可重试状态，但对象不可恢复；随后新增引用会把任务延期成永久悬空引用。
远端 delete 超时的请求结果不确定与此同构。第六轮使用现有 `media_storage_jobs` 行作为
每个 source key 的 durable tombstone，不新增表或迁移：

- delete job 在第一次物理 effect 前由 `running` 原子进入 `deleting` 并单独 commit；
  `deleting`、`succeeded` 以及 effect 后的 terminal failure 永久禁止新增引用。pre-effect
  failure 必须与 effect 后 terminal failure 可区分，不能误把未删除对象永久封死，也不能
  因 attempt 耗尽重新开放已经进入 effect 的 key。
- PostgreSQL 对所有规范化 key 使用事务级 advisory lock。多 key 必须按规范化 key 的稳定
  排序获取；锁 ID 使用跨进程稳定的 SHA-256 前 64 位有符号整数，hash 碰撞最多扩大串行
  范围，不能影响 tombstone 查询和最终引用判权的正确性。SQLite 保留串行测试语义。
- 引用 writer 与 delete effect 必须获取同一 key lock。writer 在锁内看到
  `pending/retry/running` delete 时，只能在确认尚未进入 effect 后取消；看到 durable
  tombstone 必须 fail closed。覆盖面包括 11 类模型的普通 ORM flush、ORM bulk update、
  JSON 原地 mutation/Mutable、insertmanyvalues；无法可靠拦截的写法必须改为显式 guard
  或拒绝执行，不能依赖调用方自觉。
- delete effect 在 key lock 内执行最终引用扫描并提交 `deleting`，随后释放数据库事务
  和 key lock，再调用 provider。NotFound 按幂等成功处理；timeout、任务取消、进程
  崩溃和 commit failure 均保持 durable tombstone。租约过期的 `deleting` 可按 backoff
  和 attempt 上限重新领取、幂等重删；耗尽后进入不可重新引用的 effect terminal failure。
- intent 阶段的引用扫描只用于减少无效任务，不再申请引用表强锁；最终安全性完全由
  writer guard、per-key lock 和 effect-time 重验保证。因此 Script/Subject/Episode 父行锁
  不会再与普通 FK INSERT 的子表 `ROW EXCLUSIVE` 形成反环。
- 标量引用列必须使用 `IN candidates` 在数据库端筛选。PostgreSQL JSONB 使用参数化
  `jsonb_path_exists` 做候选预筛，再由应用层按规范化规则精确判定，必须保持前导 `/` 等
  合法等价形式；非 PostgreSQL fallback 必须设置明确行数上限，超限返回冲突，禁止无限
  `.all()` 物化。promote 闭包每次查询须在执行前按 remaining 应用 `limit(remaining+1)`。
- media storage worker 空 claim 返回前必须结束当前 `SELECT FOR UPDATE` 事务，不能把空轮询
  的事务或连接继续留给调用方。

第六轮新增验收：

- PG16 上 AssetEntry、GenerationJob、TimelineRenderJob 三类普通 FK INSERT 与父删除并发
  不得再出现 `pg_blocking_pids` 双向等待或 `DeadlockDetectedError`。
- 物理 delete 成功后模拟最终 commit 失败：job 必须保持 durable tombstone；新引用必须被
  拒绝；租约过期后同一 key 可被重新领取并幂等收敛到 succeeded。
- writer 对 pending/running delete 的取消与 worker `running -> deleting` 必须由同一 key
  lock 串行；不能出现“writer 已放行、worker 同时进入 effect”的交错。
- 引用扫描测试必须证明无关标量行不会进入结果、PG JSONB 只返回候选行、fallback 超过
  配置上限会显式失败；批次大小不能导致无限结果物化。
- 多 key advisory lock 顺序、跨进程 hash 稳定性、碰撞保守串行，以及 `deleting` stale
  reclaim/backoff/attempt 耗尽后仍保持 tombstone 均须有专项覆盖。

### 第七轮写入封口、存量等价键与执行边界

第六轮复审继续沿“写入与删除共用 per-key 协议”检查所有旁路，并补齐长任务执行边界：

- 受保护表的 `INSERT ... FROM SELECT` 无法逐值判定，必须 fail closed；生产 Session
  显式拒绝会绕过 mapper/Engine 事件的 `bulk_save_objects`、`bulk_insert_mappings` 与
  `bulk_update_mappings`。Engine `before_execute` 覆盖 Core Connection DML，未编译的
  `exec_driver_sql` 由 cursor 边界拒绝。Alembic 仅在迁移 Connection 上设置显式 bypass，
  不能把迁移隔离扩大为普通业务旁路。
- 普通 ORM、Session/Core DML 在守卫判权前递归 canonicalize 标量和嵌套 JSON 中的合法
  storage key；tombstone 查询对存量 source 使用 `ltrim('/', source_key)`。标量引用预筛
  同样以 `ltrim` 匹配任意前导斜杠；PG JSONB 使用完整绑定的 JSONPath 正则匹配
  `^/*candidate$`，候选正则和 JSONPath 字符串分别安全转义，应用层继续精确规范化复核。
- Celery soft/hard limit 与媒体任务 lease 使用同一组共享常量；lease 至少覆盖 hard limit
  加 60 秒。`claim_next_job` 与 `claim_job` 发现 `attempt >= max_attempts` 时不得生成
  `max+1`：pre-effect 任务进入 `failed`，`deleting` 进入 durable `effect_failed`，均清空
  lease 并直接返回终态 claim，API/worker 不再执行 provider effect。
- 批次边界必须原样透传 `SoftTimeLimitExceeded` 与 `TimeLimitExceeded`，不能依据已有 job id
  将其吞成普通 retry/failed 结果。最终结果事务 commit 抛错时先 rollback 清理 Session；
  错误落库 CAS 未命中后，在新事务按 job identity、upload ready、job/upload asset 一致性
  及候选资产行约束复读权威状态。只有完整不变量为真才收敛成功，未知或不一致继续失败关闭。

第七轮新增验收：

- from-select、两类 legacy bulk、Core Connection tombstone 绕过与标量/嵌套 JSON
  canonicalization 均须先红后绿；既有 raw SQL、bulk update 和 same-flush guard 不回归。
- PG16 分别以 1 个和 4 个前导斜杠写入标量/JSONB 引用；在扫描硬上限为 1 时仍只返回
  候选行，SQL 文本不得内联候选 key。
- worker 实际 claim lease 必须不少于 Celery hard limit 加 60 秒；按队列和指定 ID 领取
  stale promote 时，attempt 均不能超过 max，且不得执行对象存储 I/O。
- soft/hard timeout 两参数项必须向 Celery 透传；模拟最终 commit 已应用但 ACK 丢失时，
  job/upload 必须权威收敛为 succeeded/ready，provider promote 只调用一次。

### 验收

- 跨 subject、跨 episode、跨 script 共用 key 时，删除一侧不产生 final delete intent。
- AssetEntry、MediaUpload、GenerationJob metrics 及 Video/Timeline/ShotGroup/Canvas
  任一保留模型引用均能保护 final。
- 被保护的 staging upload promote 不被取消；无引用 final 与真实孤儿 temp 正常清理。
- 多级、分支 promote 闭包中，跨剧本引用的中间 key 不误删；无引用中间 key 继续递归
  清理；重复执行不产生重复 delete intent。
- delete intent 提交后由另一会话新增引用时，effect-time 复核不得删除对象、不得 settled
  或消耗 attempt；引用移除后必须成功删除。provider 异常后任务仍可重试并最终成功。
- episode 删除对 GenerationJob/全局引用的扫描次数为常数，不随主体数增长。

## C. Pipeline 权威历史状态机

### attempt 不变量

- 五个规定 stage 必须完整且不能出现未知 stage；每个 stage 都有相同 baseline attempt。
- 同 stage attempt 唯一、从 baseline 连续递增且作用域/input/dispatch 完全一致。
- 每个非最新 attempt 必须是 `failed` 且 `retryable=true`；只有它才能产生下一 attempt。
- 非叶子 stage 合法最新态：`pending/running/done/failed/skipped`；叶子额外允许 `ready`。
  未被 worker 写入的未知状态和非叶子 `ready` 均非法。

### DAG 不变量

- 一个 stage 的实际执行证据定义为：baseline 状态不是 `pending`，或存在任意高于
  baseline 的 attempt；即使最新 retry attempt 仍是 `pending`，也不能抹掉历史证据。
- `extract` 有实际执行证据时，最新 split 必须为 done。
- `merge` 有实际执行证据时，最新 split/extract 必须均为 done。
- `enrich/parents` 有实际执行证据时，最新 split/extract/merge 必须均为 done。
- 任一叶子为 done 时，另一个叶子也必须 done，因为 finalizer 在同一事务同时写入。

### 复用与验收

- replay 与 `_load_retry_source/_validate_retry_source` 复用同一 validator。
- retry source 只有在完整历史合法、各 stage 最新态中存在 retryable failed 时才可重试；
  新 run baseline 为合法历史最大 attempt + 1。
- 合法 baseline + 单 stage retry 的 pending/running/done/failed replay 继续通过。
- done 后重试、非 retryable failed 后续 attempt、未知/缺失 stage、跳号、非法前置与
  单叶 done 均返回 409；未知 stage attempt 不能影响新 run attempt。

## 执行记录

- 已完成文档、API、模型、worker 调用链调查，并确定 direct subject 删除沿用
  script/episode 的硬删除与财务保留策略。
- A 正式红灯共 4 项：开启 SQLite 外键后的终态任务删除、活动/财务债务阻断、
  严格审计失败回滚均先失败；实现后 4 项通过。生产实现将任务锁定、outbox 取消、
  主体删除、严格审计与 commit 纳入同一事务，异常统一 rollback。
- B 三个跨入口共享 key 红灯均先失败；实现后 direct subject、episode、script 统一
  使用全局引用判权器。模型矩阵覆盖 11 类模型、19 个持久字段并通过；episode 在
  1 个与 4 个主体下的 GenerationJob SELECT 次数相同。第四、五轮曾在父资源锁后建立
  表级写屏障；第六轮以 per-key writer 协议取代该屏障。
- 第四轮 B 正式红灯 2 项：`C -> B -> A` 与 `D -> A` 闭包中，跨剧本 AssetEntry/
  MediaUpload 保留 B 时旧实现误删 B；B 无引用时旧实现又未发现并清理 `C -> B`。
  修复后有界锁定完整 promote 闭包，对闭包重新执行同一 11 表判权并反向传播必要
  source；两种情形均通过。另连续执行两次验证 delete intent 幂等。
- 第五轮 B 正式 PG 红灯 2 项：subjectless/video success finalize 和 failure finalize
  均先持 Upload，再请求 Job DML；第四轮临时 barrier 却先持 Job `EXCLUSIVE` 后等待
  Upload。PostgreSQL 16 分别报告 ShareRowExclusive/RowExclusive 与事务锁/RowShare
  的 `DeadlockDetectedError`。修复后 barrier 固化为 Upload EXCLUSIVE、其余 10 表、
  Job EXCLUSIVE 三段顺序，原 nodeid 两参数 `2 passed`；完整 media 状态机 `44 passed`。
  第六轮进一步证明该顺序仍会与普通 FK INSERT 形成反环，因此已完全移除表级 barrier。
- effect-time 红灯证明旧 worker 会删除 intent 提交后新增引用的对象。修复后活动引用使
  delete 回到 retry 且 attempt 保持 0，逻辑删除资产或移除跨会话引用后同一 intent 以
  attempt 1 成功；这是第五轮的过渡实现。第六轮改为 effect 前 durable `deleting`
  tombstone，provider 超时、崩溃或最终 commit 失败均不能重新开放该 key。
- 第六轮新增 PG 验收 8 项全部通过：AssetEntry、GenerationJob、TimelineRenderJob 三类
  FK writer 只有单向等待；覆盖最终 commit failure 后 tombstone/reclaim、writer 与 worker
  两种 advisory lock 获胜顺序、PG 标量/JSONB 预筛及更新后的 pre-effect cancel 收敛。
- writer guard 已覆盖普通 ORM、insertmanyvalues、bulk Insert/Update、JSON
  `flag_modified` 与 Session raw SQL 拒绝；same-flush 同时检查 `session.new` 和
  `session.dirty`，避免既有 job 改为 `deleting` 时新增引用覆盖 guard 结果。
- 第七轮本地正式红灯 11 项全部按原 nodeid 转绿：from-select 1 项、legacy bulk 2 项、
  Core Connection 1 项、标量/嵌套 JSON canonicalization 1 项、lease 1 项、两种 claim
  attempt 封顶 2 项、soft/hard timeout 2 项及最终 commit ACK 丢失 1 项。PG 多前导斜杠
  预筛另有 2 参数项先复现 4 斜杠漏判，修复后均通过。
- 第七轮两份 storage 套件本地为 `61 passed, 16 skipped`；注入既有 PG16 容器后为
  `79 passed`，覆盖全部 PostgreSQL 锁序、commit uncertainty、多前导斜杠预筛和真实
  HTTP 媒体流程。
- C 正式红灯共 9 项，覆盖缺失/未知 stage、非法 attempt 历史、不可能 DAG 与 retry
  source 绕过；实现后 20 项专项通过，`tests/test_pipeline_runs.py` 全量 26 项通过。
  原不真实的 extract 重试 fixture 已改为 worker 可达的 enrich 叶子重试历史。
- 第四轮 C 正式红灯 1 项：split 从未 done、extract baseline 已 failed 且 retry attempt
  仍 pending 时，旧 validator 只看 latest 而同时放过 replay 与 retry source。修复后
  validator 对每个 stage 汇总完整历史执行证据；五阶段 baseline 全 pending 和 split
  done 后 extract failed 再 retry pending 两个合法对照继续通过。
- 最新七份相关回归在任务专用 PostgreSQL 16 上运行：
  `.venv/bin/python -m pytest -q -ra tests/test_asset_library.py`
  `tests/test_script_episode_lifecycle.py tests/test_storage_references.py`
  `tests/test_pipeline.py tests/test_pipeline_runs.py tests/test_success_audit.py`
  `tests/test_media_upload_state_machine.py`，结果 `345 passed`；两份 storage 套件在 PG
  条件全开时为 `79 passed`，Timeline 全量为 `59 passed`。
- 最近一次全仓回归为 `1424 passed, 1 failed, 10 warnings`；唯一失败是并行视频可靠性任务
  中 Volcengine 测试仍 monkeypatch 已移除的兼容入口，不涉及本 Spec 的生命周期与存储行为，
  对应任务随后恢复兼容入口并完成精确回归。警告仅来自 Alembic `path_separator` 弃用；
  `.venv/bin/ruff check app tests alembic/env.py`、`.venv/bin/mypy app alembic/env.py`
  （143 个源文件）与 `git diff --check` 均通过。
- 完整测试首轮暴露两项测试基础设施问题：URL 编码密码中的 `%21` 被 Alembic
  `ConfigParser` 误作插值，已在 `alembic/env.py` 对 `%` 做配置层转义，媒体上传迁移
  `22 passed`；owner 成员迁移失败测试曾把共享主库停留在旧 revision，现改为随机临时
  数据库并以注册目标、head、sentinel 与 public schema fingerprint 约束，专项 `3 passed`。
- 测试使用本机既有 PostgreSQL 16 容器的 55438 端口；审计确认 revision 为
  `f4a8c2e1d907`、`media_uploads` 存在、临时数据库/活动测试连接/advisory locks/本任务
  随机前缀数据均为 0。未修改容器凭据，也未停止既有容器。残余风险仅为远端对象存储
  真实高延迟尚未压测；provider I/O 已不持有数据库锁。任务未启动服务、未修改视频实现
  或迁移、未提交或推送。
