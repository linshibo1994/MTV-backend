# 后端完整修复设计

> 日期：2026-07-16
> 状态：已完成方案讨论，等待书面 Spec 复核
> 范围：仅 `backend/` 后端及其测试、迁移和后端工程配置；不验证或修改前端

## 1. 背景与目标

后端 T0-T10 独立审计已经完成，结论记录在
`docs/Claude/BACKEND-FUNCTION-MATRIX.md`。现有实现具备主要模型、API 和服务骨架，
但在事务一致性、租户隔离、异步任务状态、媒体安全、部署产物和专项测试方面存在
确定性阻断或不完整实现。

本修复的目标是：

1. 修复功能矩阵及各独立审计报告中发现的全部后端缺口，不只修复 P0。
2. 保持 `TASK.md`、`PLAN.md` 与 01-09 规格中的后端契约，不扩展前端范围。
3. 使用统一的事务、幂等、取消、版本栅栏和资源授权机制闭合跨模块链路。
4. 为 T0-T10 建立可重复执行且不调用真实收费 AI 的测试证据。
5. 使所有适用于本项目的测试、Ruff、Mypy、导入、OpenAPI 和迁移验证通过。

## 2. 约束与非目标

- 不读取或修改 `.env*`、密钥、证书、生产配置和 `secrets/**`。
- 不调用真实 OpenAI、Google、火山引擎、jianyu、modelsai 或其他收费服务。
- Provider 测试只使用确定性 fake、mock 或本地样例媒体。
- 不连接、迁移或操作生产数据库；不执行生产部署和远程推送。
- 初始 Alembic migration 只有在用户单独明确授权后才生成；授权范围限定为生成、
  静态审查、空临时数据库升级/降级验证，不包含任何生产数据库操作。
- 不顺带重构无关模块；只处理功能矩阵、审计报告和验证命令直接暴露的问题。
- 用户原有工作区变更不回滚、不覆盖、不提交。
- 本 Spec 是本次修复的执行约束；`docs/Claude/PLAN.md` 中带省略号的伪代码仅为历史示意，
  其中真实 LLM 可选运行、填写 `.env` 和直接 `alembic upgrade head` 的步骤不用于本次验收。
  Provider 实现保留真实接入能力，但所有自动验收固定使用 fake Provider。

## 3. 方案选择

采用“风险优先、分阶段集成”的方案，而不是一次性全仓并发修改，也不是只修 P0。

- 一次性全仓修改能缩短表面工期，但共享模型、Celery 注册和积分服务容易产生冲突，
  回归定位成本过高。
- 只修 P0 可以快速恢复部分主路径，但无法满足“修复所有问题”和横向验收要求。
- 分阶段方案先建立可靠运行语义，再完善业务边界和部署产物；每阶段可独立测试、
  审查和提交，失败时也能准确定位。

## 4. 全局设计原则

### 4.1 租户和资源授权

所有 personal/team 资源读取与写入均采用同一顺序：

1. 验证会话和用户状态。
2. 从会话解析 `active_org_id`，确认用户仍属于该组织。
3. 加载目标资源，并验证资源所属 script、episode、subject、material、shot group 或 job。
4. 校验 personal 所有权或 team 项目成员角色。
5. 校验动作权限和资源当前状态。
6. 记录审计；审计设施故障不得把预期 401/403 改写为 500。

平台管理员与组织管理员分离。`is_admin` 不再隐式绕过组织边界；组织级管理动作必须
绑定活动组织和明确 management scope。所有 URL 中的资源 ID 都重新做归属校验，
不信任同时由客户端提交的 `script_id`。

已认证用户请求一个确实存在但无权访问的跨租户或他人资源时统一返回 403，满足 TASK
的显式验收契约；资源在数据库中确实不存在时返回 404。401、403、404 的选择不因审计
设施是否可用而变化，并由同一套 IDOR 参数化测试固定。

### 4.2 异步任务统一协议

流水线、生图、分镜、视频和时间线渲染遵循同一协议：

1. 在数据库事务中校验权限、输入版本和幂等键；创建任务、预留积分，并写入
   `TaskDispatchOutbox`。三者同事务提交。
2. API 提交后可以立即唤醒 outbox dispatcher，但不直接把一次 `delay()` 成功当成可靠投递。
   dispatcher 发布成功后标记 outbox published；Broker 不可用时保留 pending 并指数退避。
3. dispatcher/reconciler 定期扫描 pending outbox、无 outbox 的非终态任务和过期 lease。
   因进程在 commit 后崩溃而遗漏的任务会被重新发布；重复发布由 worker CAS 去重。
4. worker 使用条件更新原子领取 `starting/pending -> processing/running`，重复投递直接退出。
5. Provider 返回后先验证结构、数量、媒体类型和输入版本，再持久化产物。
6. 业务成功与积分 settle 必须形成一致终态；settle 非幂等冲突失败时不得写 succeeded。
7. 失败路径先回滚当前事务，再在新事务中记录结构化错误并 release。
8. cancel 使用终态 CAS。worker 在 Provider 前后及写产物前检查取消/版本栅栏，
   canceled 任务不能写回结果、尾帧或成功状态。

幂等键必须进入数据库列和唯一约束，不只保存在 JSON 中。每类任务保留输入版本、
attempt、错误类型、可重试标记和终态时间，便于重试且不改写历史 attempt。

`TaskDispatchOutbox` 至少包含 `id/task_type/job_id/attempt/status/available_at/publish_attempts/`
`last_error/published_at/created_at`，并唯一约束 `(task_type, job_id, attempt)`。dispatcher 使用
`FOR UPDATE SKIP LOCKED` 批量领取。发布后、标记 published 前崩溃只会造成重复投递，不会
丢任务。

任务 lease 至少包含 `lease_owner/lease_token/leased_until/heartbeat_at`。默认租期 5 分钟，
执行长调用时每 60 秒续租；只有匹配 token 的 worker 能续租或写终态。reconciler 可以重领
已过期且未取消的任务，attempt 保持同一投递尝试；用户发起显式重试才创建新的历史 attempt。

### 4.3 并发和一致性

- 积分账户使用 `SELECT FOR UPDATE` 且强制 `populate_existing`，避免 identity map 旧值。
- 余额、冻结额、状态、唯一幂等键和单活 Provider 由数据库约束兜底。
- 资产审核、画布 delta、任务领取和取消都用版本或状态条件更新；并发失败返回 409。
- `ensure` 类接口依靠唯一约束加冲突后重查，而不是“先查后插”。
- 列表排序增加稳定次序键，避免时间戳相同时分页漂移。

### 4.4 积分五段状态机

积分服务采用下列精确语义：

| 操作 | 允许前态 | 结果与幂等规则 |
|---|---|---|
| reserve | 无预约、账户 active | `estimated > 0` 且可用余额充足；同 source 和相同参数返回原预约，不重复冻结；载荷不同返回 409 |
| settle | reserved | `actual >= 0`；少于预留时释放差额，超过预留时在账户锁内检查额外可用余额；不足返回 402，不写业务成功 |
| release | reserved | 释放全部冻结并转 released；重复 release 返回原终态；captured/refunded 返回 409 |
| refund | captured | 退回已捕获的 `actual_points` 并转 refunded；重复 refund 返回原终态；reserved/released 返回 409 |
| grant | active source/target | 双账户按稳定 ID 顺序加锁，actor/target 完整落流水；同幂等键同载荷只执行一次 |

captured 后重复 settle 只有 `actual_points` 完全相同才作为幂等成功，不同值返回 409。disabled
账户禁止新的 reserve 和 grant 支出，但允许 settle/release/refund 清算已有预约。settle 因额外
余额不足或存储失败时，调用方删除未正式发布的产物、将任务标为 failed，并可靠 release；
不能留下 succeeded 业务状态。显式重试创建新 job attempt 和新 source ID，旧预约终态不可改写。

每次写入后校验 `balance_points >= 0`、`frozen_points >= 0`、
`frozen_points <= balance_points`，流水使用 `(created_at DESC, id DESC)` 稳定排序。

### 4.5 媒体边界

- 本地存储使用解析后的绝对路径和真正的父子路径判断，拒绝 `..`、相似前缀和符号链接逃逸。
- 上传扩展名不作为可信 MIME；图片由 Pillow 解码，音视频由 ffprobe 或等价探测验证。
- 上传先写临时对象，探测和数据库提交成功后再转正；任意错误都清理临时及孤儿对象。
- 媒体代理从 storage key 反查有权资源，覆盖生成图、上传图、视频、抽帧和渲染产物。
- Range 解析严格遵循 HTTP 语义；非法或不可满足范围返回 416。
- Provider 返回 URL 仅允许配置的 HTTPS 主机，并阻断本机、私网、重定向到私网和超限下载。
- S3 配置或依赖错误在启动/构造时明确失败，不静默回退本地存储。

媒体参数固定如下，并允许通过非敏感配置收紧：

- Provider 下载只接受 `https`；允许主机使用精确域名列表，不允许通配符。最多 3 次重定向，
  每一跳重新解析并拒绝 loopback、link-local、私网、保留地址和域名/IP 不一致。
- 连接超时 10 秒、总超时 120 秒，流式下载上限默认 512 MiB；超限返回任务
  `provider_media_invalid`，并删除已写临时对象。
- 图片 MIME 白名单为 PNG/JPEG/WebP；音频为 MPEG/WAV/MP4/AAC/OGG；视频为
  MP4/WebM/QuickTime。探测工具缺失或解析失败均拒绝上传；不按扩展名降级放行。
- 只支持单一 byte Range；多 Range、非法语法、start 超界或 end 早于 start 返回 416，
  响应携带 `Content-Range: bytes */<total>`。
- `signed_url(key, expires_seconds=300) -> str` 的有效期限制在 1-3600 秒。本地实现返回带鉴权
  的媒体代理 URL，不暴露文件路径；S3 实现返回对应期限的预签名 URL。

## 5. 分阶段工作流

### 阶段一：确定性运行阻断与基础契约

本阶段恢复可执行主链路，并建立后续修复依赖的测试底座。

- T0：补 CORS 配置、生产 Session secret 校验、严格错误体、稳定列表契约和依赖锁。
- T1：修复拒绝审计耦合，并更新过期权限测试夹具。
- T3：使用 Celery `chain`/`group` canvas；统一 Subject 与 ParentSubject 关系；增加输入版本栅栏；
  清理重跑 stage 旧状态；保证 attempt 唯一和结构化错误；breakdown 返回真实层级。
- T6/T7：注册 generation/storyboard worker，建立原子任务领取。
- T8：调整静态路由优先级，确保 active-provider 和 check-materials 可达。
- T9：修复 storage 工厂/API、用户主键、ffmpeg trim 参数和可持久化渲染状态。
- T10：修复本地路径逃逸和不可授权的视频/帧 storage key。

阶段验收：应用导入、OpenAPI 路由、Celery task 注册和上述回归测试通过；fake Provider 下
流水线、生图、分镜、视频和渲染任务均能到达确定终态。

### 阶段二：积分、租户与安全闭环

- T1：活动租户贯穿剧本列表/创建、积分和资源授权；限制管理员组织范围；补 Cookie
  篡改、过期、logout、switch-org、成员移除和关键审计事件测试。
- T2：修复账户锁刷新、禁用账户清算、部门向同部门用户下发、actor/target 字段、
  稳定流水排序、预约幂等和失败回滚；禁止结算失败后业务成功。
- T6/T7/T8/T9：补全部 IDOR、Viewer 取消、无积分免费调用、broker 投递失败、
  重复冻结和取消竞争。
- T8/T10：修复 Provider URL SSRF、真实 MIME 校验、对象清理、Range 416、代理授权、
  `signed_url` 契约和 S3 fail-fast。

阶段验收：多租户矩阵、积分五段服务、并发/重复请求、媒体攻击样例和各任务失败补偿测试通过。

### 阶段三：业务完整性与边界行为

- T3：对各阶段 LLM JSON 做 schema 校验；强制 enrich 12 段结果；声音主体不进入视觉生图；
  生图 resolved prompt 只能由服务端 enrich 结果生成；episode_no 限制为正整数。
- T4：修复资产 ensure 竞态、历史 approved 绕过、首次双审核覆盖、上传候选 pending 同步、
  删除候选状态和主体改名后的引用影响计算。
- T5：增加画布版本 CAS；禁止孤儿边；规范并完整 round-trip 节点 data；接入 prompt helper。
- T6：增加 Idempotency-Key、取消 API、lease/CAS、按实际成功图片数量结算、候选队列同步，
  并禁止客户端伪造 resolved prompt。
- T7：StoryboardJob 绑定 episode_version；禁止并发重复任务和旧任务覆盖；校验三层输出、
  description 模板和九条约束；图片与音频审核状态分离。
- T8：增加视频幂等列、单活 Provider 约束、镜头组归属校验、参考图缺失错误、音频过滤、
  自然序号排序、首尾帧取消栅栏和按实际结果结算。
- T9：补 arrangement 关联完整性、稳定 row_order、持久化 RenderJob、写权限取消和恢复能力。
- T10：封面按 episode/scene/group 的自然序正确选择真正首镜头首帧。

阶段验收：T0-T10 专项单元/API 测试通过；每个正常行为有正向证据，每个 IDOR、并发、
幂等、取消、失败补偿和输入校验缺口至少各有一个能在旧实现上失败的负向回归测试。

### 阶段四：数据库产物与全链路验收

该阶段的 migration 生成必须先取得单独授权。

- 补数据库 CHECK、FK、唯一约束、幂等键、单活 Provider、画布版本、任务输入版本和 RenderJob。
- 生成覆盖全部 ORM 的初始 Alembic revision，人工审查 upgrade/downgrade，不读取任何真实配置。
- 在任务专用临时 PostgreSQL/Redis 或隔离测试容器中验证空库 upgrade、模型一致性和 downgrade。
- 使用 fake LLM/图片/视频 Provider 跑一条完整后端链路：剧本到时间线渲染。
- 更新功能矩阵，将每个状态改为基于新鲜测试证据的最终结论。

阶段验收：迁移验证、完整后端链路、全量测试和静态检查均通过，任务启动的资源全部释放。

## 6. 数据模型决策

- `Subject.parent_id` 明确引用 `parent_subjects.id`；子主体保持 `is_child=true`，不再误写
  `is_parent=true`。父主体本身不伪装为 Subject 行。
- CreditAccount 增加非负及 `frozen_points <= balance_points` CHECK；预约 source 幂等采用
  数据库唯一键；清算操作允许 disabled 账户释放历史冻结。
- GenerationJob、StoryboardJob、VideoGenerationJob 和 RenderJob 均持久化幂等/输入版本、
  attempt、错误分类、可重试性和终态信息；具体字段优先复用现有命名。
- VideoProviderConfig 使用数据库部分唯一索引保证每个 job_type 只有一个 active Provider。
- Materials 增加单调版本；CanvasEdge 的 source/target 必须属于同一 material。
- AssetEntry 增加 `lifecycle_status=candidate|approved|superseded|deleted` 和可空
  `superseded_by_id`；`is_approved` 作为兼容字段与 lifecycle_status 保持约束一致。
- VideoTimelineArrangement 对所属 script/episode/job 建立完整约束；同一时间线 row_order 使用
  `DEFERRABLE INITIALLY DEFERRED` 唯一约束，使同一事务内交换顺序不会产生中间态冲突。
- CanvasNode 对 `(material_id,id)` 建唯一键，CanvasEdge 用两组复合 FK 保证 source/target
  均属于同一 material。
- TaskDispatchOutbox 为所有异步任务提供 durable dispatch；其生命周期独立于 Celery result backend。

### 6.1 RenderJob 契约

`RenderJob` 持久化字段为：`job_id/script_id/episode_id/owner_user_id/idempotency_key/`
`timeline_version/status/progress/progress_stage/progress_message/error_type/error_message/retryable/`
`input_json/output_storage_key/attempt/lease_owner/lease_token/leased_until/heartbeat_at/`
`started_at/finished_at/created_at/updated_at`。`input_json` 保存创建时按 row_order 固化的片段、
视频 job、trim 和 timeline version，worker 不在执行中重新读取可变时间线。

沿用现有端点，不增加前端依赖：

- `POST /v3api/video-timeline-arrangements/render`：要求 RUN_AI，接收
  `{script_id, episode_id}` 和可选 `Idempotency-Key`，返回 202 `{job_id,status}`。
- `GET /v3api/video-timeline-arrangements/render/{job_id}`：要求对所属剧本 READ，返回
  `job_id/status/progress/progress_stage/progress_message/error_message/storage_key`。
- `POST /v3api/video-timeline-arrangements/render/{job_id}/cancel`：仅 owner 或对所属剧本
  具有 EDIT/RUN_AI 的成员可调用；Viewer 返回 403。终态重复取消返回 409，不改写历史终态。

RenderJob 是状态权威来源，Redis 只作可丢失缓存；缓存 miss 必须回源数据库。恢复时 outbox
重新投递 starting 或 lease 过期的 processing job，worker 按 lease token 和 timeline version 写终态。

### 6.2 check-materials 契约

正式端点为
`GET /v3api/video-generation/check-materials?script_id=<id>&shot_group_id=<id>`。
服务先加载 shot group，并验证其 episode 所属 script 与 `script_id` 相同，再执行剧本 READ 授权；
不存在返回 404，存在但跨剧本/无权返回 403。

响应固定为：

```json
{
  "ready": false,
  "missing_subjects": [
    {"subject_id": "subject-char-01", "name": "林舟", "type": "character", "reason": "image_not_approved"}
  ],
  "missing_audio": [
    {"subject_id": "subject-char-01", "name": "林舟", "reason": "audio_not_approved"}
  ]
}
```

不存在的镜头组不得返回 `ready=true`。只有所有必需视觉主体存在 approved image_entry、所有
明确要求配音的角色存在 approved audio entry 时 `ready=true`；可选音频不进入 missing_audio。
静态路径必须在 `/{job_id}` 动态路径之前注册，并由 OpenAPI 路由测试固定。

### 6.3 LLM 输出契约

内部 schema 版本统一为 `pipeline.v1` 和 `storyboard.v1`：

- split：`episodes[]` 每项必须含正整数 `episode_no`、非空 `title/script_text`。
- extract/merge：`subjects[]` 每项必须含枚举 `type`、非空 `name/description`、布尔
  `is_child` 和去重正整数 `episode_nos[]`；merge 的同名同类 episode_nos 取并集。
- enrich：每个可视主体必须返回 12 个有序、非空 section 和非空 `resolved_prompts.final`；
  sound 主体跳过视觉 enrich。服务端据此生成生图 input，客户端同名字段被忽略。
- parents：每项必须含 `name/type/description/child_ids/child_count`，child ID 必须属于同剧本，
  `child_count` 必须等于去重 child_ids 数量。
- storyboard：必须形成 scene -> shot_group -> shot 三层；每个 shot 含 content 和完整 description；
  每组 prompt 含全部分时段标记、角色、叙事目的及固定九条约束。

schema 不通过先在同一次 Provider 调用策略内最多重试 2 次；仍失败写
`error_type=provider_contract_error`、`retryable=false`，不得持久化部分结果或写 succeeded。

### 6.4 资产审核状态机

- 无正式资产且首个有效候选写入：Subject `not_designed -> pending`。
- pending 批准候选：候选 `is_approved=true`，Subject `image_entry_id` 指向它并转 approved。
- approved Subject 收到新候选时保持 approved，`pending_candidate_count` 增加；批准新候选时
  旧 AssetEntry 转 `lifecycle_status=superseded`、写 `superseded_by_id` 且
  `is_approved=false`，新条目转 approved，Subject 仍为 approved。
- 删除最后一个候选：没有正式资产则转 not_designed；已有正式资产则保持 approved。
- approve 使用 Subject version 或 `(image_entry_id, updated_at)` CAS；竞争失败者返回 409，
  不覆盖先提交的新正式资产。
- 引用影响按稳定 `subject_id/asset_entry_id` 计算，不按可变名称关联。

图片与音频分别审核；批准音频不得改变图片 review_status，视频汇聚也不得用图片状态代替
音频批准状态。

这些模型变化可先落实在 ORM、纯单元和服务测试中。未取得 migration 授权前，不创建 revision、
不运行任何数据库升级，也不把 PostgreSQL CHECK/FK/部分索引测试标记为通过；相关阶段保持
“逻辑完成、数据库约束未验收”。取得授权后才运行阶段四并闭合总目标。本文的迁移门禁优先于
旧 PLAN 中的普通 `alembic upgrade head` 示例。

## 7. 错误处理契约

- 401 未登录固定为 `{"detail":"未登录"}`。
- 403 表示已认证但无权限；审计写入失败不改变该状态。
- 404 仅用于资源确实不存在；已认证用户访问确实存在但无权的对象返回 403。
- 409 用于乐观锁、重复活跃任务、审核竞争、状态竞争和幂等载荷冲突。
- 402 用于可预期的积分不足；积分设施或结算异常属于任务失败，不伪装为成功。
- 416 用于非法或不可满足的媒体 Range。
- FastAPI/Pydantic 422 也规范为仅含 `detail` 的项目统一错误体。
- worker 错误保存稳定的 `error_type`、安全的 `error_message` 和 `retryable`，不落密钥或响应正文。

## 8. 测试与验证设计

### 8.1 测试层次

1. 纯单元测试：状态机、自然排序、路径/MIME、prompt/schema、积分算术与错误映射。
2. 服务测试：AsyncSession fake 或临时数据库，验证事务、锁、幂等、取消和补偿。
3. API 测试：会话、角色矩阵、租户切换、IDOR、错误体、路由优先级和列表契约。
4. worker 测试：同步调用 Celery task，注入 fake Provider/Storage/Redis，不启动真实 AI。
5. PostgreSQL 集成测试：行锁、唯一约束、部分索引、并发竞争和 Alembic。
6. 后端链路测试：使用 fake Provider 从剧本保存推进到 RenderJob succeeded。

### 8.2 必须通过的命令类别

最终从 `backend/` 运行以下命令；虚拟环境实际路径在执行计划中探测后固定：

```bash
.venv/bin/python -m pytest -q -ra
.venv/bin/ruff check app tests
.venv/bin/mypy app
.venv/bin/python -m compileall -q app tests
uv lock --check
```

- 最终 pytest 允许的 skipped/xfail 数量为 0。真实 Provider 不进入测试收集，不能用 skip 掩盖；
  所有 Provider 测试注入 fake。
- `tests/test_openapi_contract.py` 固定 TASK 中全部端点以及现有 status/cancel/batch/check-materials
  扩展端点的 path、method 和关键 response schema，静态路径优先级也在此测试。
- `tests/test_worker_registration.py` 直接断言 pipeline/generation/storyboard/video/timeline/outbox
  task 已注册，并构造关键 Celery canvas；不依赖运行中的 Broker。
- Ruff 零错误；自动修复后复审语义变化。Mypy 零项目错误；第三方无类型信息只允许最窄的
  单行抑制，并附中文原因。
- `compileall`、应用导入和 OpenAPI 生成必须退出 0；passlib/bcrypt 等兼容告警需要消除，
  不能只隐藏日志。
- fake Provider 端到端测试必须从剧本保存推进到 RenderJob succeeded，并验证积分、资产审核、
  画布、分镜、视频参考图和时间线产物；不以真实服务可用性作为验收条件。
- 获迁移授权后增加 `pytest -q -ra -m postgres`、Alembic
  upgrade/downgrade/upgrade、`alembic check` 和 metadata 对比；同样要求零 skip/xfail。
- `uv.lock` 纳入相关提交；`uv lock --check` 证明 pyproject 与锁一致，不执行未经审查的安装脚本。

任何未运行的验证都必须在最终报告中明确原因、风险和建议命令，不能推断为通过。

### 8.3 TASK 追踪矩阵

详细缺口以 `BACKEND-FUNCTION-MATRIX.md` 为准；下表固定每个二级项的实现阶段和最低证据。
实施计划必须把“证据”展开为实际 pytest node ID，最终功能矩阵回填结果。

| TASK 项 | 阶段与实现责任 | 最低验收证据 |
|---|---|---|
| T0.1 | 阶段一依赖锁；阶段四迁移 | lock check、空库迁移 |
| T0.2 | 阶段一配置/CORS/DI | prod secret、CORS、应用导入测试 |
| T0.3 | 阶段一错误/分页契约 | 401/403/404/409/422 与列表参数测试 |
| T1.1 | 阶段四模型约束 | 五实体 metadata、FK/唯一约束集成测试 |
| T1.2 | 阶段二会话 | login/me/logout、Cookie 属性/篡改/过期、`tgw_l7_route` 测试 |
| T1.3 | 阶段二权限 | 全角色、active org、成员移除、全部资源 IDOR 参数化测试 |
| T2.1 | 阶段二 ORM；阶段四约束 | CHECK/唯一键/流水不可变性测试 |
| T2.2 | 阶段二积分服务 | 五操作、并发、重复、disabled、故障回滚和流水连续性测试 |
| T2.3 | 阶段二积分 API | user/department/org 管理范围及 actor/target API 测试 |
| T3.1 | 阶段一父子关系；阶段四 FK | Subject/ParentSubject 写入和 FK 集成测试 |
| T3.2 | 阶段一 API | 保存乐观锁、真实 breakdown、投递恢复测试 |
| T3.3 | 阶段一/三 worker | DAG、版本栅栏、attempt、outbox、结构化失败测试 |
| T3.4 | 阶段三 Provider 契约 | 五 schema、12 段、sound 跳过和服务端 prompt 测试 |
| T4.1 | 阶段三 ORM；阶段四约束 | AssetEntry/Subject 状态约束测试 |
| T4.2 | 阶段三 API | 10 端点、ensure 并发、候选过滤和分页测试 |
| T4.3 | 阶段三审核 | 全状态迁移、并发 409、替换影响测试 |
| T5.1 | 阶段三 ORM；阶段四约束 | 一剧本一画布、节点/边归属测试 |
| T5.2 | 阶段三 API | delta CAS、刷新一致、孤儿边拒绝测试 |
| T5.3 | 阶段三 schema/prompt | TASK 全字段 round-trip、模型枚举和 prompt 测试 |
| T6.1 | 阶段一/三任务状态 | CAS、lease 超时重领、终态不可改写测试 |
| T6.2 | 阶段三 API | create/get/list、Idempotency-Key、cancel 权限和重复请求测试 |
| T6.3 | 阶段一/三 worker | 注册、fake 路由、多图、实际数量结算和失败 release 测试 |
| T6.4 | 阶段三资产联动 | pending 候选、approved-only、客户端 prompt 伪造拒绝测试 |
| T7.1 | 阶段三 ORM；阶段四约束 | 输入版本、三层关联和历史 attempt 测试 |
| T7.2 | 阶段三 API | generate/status/hierarchy/workbench/cover、并发幂等和 IDOR 测试 |
| T7.3 | 阶段三 LLM-5 | 三层 schema、description、分时段和九约束测试 |
| T7.4 | 阶段三汇聚 | 四类 materials、配音角色、独立音频审核和积分必需测试 |
| T8.1 | 阶段三 ORM；阶段四约束 | 幂等唯一键、单活 Provider 和状态约束测试 |
| T8.2 | 阶段一/三 API | 静态路由、归属、create/list/get/provider/check/batch 测试 |
| T8.3 | 阶段二/三 worker | fake Provider、outbox、settle、SSRF 和失败补偿测试 |
| T8.4 | 阶段三参考汇聚 | 名称一一对应、缺图/缺音/失效对象和 ready 契约测试 |
| T8.5 | 阶段三连续性 | 自然排序、尾帧到首帧和取消栅栏测试 |
| T9.1 | 阶段三 ORM；阶段四约束 | arrangement/RenderJob FK、row_order 和版本测试 |
| T9.2 | 阶段一/三 API/worker | CRUD/reorder/render/status/cancel/recover、trim 和权限测试 |
| T10.1 | 阶段二存储 | local/S3 put/get/delete/iter/signed_url、逃逸和 fail-fast 测试 |
| T10.2 | 阶段二媒体代理 | 全 key 授权、真实 MIME、清理、流式、Range/416 和大小限制测试 |
| T10.3 | 阶段三封面 | SVG 确定性、自然序首帧和跨集隔离测试 |
| H1 | 阶段四链路 | fake Provider 全链路及积分/审核/delta/参考图/首尾帧断言 |
| H2 | 各阶段横向 | Cookie、乐观锁、终态轮询、错误体、多租户、粘性 Cookie 测试 |

## 9. 多代理 Workflow 与文件所有权

实现阶段使用干净上下文和互斥文件边界。模型强度按风险分配：

- `sol`：积分事务、租户/IDOR、媒体路径/MIME/SSRF、跨模块一致性和最终安全复审。
- `terra`：流水线/Celery、资产与画布、生成/分镜/视频/时间线独立业务实现。
- `luna`：端点/模型/错误体机械清点、专项测试补齐、静态诊断和迁移 diff 复核。

每个代理只拥有任务声明中的文件；共享文件由主会话串行集成：

- `app/workers/celery_app.py`
- `app/models/__init__.py`
- `app/api/router.py`
- `app/core/config.py`
- `backend/pyproject.toml` 与锁文件
- Alembic revision

代理交付必须包含：修改文件、RED/GREEN 命令及结果、未解决风险、未启动或已清理的资源。
主会话不直接信任代理结论，必须检查 diff 并重新运行相应测试。

若指定模型不可用，允许按 `sol -> terra -> luna` 的能力顺序降级或升级，但必须记录实际模型、
失败原因和重试；不得让多个代理无边界地修改同一文件。

## 10. 提交与资源策略

- 每阶段先检查 `git status --short`，只暂存本任务相关文件。
- 使用中文提交信息，按“测试/实现/文档”或可审查的阶段边界提交。
- 不提交用户原有删除、未跟踪规格文件或其他无关变更。
- 不执行 `git push`。
- 测试启动的 PostgreSQL、Redis、容器、worker、dev server 和监听端口必须记录命令、
  端口、PID/会话，并在阶段结束后停止和确认释放。
- 用户原有 `127.0.0.1:18888` 服务不停止、不重启。

## 11. 完成标准

只有同时满足下列条件才可宣称任务完成：

1. TASK 的 37 个二级项、功能矩阵 T0.1-T10.3 和 H1/H2 的每个后端要求均有对应实现与测试证据。
2. 所有可运行的后端测试、Ruff、Mypy、导入、OpenAPI 和 fake 端到端验证通过。
3. 获授权后，初始 Alembic migration 在隔离空库完成 upgrade/downgrade 验证；若未授权，
   必须明确将迁移列为唯一未完成高风险项，不能把总目标标记为完成。
4. 代码 review 未发现行为回归、无关改动、敏感信息、错误处理和覆盖缺口。
5. 本任务启动的所有服务和临时资源已清理，用户既有服务保持不变。
6. 相关改动已用中文提交信息提交，报告提交哈希、当前分支和真实工作区状态，且未推送。
