# 后端功能缺口报告 (BACKEND-GAPS)

> [!IMPORTANT]
> 本文是修复前的历史核验快照。其后提交 `ca4947b` 已实现并验证完整后端流水线，
> `a59f6ce` 又统一了多模态 Provider。基于当前 `main` 的逐项复核与剩余修复记录见
> [BACKEND-GAPS-WORKFLOW-2026-07-17.md](./BACKEND-GAPS-WORKFLOW-2026-07-17.md)，
> 请勿再将本文的“未实现/阻断”状态直接视为当前代码状态。

> 核验日期：2026-07-17
> 范围：仅 `backend/`；前端不在本次核验范围（由其他同事负责）。
> 代码基线：`main` 分支当前代码。
> 基线文档：`TASK.md`、`PLAN.md`、`01~09` 规格文档。
> 核验方式：11 个干净上下文子代理并行，各自独立读取规格 + 当前代码，逐条对比 TASK 验收标准，给出 `file:line` 证据。
> 对照结论：与 `BACKEND-FUNCTION-MATRIX.md`（2026-07-16）一致，生产代码尚未按修复计划修改，最近提交均为修复计划/设计文档。

---

## 0. 状态定义

- `已实现`：静态实现与可执行证据均支持当前验收项。
- `部分实现`：主路径存在，但至少一个必需边界、错误路径或测试证据缺失。
- `阻断`：存在确定性错误，主流程无法达到验收终态。
- `未实现`：要求的实现或可部署产物不存在。
- 严重度：`P0` 阻断主流程/数据不一致/安全漏洞；`P1` 边界/错误路径/并发缺陷；`P2` 质量/测试缺失/契约偏离。

---

## 1. 总览

| 模块 | 状态 | 检查项 | P0 | P1 | P2 | 未实现项 | 专项测试 |
|---|---|---:|---:|---:|---:|---:|:---:|
| T0 基础设施 | 部分实现 | 17 | 1 | 0 | 1 | 0 | 有 |
| T1 鉴权多租户 | 部分实现 | 11 | 1 | 0 | 3 | 0 | 有 |
| T2 积分账户 | 部分实现 | 16 | 1 | 2 | 1 | 0 | 有 |
| T3 剧本/流水线 | 部分实现 | 25 | 1 | 1 | 3 | 1 | 有 |
| T4 资产库 | 部分实现 | 19 | 1 | 2 | 2 | 0 | 有 |
| T5 画布 | 部分实现 | 9 | 1 | 1 | 2 | 0 | 有 |
| **T6 生成任务** | **阻断** | 18 | **2** | 4 | 2 | **6** | **无** |
| T7 分镜 | 部分实现 | 16 | 1 | 6 | 2 | 2 | 无 |
| T8 视频生成 | 部分实现 | 22 | 0 | 8 | 4 | 5 | 无 |
| T9 时间线 | 部分实现 | 12 | **3** | 3 | 2 | 2 | 无 |
| T10 媒体代理 | 部分实现 | 13 | 1 | 4 | 3 | 2 | 有 |

**11 个模块无一达到"已实现"**：10 个"部分实现"，1 个"阻断"（T6）。根因集中在几类共性问题。

测试覆盖：T0–T5、T10 有专项测试；**T6/T7/T8/T9 四个模块无专项测试**（仅靠集成测试间接覆盖），与这几个模块问题最严重相吻合。

---

## 2. 阻断性问题（P0，必须先修）

### P0-1 Alembic 无任何迁移 revision（头号共性，T0/T1/T2/T3/T4/T6/T9 标注，T5/T7/T8/T10 同样受影响）

- 要求：TASK T0.1 `backend/alembic/` 数据库迁移；PLAN §10 启动步骤 `alembic upgrade head` 必须能建表；核验红线要求 `alembic/versions/` 至少一个 revision。
- 证据：`backend/alembic/versions/` 为空目录，`find alembic/versions -type f` 返回 0 文件；`alembic/env.py:12-13` 已正确导入 `app.models` 的 `Base` 元数据但无 revision 可执行；`app/main.py:59-72` lifespan 仅 `yield + close_redis`，无 `Base.metadata.create_all` 兜底；全局 grep `create_all|metadata.create` 无命中。
- 影响：`alembic upgrade head` 是 no-op，干净数据库无任何表，所有 API 运行时 `relation does not exist` 报 500。当前测试仅靠 `Base.metadata.create_all(engine)` 临时建表跑通，**不等于生产可用**。阻断 T1 及之后所有依赖数据库的验收。
- 备注：上一轮已标注"创建覆盖全部 ORM 的初始迁移属数据库迁移操作，需用户单独授权"，未授权前不应创建或执行。

### P0-2 Celery worker 未注册（T6/T7 共性）

- 要求：TASK T6.3 `workers/generation.py` Celery worker；TASK T7.3 storyboard worker 应可被自动加载。
- 证据：`backend/app/workers/celery_app.py:15-19` include 列表仅 `pipeline / timeline_render / video_generation`，**缺 `app.workers.generation` 和 `app.workers.storyboard`**。
- 影响：生图任务、分镜任务投递到 broker 后无 worker 消费，永久滞留 `starting`，积分永久冻结，主流程阻断。

### P0-3 T5 画布增量保存不提交事务（数据丢失）

- 要求：TASK T5.2 `POST /materials/<mid>/canvas/delta` 增量保存；验收：canvas/delta 移动后刷新位置一致。
- 证据：`backend/app/api/v1/materials.py:102` `post_canvas_delta` 无 `await db.commit()`；`backend/app/crud/materials.py:101` 使用 `db.begin_nested()` 保存点；`backend/app/core/database.py:31-34` `get_db` finally 仅 `close` 不 `commit`。
- 影响：客户端发 delta，服务端返回最新画布状态看似成功，但下次 `GET /canvas` 节点/边/位置全未持久化，version 仍为旧值。测试因在 `test_canvas.py` 内手动 `raw.commit()` 而未覆盖此路由层缺口。

### P0-4 T9 三处阻断，时间线合成完全跑不通

- (a) `user.id` 类型错误：`backend/app/services/timeline_render.py:176` `"owner_user_id": user.id` 与 `:283` `owner_user_id != user.id`，但 `backend/app/models/user.py:15` `user_id` 为 PK，未继承 `UUIDPKMixin`，无 `id` 属性；对照 `auth.py:32`、`permissions.py:175` 均用 `user.user_id`。
  - 影响：`POST /render` 触发 `render()` 立即 `AttributeError` → 500；`GET /render/{job_id}` 与 `POST /cancel` 同样在 `_verify_render_access` 处 500。
- (b) worker 调用不存在的 storage API：`backend/app/workers/timeline_render.py:86,108,182` `from app.providers.storage import get_storage_provider`，但 `providers/storage.py:103` 实际是 `def get_storage`；worker 调 `provider.get_bytes/put_bytes/resolve_local_path`，而协议 `storage.py:35,39` 仅定义 `async get/put`，**这些方法不存在**。
  - 影响：`_resolve_local_path` 捕获 ImportError 返回 None；`_download_segment` 捕获 ImportError 后 `raise RuntimeError` → 外层 except 写 `status=failed`，合成任务每次失败，永远产不出最终视频，端到端流水线第 10 步断裂。
- (c) Alembic 无迁移：`video_timeline_arrangements` 表无法建表，所有时间线端点运行时 500。

### P0-5 T10 视频/抽帧 storage_key 无法鉴权访问

- 要求：TASK T10.2 前端不直连底层存储，全经代理；T10.3 视频项目封面首个镜头首帧；视频/抽帧 key 可被授权访问。
- 证据：`backend/app/providers/storage.py:98` `parse_mid_from_key` 固定取第二段为 mid；而 worker 生成的 key 形如 `/generate_images/video/frames/<sgid>_last.png`（`workers/video_generation.py:383`）和 `generate_images/video/<ident>.mp4`（`providers/video_gen_volcengine.py:421`）→ 被解析为 `mid='video'` → `api/v1/media.py:245-250` 查 `Materials('video')` 得 None → `NotFoundError`。
- 影响：前端经代理访问视频项目封面、链式首帧、生成视频文件均得 404；视频产物无法被前端获取，视频预览/封面功能阻断。

### P0-6 T7 无积分账户可免费调用 LLM（成本/安全漏洞）

- 要求：TASK T7.2 `POST /generate-storyboard` 触发 LLM 分镜生成；验收要点：无积分账户不应免费调用 LLM。
- 证据：`backend/app/workers/storyboard.py:104-132` `if account:` 才 reserve，否则跳过直接调 LLM；`backend/app/services/storyboard.py:416` 无条件 `_call_llm`；`backend/app/api/v1/shots.py:114-118` 仅校验 `Action.RUN_AI` 权限，无积分账户预检。
- 影响：任何有 `RUN_AI` 权限的用户（含无积分账户者）可无限次免费触发 LLM 分镜生成，直接产生 AI 调用费用且无积分扣减。

---

## 3. 完全未实现的功能点

| 模块 | 未实现项 |
|---|---|
| T3 | Alembic 建表迁移（lifespan 无 `create_all` 兜底，生产环境 T3 全部表无法创建） |
| T6 | Celery worker 调度（`app.workers.generation` 未加入 `celery_app.include`，`generate_images_task` 运行时不被消费） |
| T6 | Alembic 迁移（`alembic/versions/` 为空，`generation_jobs` 表无迁移 revision） |
| T6 | `POST /generation-jobs` 的 `Idempotency-Key` 幂等保护 |
| T6 | worker 原子领取（`FOR UPDATE`/条件 `UPDATE`）防重投递 |
| T6 | 少图时按实际生成张数结算（当前按请求 count 计费） |
| T6 | `resolved_prompts.final` 服务端 enrich 强制（当前接受客户端直传） |
| T7 | `StoryboardJob` 绑定输入 `episode_version`（模型无该字段，端点接收的 episode_version 仅做乐观锁比对未落库） |
| T7 | storyboard worker 注册到 `celery_app.include`（include 缺 `app.workers.storyboard`，Celery worker 无法自动加载分镜 task） |
| T8 | 幂等键 DB 唯一约束（当前仅 JSONB 存储，无独立列无 unique） |
| T8 | settle 异常时阻断 succeeded 标记（当前 settle 失败被吞仍标成功） |
| T8 | `check-materials` 镜头组不存在时返回 `ready=false`（当前返回 true） |
| T8 | `G10/G2` 按数字排序（当前 group_no 字符串排序） |
| T8 | Provider 下载 URL 的 SSRF 防护（scheme/host 白名单） |
| T9 | 渲染 worker 对接的 storage API（`get_storage_provider/get_bytes/put_bytes/resolve_local_path` 均不存在），合成主任务功能性失败 |
| T9 | Alembic 无任何 revision，`video_timeline_arrangements` 表无迁移 |
| T10 | `StorageProvider.signed_url` 方法（协议与两个实现均无此方法） |
| T10 | 视频/抽帧 key（`generate_images/video/...`）经鉴权代理的可访问路径（当前 `parse_mid_from_key` 解析为 `mid='video'` 必 404） |

---

## 4. 关键 P1（边界/并发/契约缺陷，择要）

### T2 积分账户体系
- `video_generation` worker 吞掉 settle 异常后仍将 job 标记 succeeded。
- `video_generation` worker 失败路径吞掉 release 异常，可能造成孤立冻结。

### T3 剧本模块与 AI 流水线
- `Subject.parent_id` 外键指向 `parent_subjects.id`，与 04 文档父子层次契约不符。

### T4 资产库模块
- `remove_candidate_with_recalc` 仅在 pending 时重算，未覆盖声音候选删除与 approved→not_designed 回退。
- `get_replace_impact` 仅检查 image_entry 替换，未覆盖声音替换影响与无 image_entry 时的场景。

### T5 画布模块
- Alembic versions 目录为空，无任何迁移 revision。

### T6 生成任务模块
- `POST /generation-jobs` 无 Idempotency-Key 幂等保护，重复请求重复扣费。
- `resolved_prompts.final` 接受客户端直传，未走服务端 enrich 校验。
- worker 缺原子领取（无 `SELECT FOR UPDATE`/条件 `UPDATE`），重投递会重复处理。
- settle 按请求 count 计费而非实际生成张数，少图时超额扣费。

### T7 分镜模块
- storyboard worker 未注册到 `celery_app.include` 列表。
- 分镜任务并发去重存在竞态（无行级锁/无唯一约束）。
- `check-materials` 端点存在 IDOR（`shot_group_id` 未校验归属）。
- `StoryboardJob` 未绑定触发时的输入 episode_version。
- LLM 输出未做 schema 校验，字段缺失将致运行时异常。
- `alembic/versions/` 目录为空，无任何迁移 revision。

### T8 视频生成模块
- 动态 `{job_id}` 路由遮蔽 `check-materials` / `active-provider` 两个静态 GET。
- 幂等键仅存于 JSONB，无 DB 唯一约束，并发可重复创建 + 重复冻结。
- settle 异常被吞后仍标记 succeeded。
- 取消任务后 worker 仍写尾帧并返回 succeeded。
- `check-materials` 未校验 shot_group 归属剧本，存在 IDOR。
- 镜头组不存在时 `check-materials` 返回 `ready=true`。
- `group_no` 字符串排序导致 `G10<G2`，首尾帧链式衔接错乱。
- Alembic versions 目录为空，无任何迁移 revision。

### T9 时间线编排模块
- `cancel` 端点仅做 READ 级鉴权，Viewer 可取消他人 render 任务。
- RenderJob 仅存 Redis 且 24h TTL，无持久化 ORM 表。
- `row_order` 无 DB 级唯一约束且 `add_arrangement` 无行级锁，并发可重号。

### T10 媒体代理模块
- 非法 Range 请求回退为完整 200 响应。
- S3 缺 boto3 依赖时静默回退 local。
- `LocalStorageProvider` 路径边界 `startswith` 检查可被同前缀目录逃逸。
- `get_cover` 跨集回退未在当前集内继续查找后续 group。

---

## 5. 各模块一句话总结

- **T0 基础设施**：骨架基本完整（分层目录、config/database/redis/deps/ids/errors、main.py 工厂、`/v3api` 前缀、CORS、统一错误体、health 端点、401 未登录、UUID v4、UTC 时间戳、列表/分页约定、生产 secret 拒绝、docker-compose、pyproject、.env.example、tests 均已实现）；唯一阻断是 `alembic/versions/` 无任何 migration revision。
- **T1 鉴权多租户**：模型、路由、会话、权限矩阵与资源级 IDOR 防护逻辑均已实现且有专项测试；唯一阻断是 `alembic/versions/` 无任何迁移 revision，五张鉴权表无法建表。
- **T2 积分账户**：三段式服务（reserve/settle/release/refund/grant）的 ORM 模型、状态机、行级锁 `FOR UPDATE` + `populate_existing`、幂等键、单事务集成均完整且测试覆盖充分；唯一阻断是 Alembic versions 目录为空。
- **T3 剧本/流水线**：六模型字段、10 个 API 端点、Celery DAG 编排（chain+chord+group）、五阶段 LLM 服务、`input_version` 绑定、attempt 重试与行锁结算均严谨实现，prompt 严格遵循 06 文档第 12 节；阻断/偏离为 Alembic 无迁移 + `Subject.parent_id` 外键指向错误。
- **T4 资产库**：实体模型、全部 API 端点、审核状态机、并发 `FOR UPDATE` 锁与幂等画布项目均已实现并有专项测试；阻断为 Alembic versions 目录为空，CHECK/唯一约束/索引不会在真实 DB 落地；superseded 终态回退与声音候选删除的重算路径语义不完整。
- **T5 画布**：三模型字段齐全、4 个 API 端点全部注册、节点 data 字段与 prompt 自动构造模板完整对齐 06 文档；阻断为 `POST /canvas/delta` 缺事务提交，增量保存写入不会持久化。
- **T6 生成任务**：模型/路由/Provider/worker 主体代码齐全且逻辑较完整（含 settle/release 补偿、候选入库状态机、参考图过滤），但两个 P0 阻断（Celery include 未注册 generation worker、Alembic 无迁移），叠加 Idempotency-Key 缺失、`resolved_prompts.final` 可客户端伪造、无原子领取、少图仍按请求数结算等并发与契约缺陷，且无专项测试。
- **T7 分镜**：模型字段、API 端点、LLM 分镜服务、materials/audio_elements 汇聚、三层层级查询均已实现并注册路由，但多项运行时缺陷：无积分账户可免费触发 LLM、Celery include 缺失 storyboard worker、并发去重竞态、StoryboardJob 未绑定输入版本、check-materials IDOR、alembic 无迁移、无专项测试。
- **T8 视频生成**：实体模型、Provider 抽象、Worker 编排、参考图汇聚主链路代码齐备，但存在路由遮蔽、幂等键无 DB 唯一约束、settle 异常仍标成功、首尾帧 group_no 字符串排序错乱、check-materials IDOR 与 ready 误报、无 Alembic 迁移、无专项测试等多处阻断与并发缺陷。
- **T9 时间线**：路由、模型、CRUD、worker 骨架齐全且端点已注册，但三处运行时阻断：render 服务用 `user.id`（User 主键是 `user_id`）、worker 调用不存在的 storage 工厂与方法、Alembic 无任何迁移 revision。
- **T10 媒体代理**：媒体代理路由、SVG 封面、上传 MIME 探测主体已实现并挂载，但视频/抽帧 storage_key（`generate_images/video/...`）无法通过 `get_media` 鉴权（被 `parse_mid_from_key` 当作 `mid='video'` 必 404），且非法 Range 回退完整 200、S3 缺依赖静默回退 local、local 路径 `startswith` 边界可逃逸、协议缺 `signed_url` 方法。

---

## 6. 横向验收结论

| 验收项 | 状态 | 证据/原因 |
|---|---|---|
| H1 全链路 | 阻断 | Alembic 缺迁移 + T6/T7 worker 未注册 + T9 storage API 不匹配 + T10 视频 key 不可达，任一项都使 1 剧本全流程跑不通 |
| H1 积分一致性 | 阻断 | settle/release 异常被吞 + identity map 旧值，可破坏账实一致 |
| H1 资产审核 | 部分实现 | 正常主路径存在，首次并发和上传状态同步未通过 |
| H1 画布 delta | 阻断 | 路由层缺 `db.commit()`，写入不持久化 |
| H1 多参考图/首尾帧 | 部分实现 | 主路径存在，排序、音频、失效对象和取消竞争未通过 |
| H2 Cookie/401 | 部分实现 | 基本路径通过，生产 secret 与完整 Cookie 测试缺失 |
| H2 乐观锁 | 部分实现 | Episode 原子更新存在，异步旧任务保护未闭合 |
| H2 状态轮询 | 阻断 | T6/T7 worker 未注册、T9 render 500，多类任务无法可靠到终态 |
| H2 错误体 | 部分实现 | 401 基本符合，422 严格形状不符，审计异常可改写 403 |
| H2 多租户 | 部分实现 | active org 未贯穿团队列表、创建与积分；存在跨资源 IDOR（T7/T8 check-materials） |

---

## 7. 建议修复顺序

1. **先授权并生成 Alembic 初始迁移** —— 一条修复解除 T0/T1/T2/T3/T4/T6/T9 等 7 个模块的部署阻断（最大杠杆）。
2. **补 `celery_app.include`**（`generation`、`storyboard`）—— 解除 T6/T7 任务不消费。
3. **T9 三连修**：`user.id` → `user.user_id`；worker 对接真实 storage 协议（补 `get_bytes/put_bytes` 或改用现有 `get/put`）；补迁移。
4. **T5**：`post_canvas_delta` 补 `await db.commit()`。
5. **T10**：`parse_mid_from_key` 支持 `video` 段；补 `signed_url`。
6. **T7**：分镜前积分预检 + worker 注册 + LLM 输出 schema 校验。
7. 再按 P1 逐模块补并发锁、幂等唯一约束，IDOR 归属校验，状态机回退。

---

## 8. 红线（核验过程遵守）

- 不读取 `.env*` 或任何密钥文件；不调用真实收费 AI；不修改任何代码（只读核验）。
- 不启动服务/容器/端口；不创建或执行 Alembic 迁移。
- 所有结论均有 `file:line` 代码证据或明确标注"不存在"。

> 本报告由 11 个独立干净上下文子代理（Workflow `wf_b74cf33a-066`）并行核验汇总，原始结构化结果见 workflow journal。
