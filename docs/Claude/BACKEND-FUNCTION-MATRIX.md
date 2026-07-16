# 后端功能核验矩阵

> 核验日期：2026-07-16
> 范围：仅 `backend/`；前端不在本次验收范围。
> 基线：`docs/Claude/TASK.md`、`PLAN.md`、01-09 规格与探索证据。

## 状态定义

- `已实现`：静态实现与可执行证据均支持当前验收项。
- `部分实现`：主路径存在，但至少一个必需边界、错误路径或测试证据缺失。
- `阻断`：存在确定性错误，主流程无法达到验收终态。
- `未实现`：要求的实现或可部署产物不存在。
- `E1/E2` 仅表示原平台证据等级；`D` 表示复刻设计验收，不提升为原平台事实。

## 验证基线

| 验证 | 结果 | 说明 |
|---|---:|---|
| Python 3.12 pytest | 42 passed / 34 failed | 31 个失败为权限 mock 夹具过期，3 个暴露拒绝审计生产耦合 |
| Ruff | 83 项 | 79 项可自动修复；主要为导入顺序/未使用导入 |
| Mypy | 36-37 项 | 含时间线存储 API、`User.id` 等真实运行时缺陷，也含 SQLAlchemy 存根噪声 |
| AST | 112 个 Python 文件通过 | 不等价于运行时通过 |
| 应用导入 / OpenAPI | 成功，53 paths / 87 schemas | 出现 passlib/bcrypt 兼容告警 |
| 专项测试 | T0、T2-T10 均为 0 | 当前仅 `backend/tests/test_auth.py` |
| 数据库迁移 | 0 revision | `backend/alembic/versions/` 不存在 |

## T0-T3 基础与核心流水线

| TASK 项 | 证据 | 状态 | 代码/测试证据 | 剩余缺口 |
|---|---|---|---|---|
| T0.1 项目结构 | D | 部分实现 | `backend/app/` 分层、`pyproject.toml`、`docker-compose.yml`、Alembic 环境均存在 | 无依赖锁；无 Alembic revision；`.env.example` 仅确认存在、未读取 |
| T0.2 配置与依赖注入 | D | 部分实现 | `app/core/config.py:10`、`database.py:1`、`redis.py:1`、`deps.py:1`、`main.py:36` | `main.py` 未装配 CORS；生产模式未拒绝默认 Session secret |
| T0.3 统一约定 | D | 部分实现 | `/v3api/health` 位于 `app/api/router.py:26`；统一异常处理在 `core/errors.py` | 422 额外返回 `errors`，偏离严格 `{detail}`；分页/列表契约缺少全端点测试 |
| T1.1 多租户实体 | D | 已实现 | Organization/Department/User/UserOrganization/ProjectMember 五个模型均存在 | 无迁移与约束集成测试 |
| T1.2 会话鉴权 | E2/D | 部分实现 | `api/v1/auth.py:76`、`:153`、`:161`、`:197`；`core/auth.py:31` | 默认 `dev-only-change-me` 可进入 prod；Cookie 篡改/过期/logout 成功路径无测试 |
| T1.3 双权限轴 | D | 部分实现 | 角色矩阵 `core/permissions.py:73`；团队成员检查 `:138` | active org 未贯穿列表/创建/积分；拒绝审计故障可把 403 变 500；多处 IDOR/取消权限缺口 |
| T2.1 积分实体 | E2/D | 部分实现 | `models/credit.py:11`、`credit_log.py:12`、`credit_reservation.py:19` | 无余额/冻结 CHECK；部分唯一键弱于 PLAN；无迁移 |
| T2.2 五段事务服务 | D | 阻断 | `services/credits.py:97` 起已实现 reserve/settle/release/refund/grant | 锁查询未 `populate_existing`；结算失败仍可让视频/流水线成功；disabled 账户不能 release/refund |
| T2.3 积分 API | E2/D | 部分实现 | `api/v1/credits.py:42`、`:52`、`:79` | 部门向同部门用户下发被拒绝；active org 与管理员边界未闭合；无专项测试 |
| T3.1 剧本/主体/流水线模型 | E2/D | 阻断 | 六类模型均存在 | `Subject.parent_id` 指向 `subjects.id`，但写入 `parent_subjects.id`；无迁移 |
| T3.2 剧本与分集 API | E2/D | 部分实现 | scripts/episodes/breakdown/status/news 空列表端点齐全；乐观锁在 `crud/script.py:195` | breakdown 固定返回空 shots/groups；流水线投递异常无补偿 |
| T3.3 五阶段 worker | D | 阻断 | 五阶段 task 在 `workers/pipeline.py:316` 起注册 | `celery_app.chain/group` 不存在；旧 split 可覆盖新文本；attempt 无锁；结算异常被吞 |
| T3.4 LLM Provider / Prompt | E2/D | 部分实现 | Provider `providers/llm_openai.py:39`；12 段模板 `services/pipeline/enrich.py:35` | LLM 输出无 schema/12 段强校验；enrich 结果未成为生图服务端唯一来源 |

## T4-T7 资产、画布、图片与分镜

| TASK 项 | 证据 | 状态 | 代码/测试证据 | 剩余缺口 |
|---|---|---|---|---|
| T4.1 资产与主体模型 | E2/D | 部分实现 | `models/asset_entry.py` 与 Subject 审核字段存在 | 无迁移；数据库枚举/状态约束弱 |
| T4.2 资产库 API | E2/D | 部分实现 | TASK 所列 10 个端点均存在于 `api/v1/asset_library.py` | ensure 先查后插并发 500；历史 approved 资产可绕过候选队列；无测试 |
| T4.3 审核状态机 | E2/D | 部分实现 | `services/review.py` 实现 approve/delete/impact 主路径 | 首次双审核可互相覆盖；上传候选不切 pending；改名后按名称计算影响会漏报 |
| T5.1 画布模型 | E2/D | 已实现 | Materials/CanvasNode/CanvasEdge 三模型存在 | 无迁移和约束集成测试 |
| T5.2 画布 API | E2/D | 部分实现 | metadata/canvas/delta/image/ensure 五端点齐全 | delta 版本可省略且仅秒级；并发丢更新；孤儿边可持久化 |
| T5.3 节点 data / prompt | E2 | 部分实现 | `schemas/materials.py:26` 有 prompt helper | data 仍是任意 dict，helper 未接入写入链路，缺完整字段 round-trip 测试 |
| T6.1 GenerationJob | E2 | 已实现 | `models/generation_job.py:16` 字段与状态存在 | 无迁移；终态 CAS/lease 不完整 |
| T6.2 生成任务 API | E2/D | 部分实现 | POST/get/list 三端点存在 | 无 Idempotency-Key 和取消 API；重复请求会重复冻结/调用 AI |
| T6.3 图片 Provider/worker | E2/D | 阻断 | Google/OpenAI 路由与 worker 实现存在 | `workers/celery_app.py:15` 未 include generation；worker 无原子领取；少图仍按请求数结算 |
| T6.4 资产联动 | E2/D | 部分实现 | 成功写未审核 AssetEntry，视频仅聚合 approved image_entry | resolved prompt 可由客户端伪造，批量路径使用裸 description；无专项测试 |
| T7.1 分镜模型 | E2/D | 部分实现 | Scene/ShotGroup/Shot/StoryboardJob 存在 | StoryboardJob 未绑定输入版本；无迁移 |
| T7.2 分镜 API | E1/E2/D | 部分实现 | generate/status/hierarchy/workbench/cover 均存在 | 并发先查后写可创建多个任务；相关 check-materials 存在 IDOR |
| T7.3 LLM-5 分镜 | E2/D | 部分实现 | prompt 分时段、角色、叙事目的和 9 条约束已编码 | storyboard task 未 include；旧任务可覆盖新分镜；description/schema 未验证 |
| T7.4 materials/audio 汇聚 | E2 | 部分实现 | `services/storyboard.py:479` 解析分类 | 无账户时仍可免费调用 LLM；音频审核状态与图片状态错误耦合 |

## T8-T10 视频、时间线与媒体

| TASK 项 | 证据 | 状态 | 代码/测试证据 | 剩余缺口 |
|---|---|---|---|---|
| T8.1 视频模型 | E2/D | 部分实现 | VideoGenerationJob/VideoProviderConfig 存在 | 幂等键仅在 JSON，无唯一约束；active provider 无单活约束；无迁移 |
| T8.2 视频 API | E2/D | 阻断 | create/list/get/provider/check/batch 代码存在 | `/{job_id}` 遮蔽 active-provider/check-materials；check-materials 未绑定 shot group 归属 |
| T8.3 视频 Provider/worker | D | 部分实现 | jianyu/modelsai 路由 Seedance；worker 进度路径存在 | settle 异常仍 succeeded；broker 失败永久 starting/frozen；Provider URL 下载存在条件型 SSRF |
| T8.4 多参考图 | E2/D | 部分实现 | approved 图片按 names 顺序汇聚 | 不存在镜头组返回 ready=true；音频错误过滤；存储对象缺失会静默跳过 |
| T8.5 首尾帧 | E2/D | 部分实现 | reference_frames 与尾帧写回存在 | `G10/G2` 字符串排序错误；取消后 worker 仍可写尾帧/返回成功 |
| T9.1 时间线模型 | E2/D | 部分实现 | VideoTimelineArrangement 字段存在 | row_order 无唯一；关联约束不足；无持久化 RenderJob；无迁移 |
| T9.2 时间线 API/渲染 | E2/D | 阻断 | list/create/patch/delete/reorder/render/status/cancel 均注册 | worker 调不存在的 storage API；使用 `User.id`；trim 参数错误；Viewer 可取消；状态仅 Redis |
| T10.1 StorageProvider | D | 阻断 | Local/S3 的 put/get/delete/iter 实现存在 | 无 signed_url；local `startswith` 路径边界可逃逸；S3 缺依赖时静默回退 local |
| T10.2 媒体代理 | E2/D | 阻断 | GET/POST 路由、Range 与流式返回存在 | 伪图片扩展名绕过 MIME；视频/帧 key 无法授权；错误后对象残留；非法 Range 返回完整 200 |
| T10.3 封面 | E2 | 部分实现 | 确定性 SVG 已实现；shot cover 端点存在 | group_no 字符串排序与跨集回退可能不是真正首镜头首帧 |

## 横向验收

| 验收项 | 状态 | 证据/原因 |
|---|---|---|
| H1 全链路 | 阻断 | T3 DAG、T6/T7 task 注册、T8 静态路由、T9 storage API、T10 视频 key 任一项均可阻断 |
| H1 积分一致性 | 阻断 | identity map 旧值与 settle 异常吞并均可破坏账实一致 |
| H1 资产审核 | 部分实现 | 正常主路径存在，首次并发和上传状态同步未通过 |
| H1 画布 delta | 部分实现 | 单请求可写，版本与并发完整性未通过 |
| H1 多参考图/首尾帧 | 部分实现 | 主路径存在，排序、音频、失效对象和取消竞争未通过 |
| H2 Cookie/401 | 部分实现 | 基本路径通过，生产 secret 与完整 Cookie 测试缺失 |
| H2 乐观锁 | 部分实现 | Episode 原子更新存在，异步旧任务保护未闭合 |
| H2 状态轮询 | 部分实现 | 字段/端点存在，多类任务无法可靠到终态 |
| H2 错误体 | 部分实现 | 401 基本符合，422 严格形状不符，审计异常可改写 403 |
| H2 多租户 | 阻断 | active org 未贯穿团队列表、创建与积分；存在跨资源 IDOR |

## 修复门禁

- 生产代码尚未修改；需用户批准分阶段修复设计后按 TDD 开始。
- 初始 Alembic migration 需单独明确授权；未授权前不生成或执行。
- 不调用真实收费 AI，不读取 `.env*`/密钥，不验证或修改前端。
