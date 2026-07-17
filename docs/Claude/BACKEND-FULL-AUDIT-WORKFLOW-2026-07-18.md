# 后端全功能审计与实现工作流（2026-07-18）

## 1. 目标与范围

本任务只实现和验证后端，不创建或修改前端代码。需求基线为 `docs/` 下全部文档、探索记录与截图；实现基线为当前 `main` 分支 `HEAD` 加任务开始前已存在的 5 个未提交 Provider 相关文件。

验收目标：

1. 对 T0–T10 的后端功能逐项确认实现状态和新鲜验证证据。
2. 补齐产品规格中存在、但原后端 `TASK.md` 未列出的资源生命周期接口。
3. 使用真实 `.env` 配置对文本、图片和视频 Provider 做最小可计费联调，不回显密钥。
4. 运行与变更范围匹配的测试、静态检查、迁移和可执行的基础设施流程。
5. 仅提交本任务相关文件，使用中文提交信息，不推送远程。

## 2. 证据口径

- `E1/E2`：原平台可见或采集到的兼容行为。
- `D`：`08`、`TASK.md`、修复 Spec 中定义的复刻设计与安全验收。
- `U/推断`：文档没有真实写请求证据，但产品 UI 明确需要的后端能力；实现时使用当前 REST、权限、审计和事务约定。
- 文档明确排除的新闻播报、实时协同、移动端、自助注册、TTS、字幕、特效/转场和多语言不纳入本期实现。

## 3. 最终实现矩阵

| 模块 | 文档功能 | 最终结论 | 新鲜证据 |
|---|---|---|---|
| T0 基础设施 | FastAPI 分层、配置、数据库、Redis、错误、健康检查、迁移、锁文件 | 已实现并真实验收 | PostgreSQL 迁移往返、Redis/Celery 投递、锁文件检查通过 |
| T1 鉴权/多租户 | 登录、登出、me、switch-org、Cookie、个人/团队权限、IDOR 防护 | 已实现；补齐团队成员 CRUD | 权限、跨项目 403、最后管理者保护和审计测试通过 |
| T2 积分 | account/log/grant、reserve/settle/release/refund、幂等、行锁 | 已实现 | V5 同一实体链路覆盖 reserve/settle/release，最终 frozen=0 |
| T3 剧本流水线 | 生命周期、分集、乐观锁、五阶段流水线、手动重提取/失败重试 | 已实现 | B1/B2/B4、活动任务删除保护和真实 extract 契约通过 |
| T4 资产库 | 主体 CRUD、人工锁、差异建议、父主体、候选、审核、替换影响 | 已实现 | B3/B6 及候选审核集成链路通过 |
| T5 画布 | materials、canvas、delta、节点 data、图片元数据 | 已实现 | V5 验证可信画布字段进入下游 |
| T6 生图 | 单个/批量、幂等、取消、Provider、实际张数结算、候选联动 | 已实现并真实外呼 | Qwen Image 2.0 生成、下载、存储、清理成功 |
| T7 分镜 | 生成/状态/编辑/取消/重试、工作台、封面、积分与版本栅栏 | 已实现 | V5 fake Provider 全链路及真实文本 LLM 契约通过 |
| T8 视频 | 单个/批量、素材检查、Provider、取消、多参考图、首尾帧 | 已实现并真实外呼 | Seedance 2.0 提交、轮询、下载、存储成功 |
| T9 时间线 | CRUD、重排、render/status/cancel/retry、持久化、trim | 已实现 | V5 最终 `TimelineRenderJob=succeeded` |
| T10 媒体 | 本地/S3、上传探测、鉴权代理、Range、signed URL、封面 | 已实现 | 存储、安全测试及真实媒体存取清理通过 |

最终质量基线：`702 passed, 2 skipped in 8.09s`；2 个跳过项是仅在配置真实 PostgreSQL URL 时运行的迁移测试，并已单独在 PostgreSQL 16 上以 `20 passed` 验证。Ruff、Mypy（135 个源文件）、compileall、`uv lock --check` 通过；Alembic 单 head `f6c9a31e8b42`，`base → head`、`head → d4 → head` 与 `alembic check` 均通过。

## 4. 已发现并实现的全部后端缺口

### B1. 剧本完整生命周期

- `PATCH /v3api/scripts/{script_id}`：重命名剧本；沿用 `Action.EDIT`、标题长度校验、审计与统一错误体。
- `POST /v3api/scripts/{script_id}/copy`：复制剧本及分集文本，生成新的个人/团队剧本；不复制生成任务、积分预约、媒体产物和审核正式资产，避免跨租户/跨所有者泄漏。
- `DELETE /v3api/scripts/{script_id}`：删除剧本；仅 Owner/ProjectManager，可检测依赖并要求显式确认，保证事务一致和审计可追踪。
- 删除在父资源行锁内二次检查活动任务；关联媒体先阻塞协调进行中的 promote，再取消未执行 promote，并对 source/final/业务存储键幂等创建物理删除任务，避免并发晋升后媒体复活。

来源：`02` 剧本卡片更多菜单、`09` SCR-L06/SCR-L07，以及 PRD 的剧本管理范围。

### B2. 分集完整生命周期

- `PUT /v3api/scripts/{script_id}/episodes/{episode_id}` 增加 `episode_no` 可选字段，继续使用 `episode_version/client_save_seq` 乐观锁。
- `DELETE /v3api/scripts/{script_id}/episodes/{episode_id}`：删除分集；返回主体/镜头/资产/视频依赖摘要，默认有依赖时拒绝，显式确认后按数据库关系安全处理。
- 集号必须为正整数且同剧本唯一；冲突返回 409。
- 剧本与分集统一按父行到子行的顺序加锁，防止依赖检查、任务创建和删除之间出现 TOCTOU；真实 PostgreSQL 双事务测试确认并发子记录插入会阻塞并由外键拒绝。

来源：`01/02/05` 的新增、双击改集号、删除分集，以及 `08` AC-SE06/AC-SE07。

### B3. 手动新增主体

- `POST /v3api/scripts/{script_id}/asset-library/subjects`：新增角色、场景、道具或声音主体。
- 校验类型、名称、描述、所属集号；场景兼容 `scene/background` 映射；初始审核状态为 `not_designed`。
- 使用资源级权限、唯一性/冲突语义和审计；新增后能被 breakdown、资产库、画布 ensure 和后续生成链路读取。

来源：`02` 资产库“添加”、`09` SUB-03，以及截图中的主体“新增”。

### B4. 流水线手动重新提取与失败重试

- 新增 `POST /v3api/scripts/{script_id}/episodes/{episode_id}/pipeline-runs`，支持 `manual_reextract` 与 `retry_failed`。
- 强制 `Idempotency-Key`、`Action.RUN_AI`、输入版本栅栏、活动 run 409；失败重试仅接受最新且 `retryable=true` 的失败。
- 新建不可变 run/attempt、积分预约和 outbox，不改写旧失败记录；幂等重放不重复审计。

来源：`06` 手动重新提取、`08` 失败重试状态机、`09` SUB-09。

### B5. 团队项目成员管理

- 新增成员列表、添加、角色修改、移除四个接口，角色为 `owner/manager/editor/reviewer/viewer`。
- 仅同组织有效用户可加入，写操作使用剧本行锁；跨项目成员 ID 返回 403。
- 仅 `MANAGE_MEMBERS` 可操作，禁止移除或降级最后一名 owner/manager；API Schema 与数据库 CHECK 双重约束角色。
- 团队项目所有者强绑定 `Script.owner_user_id`；存量迁移补齐缺失 owner 成员、纠正错误角色、降级虚假 owner，并在 owner 用户不存在时 fail closed。

来源：`08` ProjectManager 权限与 `PRD` 多人分工设计；该项为 D 级前向设计，无原平台写请求证据。

### B6. 人工主体字段锁与差异建议

- 人工创建默认锁定名称/描述，人工编辑字段自动锁定并支持 `expected_subject_version`。
- 重提取/merge 不覆盖锁定字段；产生可查询、接受或忽略的 `SubjectChangeSuggestion`。
- 接受建议使用行锁、主体版本栅栏、同名检查和审计；歧义匹配时保留人工主体。
- 使用 `pipeline_name` 作为稳定流水线身份，展示名称可人工修改而不破坏后续 merge；建议按主体版本演进并淘汰旧 pending 项。
- 所有主体身份写入统一使用 `Script FOR UPDATE → Subject FOR UPDATE` 锁序；PostgreSQL 方言 SQL 与双会话事务锁语义测试确认并发改为同名时严格一个成功、一个 409。

来源：`08` AC-SU05。

## 5. 真实验证结果

### V1. 文本 LLM：通过

- 使用 `bailian / deepseek-v4-flash / https://dashscope.aliyuncs.com/compatible-mode/v1`。
- 先验证最小 JSON，再使用项目 extract 原始 Prompt 与 `ExtractOutput` Schema 真实调用。
- 返回 `pipeline.v1`、5 个主体，覆盖 character/scene/prop/sound，所有 `episode_nos=[1]`。

### V2. 图片生成：通过并修复真实缺陷

- 发现 `qwen-image-2.0` 被错误发送到旧异步 text2image 接口；将 `.env` Base URL 修正为地域根地址，并实现同步多模态接口分流。
- 旧 `wan2.1-t2i-*` 等模型仍保留异步任务接口。
- 两次真实生成均成功，PNG 分别约 1.14 MB；第二次完成对象存储写入、读大小和删除，清理后不存在。

### V3. 视频生成：通过

- 火山 Provider 已从旧云账号 AK/SK 改为 `VIDEO_GEN_API_KEY/VOLCENGINE_API_KEY` + Ark Base URL 的 Bearer Token。
- `doubao-seedance-2-0-260128` 真实 4 秒任务完成提交、轮询、下载和本地存储，MP4 为 419,017 bytes。
- 任务专用对象 `generate_images/video/provider-smoke-20260718.mp4` 已删除并确认不存在。

### V4. 基础设施：通过

- PostgreSQL 16 真实执行最终迁移链 `base → head → alembic check → d4 → head`，最终 head 为 `f6c9a31e8b42`；owner 存量三态修复、缺失用户回滚和重复升级幂等测试通过。
- Redis 7 + Celery 5.6 Worker 注册全部任务并真实消费 `outbox.dispatch`，结果 `SUCCESS`。
- 使用过的 55432/55433/55435/55436/55437/56379、容器与 Worker 会话均已停止并确认释放。

### V5. 单条完整后端链路：通过

- 新增 `tests/test_full_backend_flow.py`，同一 script/episode 贯通保存与旧版本拒绝、五阶段 fake LLM、主体候选审核、画布、分镜、视频、时间线与 render。
- Pipeline/Video/Timeline 三类 outbox 均真实经过 `dispatch_outbox_batch` 的 claim→published，重放不重复发布；之后执行确定性 worker/服务。
- 最终断言 `TimelineRenderJob=succeeded`、`input_version=2`、最终 storage_key 存在、reserve/settle/release 均出现且 frozen=0。

### V6. 后端性能基线：通过

数据集：321 剧本、240 分集、1000 主体、2000 资产、600 视频任务/时间线片段；HTTPX ASGITransport 真实经过 FastAPI、会话鉴权、SQLAlchemy 与 PostgreSQL，单客户端串行、热缓存。

| 接口 | 样本 | P50 | P95 | P99 | 失败率 |
|---|---:|---:|---:|---:|---:|
| 剧本分页 | 120 | 99.140 ms | 120.566 ms | 127.993 ms | 0% |
| 分集列表 | 120 | 89.834 ms | 116.441 ms | 121.058 ms | 0% |
| 主体列表 | 120 | 116.761 ms | 137.192 ms | 141.496 ms | 0% |
| 资产条目 | 120 | 98.484 ms | 117.899 ms | 128.133 ms | 0% |
| 时间线列表 | 120 | 107.897 ms | 122.042 ms | 129.148 ms | 0% |
| 流水线状态 | 120 | 369.271 ms | 417.806 ms | 439.661 ms | 0% |
| 异步任务创建（仅事务/outbox） | 100 | 40.720 ms | 71.736 ms | 86.220 ms | 0% |

流水线状态接口是明显离群项；当前没有后端 SLO，因此只记录基线，不虚构“达标”。后台 AI/Worker 执行未计入任务创建延迟。

## 6. 子代理分工

1. `script_episode_lifecycle`：B1/B2、B6、活动任务删除保护及最终独立 Review。
2. `manual_subject`：B3、B5 和 V5 完整链路测试。
3. `backend_gap_review`：独立缺口复核、B4 和 V6 性能基线。
4. 主会话：整合、Provider 真实联调、基础设施验证、数据库角色约束、最终门禁与提交。

## 7. 完成门槛

- B1–B6 的正常、权限、冲突、依赖、幂等和审计路径均有自动化测试：完成。
- V1–V6 均有新鲜执行证据：完成。
- 全量 pytest、Ruff、Mypy、compileall、锁文件与迁移检查：完成。
- 所有本任务启动的服务、容器、进程、端口和媒体对象：已清理。
- 最终独立 Review 已完成三轮，发现并修复权限、所有者不变量、删除 TOCTOU、媒体 promote/delete 竞态、主体同名并发等问题；敏感信息/无关改动复核与中文 Git 提交在最终门禁后执行，不得 push。

## 8. 明确排除项

- 前端专属：搜索防抖、未保存离开拦截、高亮、画布撤销/缩放/小地图、Toast、响应式、浏览器兼容和 a11y；本任务未修改前端。
- 文档明确非范围：新闻播报实功能、实时协同、移动端、自助注册、TTS、字幕、特效/转场、多语言、画布分镜/线索面板。
- 探索清单中仅标为“待验证”且没有产品合同的登录验证码、个人资料、音乐音效、发布分享、数据导入导出，不作为已确认后端需求冒充实现。
