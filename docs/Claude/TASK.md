# 智绘 (Flow App) 后端复刻 — 任务清单 (TASK)

> 版本: 1.0 | 日期: 2026-07-15 | 范围: 后端 `/v3api/` 全量复刻
> 技术栈: Python 3.12 + FastAPI + PostgreSQL + Redis + Celery
> AI 供应商: 真实接入 (OpenAI / Google Gemini / 字节豆包 Seedance)
> 参考文档: `docs/Claude/01~08` 全部规格

---

## 0. 任务总览

后端按资源域拆分为 10 个子系统，依赖关系如下：

```
T0 基础设施  ──┬──> T1 鉴权多租户 ──┬──> T3 剧本与流水线 ──┬──> T5 画布 ──> T6 生成任务
              │                      │                        ├──> T7 分镜 ──> T8 视频生成 ──> T9 时间线
              │                      └──> T2 积分账户 ──────────┘ (贯穿所有 AI 任务)
              └──> T4 资产库 ──────────────────────────────────┘
T10 媒体代理 (sapi 等价) 贯穿 T5/T6/T8
```

可并行的模块分组（同一组内无写冲突）：
- **组 A**: T0 基础设施 + T1 鉴权 + T2 积分 (T0 先行，T1/T2 并行)
- **组 B**: T3 剧本流水线 + T4 资产库 (并行，共享 Subject 模型)
- **组 C**: T5 画布 + T10 媒体代理 (并行)
- **组 D**: T6 生成任务 + T7 分镜 (并行)
- **组 E**: T8 视频生成 + T9 时间线 (并行，T8→T9 弱依赖)

---

## T0. 基础设施与项目骨架

### T0.1 项目结构
- [ ] `backend/app/` 主包，分层: `api/` (路由) `crud/` (数据访问) `models/` (ORM) `schemas/` (Pydantic) `services/` (业务) `providers/` (AI 供应商) `core/` (配置/鉴权/中间件) `workers/` (Celery 任务)
- [ ] `backend/requirements.txt` / `pyproject.toml` 依赖锁定
- [ ] `backend/.env.example` 环境变量模板
- [ ] `backend/alembic/` 数据库迁移
- [ ] `backend/docker-compose.yml` (Postgres + Redis)
- [ ] `backend/tests/` 测试目录

### T0.2 配置与依赖注入
- [ ] `core/config.py` Pydantic Settings，读取 env (DB/Redis/AI keys/Cookie secret/存储路径)
- [ ] `core/database.py` SQLAlchemy 2.0 async engine + sessionmaker
- [ ] `core/redis.py` Redis async 连接
- [ ] `core/deps.py` FastAPI 依赖 (get_db / get_current_user / get_redis)
- [ ] `main.py` FastAPI app 工厂，挂载 `/v3api` 前缀，CORS，异常处理 `{"detail":...}` 统一错误体

### T0.3 统一约定
- [ ] 所有错误响应统一 `{"detail": "<message>"}`，401 未登录返回 `{"detail":"未登录"}`
- [ ] UUID v4 主键 (Char(36)/PG UUID)
- [ ] ISO 8601 时间，UTC 存储
- [ ] 列表响应统一 `{"items": [...], "total": N}` 或 `{"items":[...]}`
- [ ] 分页参数 `limit`/`offset`，scope=`personal|team`

**验收**: `uvicorn app.main:app --reload` 启动成功，`GET /v3api/health` 返回 200，未带 Cookie 访问受保护路由返回 401 `{"detail":"未登录"}`。

---

## T1. 鉴权与多租户

### T1.1 实体模型
- [ ] `models/organization.py` Organization (id, org_name, org_code)
- [ ] `models/department.py` Department (id, org_id, name)
- [ ] `models/user.py` User (user_id, username, password_hash, enabled, is_admin, account_type, org_id, department_id, can_manage_accounts, management_scope)
- [ ] `models/user_organization.py` 用户可访问企业关联表 (user_id, org_id, org_name, org_code)
- [ ] `models/project_member.py` 团队项目成员 (script_id, user_id, project_role)

### T1.2 会话鉴权
- [ ] `POST /v3api/auth/login` 提交 username/password，校验后 `Set-Cookie` httpOnly 会话 (signed session via itsdangerous/Starlette SessionMiddleware)，下发 `tgw_l7_route` 兼容 cookie
- [ ] `POST /v3api/auth/logout` 清除会话
- [ ] `GET /v3api/auth/me` 返回 User + organizations[] + 当前会话租户
- [ ] `core/auth.py` 会话依赖：解析 Cookie，未登录 raise 401 `{"detail":"未登录"}`
- [ ] 会话过期/无效统一 401

### T1.3 权限轴
- [ ] 组织轴：org_id / department_id / management_scope (none|department|org) / can_manage_accounts / is_admin
- [ ] 内容轴：剧本 scope (personal|team) / owner_user_id / created_by_user_id / current_user_project_role
- [ ] `core/permissions.py` 授权检查：会话有效→确定租户→加载资源→校验组织/部门成员→校验所有权或项目角色→校验动作与状态→审计事件
- [ ] 资源级授权：改 URL 中任意 id (script/episode/subject/materials/shot/job) 时服务端重新校验归属

**验收**: 登录后 `GET /v3api/auth/me` 返回完整用户对象；未登录 401；改 URL id 访问他人资源 403。

---

## T2. 积分账户体系

### T2.1 实体模型
- [ ] `models/credit.py` CreditAccount (id, owner_type user|org|department, owner_id, org_id, department_id, balance_points, frozen_points, status active|disabled, disabled_reason, disabled_at, timestamps)
- [ ] `models/credit_log.py` CreditLog (id, account_id, actor_user_id, actor_username, target_user_id, target_username, reservation_id, source_type, source_id, log_type, points_delta, frozen_delta, balance_after, frozen_after, note, created_at)
- [ ] `models/credit_reservation.py` CreditReservation (id, account_id, source_type, source_id, reserved_points, status, created_at, released_at) — 串联 reserve/settle/release

### T2.2 计费三段式服务
- [ ] `services/credits.py`:
  - `reserve(account, source_type, source_id, estimated_points)` → frozen += N, 可用 -= N, 写 reserve log + reservation
  - `settle(reservation, actual_points)` → balance -= actual, frozen -= reserved, 写 settle log (points_delta 负, frozen_delta 负)
  - `release(reservation)` → frozen -= reserved, 写 release log (失败/取消返还)
  - `refund(reservation, points)` → balance += points, 写 refund log
  - `grant(from_account, to_account, points)` → 部门下发，写 grant log，actor≠target 代付
- [ ] 所有积分操作在 DB 事务内，account 行级锁 `SELECT ... FOR UPDATE`，写流水记录 balance_after/frozen_after
- [ ] 幂等：reservation_id 串联，同一 source_id 重复 settle 拒绝

### T2.3 API 端点
- [ ] `GET /v3api/credits/accounts` 按 owner 维度返回当前用户可访问账户列表
- [ ] `GET /v3api/credits/logs?account_id=&limit=` 流水按时间倒序
- [ ] `POST /v3api/credits/grant` (管理员) 部门额度下发
- [ ] (内部接口，非暴露) 供 T6/T8 调用 reserve/settle/release

**验收**: reserve 后 frozen 增加、可用减少、余额不变；settle 后余额扣减、冻结释放；release 后冻结返还；流水 balance_after 连续一致；actor≠target 代付可执行。

---

## T3. 剧本模块与 AI 流水线

### T3.1 实体模型
- [ ] `models/script.py` Script (id, title, scope personal|team, owner_user_id, org_id, created_by_user_id, timestamps)
- [ ] `models/episode.py` Episode (episode_id, script_id, episode_no, title, script_text, script_file_storage_key, schema_version, episode_version, client_save_seq, timestamps)
- [ ] `models/subject.py` Subject (id, script_id, type character|scene|prop|sound, name, description, is_child, is_parent, parent_id, episode_nos JSON, review_status, timestamps)
- [ ] `models/parent_subject.py` ParentSubject (id, script_id, name, type, description, child_count, timestamps)
- [ ] `models/pipeline_stage.py` PipelineStage (id, script_id, stage split|extract|merge|enrich|parents, status pending|running|done|failed, detail, current, total, timestamps)
- [ ] `models/pipeline_job.py` PipelineJob (id, script_id, stage, episode_id, status, input_version, attempt, timestamps) — 失败重试创建新 attempt，原记录不可改写

### T3.2 API 端点
- [ ] `GET /v3api/scripts/page?limit=&offset=&scope=personal|team` 列表，团队项目带 current_user_project_role
- [ ] `POST /v3api/scripts` 新建剧本 {title, scope}
- [ ] `GET /v3api/scripts/<id>` 详情
- [ ] `GET /v3api/scripts/<id>/episodes` 分集列表
- [ ] `POST /v3api/scripts/<id>/episodes` 新增分集
- [ ] `PUT /v3api/scripts/<id>/episodes/<eid>` 保存：带 episode_version/client_save_seq 乐观锁，冲突 409，递增 version，触发重新提取
- [ ] `GET /v3api/scripts/<id>/episodes/<eid>/breakdown` 返回 {episode, subjects[], shots[], shot_groups[]}
- [ ] `GET /v3api/scripts/<id>/pipeline-status` 5 阶段状态 + running/failed/all_done
- [ ] `GET /v3api/scripts/<id>/extract-subjects-status` 提取+合并状态 (merge_job_id, has_active_merge)
- [ ] `GET /v3api/scripts/news-broadcast/jobs` 返回 [] (本期不实现)

### T3.3 流水线 worker (Celery)
- [ ] `workers/pipeline.py` job 链：split → extract(每集) → merge → enrich → parents
- [ ] 阶段按 DAG 校验（enrich 可在 parents done 时仍 pending，非严格串行）
- [ ] 任务绑定输入版本：旧任务成功不覆盖新文本结果
- [ ] 失败保留结构化错误 + 可重试性，重试创建新 attempt

### T3.4 LLM Provider (LLM-1~5)
- [ ] `providers/llm.py` 抽象 `LLMProvider` (chat/complete JSON 模式)
- [ ] `providers/llm_openai.py` OpenAI 实现 (gpt-4o/4o-mini)
- [ ] `services/pipeline/`:
  - `split.py` 剧本拆分 (整段→集列表)
  - `extract.py` 主体提取 (单集→subjects[type/name/description/is_child/episode_nos]，description 详细可用作生图/配音 prompt)
  - `merge.py` 跨集合并 (同名同类 episode_nos 取并集)
  - `enrich.py` 设计线索增强 (12 段落摄影级 prompt → resolved_prompts.final)
  - `parents.py` 父主体归纳 (child_count 形态数)
- [ ] 各阶段 prompt 骨架严格遵循 `06-AI能力与Prompt工程文档` 第 12 节契约

**验收**: 输入剧本文本，pipeline-status 5 阶段逐个 done，extract 产出 4 类主体含详细 description，merge 跨集去重 episode_nos 取并集，enrich 产出 12 段落 prompt，parents 产出父子层次 + child_count；并发保存 episode_version 冲突返回 409。

---

## T4. 资产库模块

### T4.1 实体模型
- [ ] `models/asset_entry.py` AssetEntry (id, script_id, subject_id, kind image|audio, storage_key, mime_type, size_bytes, source generate_images|upload_images, sha256, is_approved, timestamps)
- [ ] Subject 模型扩展: review_status (not_designed|pending|approved), image_entry_id, pending_candidate_count (计算字段), whitelist_status

### T4.2 API 端点
- [ ] `GET /v3api/scripts/<id>/asset/entries?limit=&offset=` 资产条目 (列表页取计数)
- [ ] `GET /v3api/scripts/<id>/asset-library/subjects?limit=&offset=` 主体列表 (含 image_entry/audio_entries/pending_candidate_count/review_status_label)
- [ ] `GET /v3api/scripts/<id>/asset-library/parent-subjects` 父主体列表 (child_count)
- [ ] `PATCH /v3api/scripts/<id>/asset-library/subjects/<sid>` 编辑主体 (name/description/type/episode_nos)
- [ ] `DELETE /v3api/scripts/<id>/asset-library/subjects/<sid>` 删除主体
- [ ] `GET /v3api/scripts/<id>/asset-library/subjects/<sid>/candidates` 候选图片+声音列表
- [ ] `POST /v3api/scripts/<id>/asset-library/subjects/<sid>/approve-image` 设为 image_entry，review_status=approved
- [ ] `DELETE /v3api/scripts/<id>/asset-library/subjects/<sid>/candidates/<cid>` 移出队列
- [ ] `POST /v3api/scripts/<id>/asset-library/subjects/<sid>/approve-audio` 挂载 audio_entries
- [ ] `GET /v3api/scripts/<id>/canvas-project/ensure` 幂等创建/返回 materials_id

### T4.3 审核状态机
- [ ] not_designed → (生图成功) → pending (候选) → (通过图片) → approved
- [ ] pending → (删除候选) → not_designed/rejected
- [ ] approved → (通过新版本) → superseded
- [ ] 两审核者同时操作：后提交者收到冲突，不覆盖新正式资产
- [ ] 替换被镜头引用的正式资产前显示影响摘要

**验收**: 主体列表按父主体分组；统计已审核/未审核/全部；审核状态机三态切换正确；background 类型 UI 显示"场景"；替换被引用资产前确认。

---

## T5. 画布模块 (Canvas)

### T5.1 实体模型
- [ ] `models/materials.py` Materials (id, name, script_id, timestamps) — 1 剧本 1 画布项目
- [ ] `models/canvas_node.py` CanvasNode (id, material_id, type imageUpload|text|generated|uploaded|storyboard, position JSON, data JSON, timestamps)
- [ ] `models/canvas_edge.py` CanvasEdge (id, material_id, source, target, data JSON, timestamps)

### T5.2 API 端点
- [ ] `GET /v3api/scripts/<script_id>/canvas-project/ensure` 幂等 (T4 已列，此处实现)
- [ ] `GET /v3api/materials/<id>` 画布项目元数据
- [ ] `GET /v3api/materials/<id>/canvas` 返回 {material_id, nodes[], edges[]}
- [ ] `POST /v3api/materials/<mid>/canvas/delta` 增量保存 (节点移动/增删/参数变更的 delta 差异)
- [ ] `GET /v3api/materials/<mid>/image/<image_id>` 图片资产元数据 (storage_key/mime_type/size_bytes/source)

### T5.3 节点 data 模型
- [ ] imageUpload 节点 data 完整字段: title/variant/generated/uploaded/text/storyboard/imageUrl/prompt/model(banana-2|gpt-image-2)/size/aspect/isGenerating/generationJobId/subjectId/subjectName/subjectType/subjectDescription/subjectEpisodeIds/subjectEpisodeNos/subjectDesignStatus/promptVariationIndex/imagesExpanded/generatingStartedAt/assetId/imageOutputs[]/primaryImageIndex
- [ ] prompt 自动构造: 固定模板 + 主体名/类型/描述 + 要求

**验收**: ensure 幂等；canvas/delta 移动后刷新位置一致；image 元数据返回正确；图片实际 URL 走 sapi 代理路由 (T10)。

---

## T6. 生成任务模块 (Generation Jobs)

### T6.1 实体模型
- [ ] `models/generation_job.py` GenerationJob (job_id, material_id, script_id, episode_id, scene_no, job_type generate_images, status starting|processing|succeeded|failed|canceled, progress 0-100, progress_stage, progress_message, error_message, input_json JSON, output_json JSON, metrics JSON, timestamps)
- [ ] input_json 含 prompt + resolved_prompts.final

### T6.2 API 端点
- [ ] `POST /v3api/generation-jobs` 创建生图任务 (绑定 material_id + subjectId + model)，触发积分 reserve → 异步 worker → settle/release
- [ ] `GET /v3api/generation-jobs/<job_id>` 状态轮询
- [ ] `GET /v3api/generation-jobs?material_id=` 按画布过滤

### T6.3 文生图 Provider
- [ ] `providers/image_gen.py` 抽象 `ImageGenProvider`
- [ ] `providers/image_gen_google.py` banana-2 = Google Nano Banana (Gemini 3.1 Flash Image)，2k 16:9
- [ ] `providers/image_gen_openai.py` gpt-image-2 = OpenAI GPT Image 2
- [ ] `workers/generation.py` Celery worker：读 input_json → 调 provider → 存图 (storage_key=/generate_images/<mid>/<id>.png) → 更新 imageOutputs[] → settle 积分
- [ ] 支持生成多张选一 (imageOutputs[] + primaryImageIndex)
- [ ] 失败 release 积分，写 error_message

### T6.4 与资产库联动
- [ ] 生成成功后图片进入审核候选队列 (subject.pending_candidate_count++)
- [ ] 未通过审核的图片不进入视频生成参考图汇聚

**验收**: 创建任务后轮询到 succeeded；input_json.resolved_prompts.final 含 12 段落；生成图存储到 /generate_images/；积分 reserve→settle 流水正确；失败 release 释放冻结。

---

## T7. 分镜模块 (Shots)

### T7.1 实体模型
- [ ] `models/scene.py` Scene (id, episode_id, scene_no, scene_key bg:<background>, background, label, shot_group_count, pending_shot_count)
- [ ] `models/shot_group.py` ShotGroup (shot_group_id, scene_no, group_no G1, prompt, reference_frames JSON {first_frame,last_frame,reference_frame}, duration_seconds, video_duration_seconds, materials JSON {character[],background[],prop[],other[]}, audio_elements JSON, description_text, unit_id group:<id>, unit_runtime_key, timestamps)
- [ ] `models/shot.py` Shot (shot_id, scene_no, shot_group_id, group_no, shot_no, display_no, content, description)

### T7.2 API 端点
- [ ] `POST /v3api/scripts/<sid>/episodes/<eid>/generate-storyboard` 触发 LLM 分镜生成 (异步 job)
- [ ] `GET /v3api/scripts/<sid>/episodes/<eid>/shot-hierarchy` 返回 {episode_id, episode_version, scenes[]: {scene_no, groups[]: {shot_group_id, scene_no, group_no, prompt, reference_frames, shots[]}, ungrouped_shots[]}}
- [ ] `GET /v3api/shots/<sid>/episodes/<eid>/video-workbench` 返回 workbench (含 unit_id/unit_runtime_key/materials/audio_elements/duration_seconds)
- [ ] `GET /v3api/shots/<script_id>/cover` 视频项目封面 (首个镜头首帧)

### T7.3 LLM-5 分镜生成
- [ ] `services/storyboard.py` 输入单集剧本+主体列表 → LLM 产出 场景→镜头组→镜头，每镜头 content + description，每镜头组 prompt (多镜头分时段 + 【角色】+【叙事目的】+【约束】)
- [ ] description 结构化模板: "在【背景】中采用【机位】拍摄视角，画面主体【角色】关键动作【】，整体画面【】，表达了【情绪】，光影【】，叙事目的【】。"
- [ ] 约束锁固定 9 条 (一致性/不脸崩/不畸形/切镜丝滑/4K/语音清晰/情感贴合/唇形对位/禁字幕) 放 prompt 末尾

### T7.4 materials 自动汇聚
- [ ] 分镜生成时从 prompt 解析引用主体名，填充 shot_group.materials {character/background/prop/other}
- [ ] audio_elements 需配音角色

**验收**: 分镜生成产出三层层级；shot_group.prompt 含【镜头N:起~止s】+【角色】+【叙事目的】+【约束】；video-workbench 含 unit_id=group:<id>；materials 正确分组。

---

## T8. 视频生成模块

### T8.1 实体模型
- [ ] `models/video_generation_job.py` VideoGenerationJob (job_id, script_id, episode_id, scene_no, unit_id group:<shot_group_id>, unit_runtime_key, provider_key jianyu|modelsai, job_type all_reference, status, progress, progress_stage, progress_message, error_message, input_json JSON {model doubao-seedance-2-0-260128, reference_images[], reference_image_names[], reference_audios[], reference_audio_names[], reference_videos[], reference_video_names[], prompt}, output_json, metrics, timestamps)
- [ ] `models/video_provider.py` VideoProviderConfig (provider_key, job_type, model, is_active)

### T8.2 API 端点
- [ ] `POST /v3api/video-generation-jobs` 创建视频生成任务 (绑定 shot_group_id)，自动汇聚 reference_images → 积分 reserve → 异步 worker
- [ ] `GET /v3api/video-generation-jobs?script_id=&episode_id=&limit=` 列表轮询
- [ ] `GET /v3api/video-generation/<job_id>` 单任务状态
- [ ] `GET /v3api/video-generation/active-provider` 当前激活供应商+模型 {provider_key, job_type, model}
- [ ] `POST /v3api/video-generation/active-provider` 切换供应商 (管理员)

### T8.3 图生视频 Provider
- [ ] `providers/video_gen.py` 抽象 `VideoGenProvider`
- [ ] `providers/video_gen_volcengine.py` 字节豆包 Seedance 2.0 (doubao-seedance-2-0-260128)，all_reference 多参考图生视频，走 volcengine SDK/HTTP
- [ ] jianyu / modelsai 两个 provider_key 路由到同一底层模型，可切换
- [ ] `workers/video_generation.py` Celery worker：汇聚参考图 → 调 provider → 存视频 → settle 积分

### T8.4 多参考图自动汇聚
- [ ] 解析 shot_group.materials 各类别主体名 → 查每主体已通过 image_entry (approved) → 汇集为 reference_images[] + reference_image_names[] (一一对应)
- [ ] 参考图元数据: {kind:"storage", storage_key, mime_type, size_bytes, sha256}
- [ ] 未通过主体 (image_entry==null) 不进入参考图，[检查素材] 接口标红缺失

### T8.5 首尾帧连续性
- [ ] reference_frames {first_frame, last_frame, reference_frame}
- [ ] 链式衔接：上一组 last_frame → 下一组 first_frame
- [ ] 生成后从视频末尾提取帧填充 last_frame

**验收**: 视频生成按 provider_key/model 路由；进度 0-100 轮询到 succeeded；reference_images 与主体名一一对应；未通过主体不进参考图；失败 release 积分；provider 可切换。

---

## T9. 时间线编排模块

### T9.1 实体模型
- [ ] `models/video_timeline_arrangement.py` VideoTimelineArrangement (id, script_id, episode_id, unit_runtime_key, job_id, row_order, is_selected, trim_start_seconds float, trim_end_seconds float|null, timestamps)

### T9.2 API 端点
- [ ] `GET /v3api/video-timeline-arrangements?script_id=&episode_id=` 返回 {script_id, episode_id, items[]}
- [ ] `POST /v3api/video-timeline-arrangements` 添加片段到时间线 (关联 job_id)
- [ ] `PATCH /v3api/video-timeline-arrangements/<id>` 更新 row_order / trim_start / trim_end / is_selected
- [ ] `PUT /v3api/video-timeline-arrangements/reorder` 批量重排序
- [ ] `POST /v3api/video-timeline-arrangements/render` 合成最终视频 (按 row_order + trim 裁剪，异步 job)

**验收**: 时间线按 row_order 排列；trim_start/end_seconds 控制裁剪；合成任务异步可轮询。

---

## T10. 媒体代理模块 (sapi 等价)

### T10.1 对象存储抽象
- [ ] `providers/storage.py` 抽象 `StorageProvider` (put/get/delete/signed_url)
- [ ] `providers/storage_local.py` 本地文件系统实现 (storage/generate_images/ + upload_images/) — 开发默认
- [ ] `providers/storage_s3.py` S3 兼容实现 (MinIO/COS) — 生产可选

### T10.2 鉴权代理路由
- [ ] `GET /api/x/cctv/zhihui/flow-app/{storage_key:path}` 鉴权代理：会话校验 → 流式返回媒体 (等价 sapi.yangshipin.cn)
- [ ] `POST /api/x/cctv/zhihui/flow-app/upload_images/...` 上传 (用户上传图/视频/音频)
- [ ] storage_key 两类前缀: `/generate_images/<mid>/<id>.png` (AI 生成) / `/upload_images/<mid>/<id>.<ext>` (用户上传)
- [ ] 上传校验真实 MIME 与媒体解析结果 (Pillow/pydub/ffmpeg 探测)

### T10.3 封面生成
- [ ] 资产库列表封面: 按标题确定性生成 3 段线性渐变 SVG + 高斯模糊 (data:image/svg+xml，零存储)
- [ ] 视频项目封面: 首个镜头首帧 (GET /v3api/shots/<script_id>/cover)

**验收**: 前端不直连底层存储，全经代理；storage_key 两类前缀；SVG 封面确定性可复现；上传 MIME 校验。

---

## 横向验收 (跨模块)

### H1 端到端流程
- [ ] 剧本输入 → split → extract → merge → enrich → parents → 资产生成 → 审核 → 画布排布 → 分镜 → 视频生成 → 时间线合成 全链路跑通 1 个剧本
- [ ] 积分三段式在视频生成任务上正确扣减与返还
- [ ] 资产审核候选→通过→image_entry 全流程
- [ ] 画布 delta 保存刷新一致
- [ ] 分镜首尾帧链式衔接
- [ ] 视频生成多参考图自动汇聚

### H2 非功能
- [ ] httpOnly Cookie 鉴权，401 `{"detail":"未登录"}`
- [ ] 乐观锁 episode_version/client_save_seq 并发冲突拒绝
- [ ] 异步任务 status/progress/progress_stage 轮询可达终态
- [ ] 错误体统一 `{"detail":...}`
- [ ] 多租户 scope personal/team 隔离
- [ ] tgw_l7_route 会话粘性兼容

---

## 环境变量 (需用户提供真实 AI key)

```
DATABASE_URL=postgresql+asyncpg://flow:flow@localhost:5432/flowapp
REDIS_URL=redis://localhost:6379/0
SESSION_SECRET=<随机长字符串>
STORAGE_DRIVER=local  # local | s3
STORAGE_ROOT=./storage

# AI 供应商 (真实接入)
OPENAI_API_KEY=sk-...
OPENAI_IMAGE_MODEL=gpt-image-2
OPENAI_LLM_MODEL=gpt-4o

GOOGLE_API_KEY=...
GOOGLE_IMAGE_MODEL=gemini-3.1-flash-image  # banana-2 / Nano Banana

VOLCENGINE_ACCESS_KEY=...   # 字节豆包 Seedance
VOLCENGINE_SECRET_KEY=...
SEEDANCE_MODEL=doubao-seedance-2-0-260128

# 积分定价 (单位: 积分)
PRICE_EXTRACT_PER_EPISODE=10
PRICE_MERGE=20
PRICE_ENRICH_PER_SUBJECT=5
PRICE_IMAGE_GEN=30
PRICE_STORYBOARD_PER_EPISODE=50
PRICE_VIDEO_GEN=900   # 784~994 实测区间中值
PRICE_VIDEO_GEN_FROZEN=210
```
