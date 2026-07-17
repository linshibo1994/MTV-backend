# 数据模型与 API 参考文档

> 适用对象：后端工程师 / 全栈工程师 / AI 流水线工程师
> 目标：基于本文档可从零复刻"智绘 (Flow App)"后端全部实体模型与接口契约。
> 事实来源：探索笔记（00-05），本文不臆造笔记未记录的功能。

---

## 0. 全局约定

| 项目 | 值 |
|---|---|
| 平台名称 | 智绘 / Flow App |
| 前端根路由 | `https://zhihui.yangshipin.cn/v3/` |
| 后端 API 基址 | `https://zhihui.yangshipin.cn/v3api/` |
| 图片/对象存储基址 | `https://sapi.yangshipin.cn/api/x/cctv/zhihui/flow-app/` |
| 服务器标识 | `IIS/1.1.1`（疑似 ASP.NET / .NET 后端） |
| 鉴权方式 | httpOnly Cookie 会话（JS 不可见），登录端点提交后 `Set-Cookie` |
| 网关 | 腾讯云 L7 `tgw_l7_route` cookie，`yangshipin.cn` 为央视频域名 |
| 未登录响应 | HTTP 401，`{"detail":"未登录"}` |
| ID 格式 | UUID v4 字符串 |
| 时间格式 | ISO 8601 字符串（如 `2026-07-14T06:37:36`） |
| 异步任务 | 前端轮询 `status` / `progress` 字段，完成状态为 `succeeded` / `failed` / `canceled` |

### 命名规约
- 所有 UUID 字段以 `_id` 或 `id` 结尾（如 `script_id`、`subject_id`、`job_id`）。
- 主体类型（subject_type）在剧本拆解中用 `character/scene/prop/sound`；在资产库中 `background` 与 `scene` 同义（均表示场景）。
- 异步流水线阶段名见 1.3 节定义。

---

## 1. 实体模型

### 1.1 User（用户）

来源：`GET /v3api/auth/me`。

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | string(UUID) | 用户唯一 ID |
| username | string | 用户名（登录名） |
| enabled | boolean | 是否启用 |
| is_admin | boolean | 是否管理员 |
| account_type | enum | `personal` / `team` |
| org_id | string(UUID) | 所属企业 ID |
| department_id | string(UUID) | 所属部门 ID |
| can_manage_accounts | boolean | 是否可管理积分账户 |
| management_scope | enum | 账户管理范围：`none` / `department` / `org` |
| organizations | Organization[] | 所属企业列表 |

**枚举**

| 字段 | 取值 | 含义 |
|---|---|---|
| account_type | `personal` | 个人账号 |
| | `team` | 团队账号 |
| management_scope | `none` | 不可管理账户 |
| | `department` | 可管理本部门账户 |
| | `org` | 可管理本企业账户 |

### 1.2 Organization（企业）与 Department（部门）

Organization（企业，来自 `auth/me` 的 `organizations` 数组）：

| 字段 | 类型 | 说明 |
|---|---|---|
| id | string(UUID) | 企业 ID |
| org_name | string | 企业名称（如"默认企业"） |
| org_code | string | 企业编码（如 `default-enterprise`） |

Department（部门）：通过 `department_id` 关联，多租户层级为 `用户 -> 部门(department) -> 企业(org)`。部门/企业为积分账户的 owner 维度之一。

### 1.3 CreditAccount（积分账户）

来源：`GET /v3api/credits/accounts`。一个账户按 owner 维度（用户/组织/部门）挂载。

| 字段 | 类型 | 说明 |
|---|---|---|
| id | string(UUID) | 账户 ID |
| owner_type | enum | `user` / `org` / `department` |
| owner_id | string(UUID) | owner 实体 ID |
| org_id | string(UUID) | 所属企业 |
| department_id | string(UUID) | 所属部门 |
| balance_points | int | 余额（实际可用基础） |
| frozen_points | int | 冻结（预扣/进行中任务占用） |
| status | enum | `active` / `disabled` |
| disabled_reason | string\|null | 禁用原因 |
| disabled_at | string\|null | 禁用时间 |
| created_at | string(ISO) | 创建时间 |
| updated_at | string(ISO) | 更新时间 |

- 前端展示"可用积分" = `balance_points - frozen_points`。

**枚举**

| 字段 | 取值 | 含义 |
|---|---|---|
| owner_type | `user` | 用户级账户 |
| | `org` | 企业级账户 |
| | `department` | 部门级账户 |
| status | `active` | 正常 |
| | `disabled` | 禁用 |

### 1.4 CreditLog（积分流水）

来源：`GET /v3api/credits/logs`。

| 字段 | 类型 | 说明 |
|---|---|---|
| id | string(UUID) | 流水 ID |
| account_id | string(UUID) | 关联账户 |
| actor_user_id | string(UUID) | 操作者用户 ID |
| actor_username | string | 操作者用户名 |
| target_user_id | string(UUID) | 被操作账户归属用户 ID |
| target_username | string | 被操作账户归属用户名 |
| reservation_id | string(UUID) | 预扣预约 ID（冻结/结算关联） |
| source_type | enum | 来源类型 |
| source_id | string(UUID) | 关联任务/对象 ID |
| log_type | enum | 流水类型 |
| points_delta | int | 余额变动（负=消耗） |
| frozen_delta | int | 冻结变动 |
| balance_after | int | 操作后余额 |
| frozen_after | int | 操作后冻结 |
| note | string\|null | 备注 |
| created_at | string(ISO) | 时间 |

**枚举**

| 字段 | 取值 | 含义 |
|---|---|---|
| log_type | `reserve` | 冻结预扣 |
| | `settle` | 结算扣余额并释放冻结 |
| | `release` | 释放冻结（失败/取消返还） |
| | `grant` | 下发/授予 |
| | `refund` | 返还 |
| source_type | `generation_job` | 生成任务消耗 |
| | `manual` | 人工操作 |
| | `admin_grant` | 管理员授予 |
| | （其他） | 待扩展 |

**计费三段式（异步任务积分预扣）**

1. **reserve 冻结**：任务启动按预估预扣，`frozen_points` 增加（可用减少，余额不变）。
2. **settle 结算**：任务完成按实际消耗结算，`points_delta`（负，扣余额）、`frozen_delta`（负，释放冻结）。
3. **release/refund 释放/返还**：任务失败/取消，释放冻结返还到余额。
- 支持组织/部门代付：`actor ≠ target`（管理员为下属账户操作）。

### 1.5 Script（剧本）

来源：`GET /v3api/scripts/<id>` 与 `GET /v3api/scripts/page`。

| 字段 | 类型 | 说明 |
|---|---|---|
| id | string(UUID) | 剧本 ID |
| title | string | 标题 |
| scope | enum | `personal` / `team` |
| owner_user_id | string(UUID) | 所有者用户 |
| owner_username | string | 所有者用户名（列表项） |
| org_id | string(UUID)\|null | 企业 ID（个人项目为 null） |
| created_by_user_id | string(UUID) | 创建者 |
| current_user_project_role | string\|null | 当前用户在该项目的角色（团队协作权限，列表项） |
| created_at | string(ISO) | 创建时间 |
| updated_at | string(ISO) | 更新时间 |

### 1.6 Episode（分集）

来源：`GET /v3api/scripts/<id>/episodes`。

| 字段 | 类型 | 说明 |
|---|---|---|
| episode_id | string(UUID) | 分集 ID |
| script_id | string(UUID) | 所属剧本 |
| episode_no | int | 集号（从 1 起） |
| title | string | 集标题（如"第 1 集"） |
| episode_version | int | 乐观锁版本号 |
| updated_at | string(ISO) | 更新时间 |

breakdown 中完整 Episode 对象包含更多字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| script_text | string | 剧本文本（大文本） |
| script_file_storage_key | string\|null | 剧本文件对象存储 key（支持文件存储，为 null 表示纯文本） |
| schema_version | int | 数据结构版本 |
| client_save_seq | int | 客户端保存序号（乐观并发） |
| created_at | string(ISO) | 创建时间 |

### 1.7 Subject（资产主体）

来源：`GET /v3api/scripts/<id>/asset-library/subjects` 与 breakdown `subjects[]`。

资产库视角字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| subject_id | string(UUID) | 主体 ID |
| subject_name | string | 主体名称 |
| subject_type | enum | `character` / `background` / `prop` / `sound`（`background` 即 `scene`） |
| subject_description | string\|null | AI 生成描述（外观/音色，可直接作为生图/配音 prompt） |
| episodes / episode_ids / episode_nos | array | 出现的集（int 集号） |
| parent_id | string(UUID)\|null | 父主体 ID |
| parent_name | string\|null | 父主体名称 |
| is_parent | boolean | 是否父主体 |
| review_status | enum | `not_designed` / `approved` / `pending` |
| review_status_label | string | 中文标签：未通过/已通过/未审核 |
| pending_candidate_count | int | 待审候选数 |
| image_entry | object\|null | 已通过图片资产（单个） |
| audio_entries | array | 挂载的声音资产（多个） |
| whitelist_status | string\|null | 白名单状态 |
| created_at | string(ISO) | 创建时间 |
| updated_at | string(ISO) | 更新时间 |

breakdown 视角字段（精简）：

| 字段 | 类型 | 说明 |
|---|---|---|
| id | string(UUID) | 主体 ID |
| type | enum | `character` / `scene` / `prop` / `sound` |
| name | string | 名称 |
| description | string | AI 生成描述 |
| is_child | boolean | 是否子主体 |
| episode_nos | int[] | 出现集号 |

**枚举**

| 字段 | 取值 | 含义 |
|---|---|---|
| subject_type | `character` | 角色 |
| | `scene` / `background` | 场景（两名称同义，breakdown 用 scene，资产库用 background） |
| | `prop` | 道具 |
| | `sound` | 声音/配音音色 |
| review_status | `not_designed` | 未设计/未通过 |
| | `approved` | 已通过 |
| | `pending` | 待审 |

### 1.8 ParentSubject（父主体）

来源：`GET /v3api/scripts/<id>/asset-library/parent-subjects`。父主体为同类子主体的聚合（跨集去重）。

| 字段 | 类型 | 说明 |
|---|---|---|
| subject_id | string(UUID) | 父主体 ID |
| name | string | 名称 |
| type | enum | `character`/`background`/`prop`/`sound` |
| description | string | 描述 |
| child_count | int | 形态数（子主体数量） |
| created_at | string(ISO) | 创建时间 |
| updated_at | string(ISO) | 更新时间 |

### 1.9 AssetEntry（资产条目）

`image_entry`（单个已通过图片资产）与 `audio_entries`（多个声音资产）挂载于 Subject。审核候选进入 `pending_candidate_count`。

**image_entry / 候选图片资产字段（推定）**

| 字段 | 类型 | 说明 |
|---|---|---|
| asset_id / image_id | string(UUID) | 资产 ID |
| storage_key | string | 存储路径（如 `/generate_images/<mid>/<id>.png`） |
| mime_type | string | MIME 类型 |
| size_bytes | int | 字节数 |
| source | enum | `generate_images` / `upload_images` |

**审核流程**：生成图片 -> 进入审核队列（候选/pending_candidate） -> 人工审核 -> 通过则成为该主体 `image_entry`；删除则移出队列。

### 1.10 Materials（画布项目）

来源：`GET /v3api/materials/<id>`。一个剧本对应一个 materials 画布项目。

| 字段 | 类型 | 说明 |
|---|---|---|
| id | string(UUID) | 画布项目 ID（materials_id） |
| name | string | 名称（同剧本标题） |
| script_id | string(UUID) | 关联剧本 |
| created_at | string(ISO) | 创建时间 |
| updated_at | string(ISO) | 更新时间 |

### 1.11 CanvasNode（画布节点）

来源：`GET /v3api/materials/<id>/canvas` 的 `nodes[]`，基于 React Flow。

节点对象：

| 字段 | 类型 | 说明 |
|---|---|---|
| id | string(UUID) | 节点 ID |
| type | string | 节点类型，已观察 `imageUpload`（还有 text/generated/uploaded/storyboard 等） |
| position | {x:float, y:float} | 画布坐标 |
| data | object | 节点数据（见下） |

`data` 字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| title | string | 节点标题 |
| variant | enum | `generated` / `uploaded` / `text` / `storyboard` 等 |
| imageUrl | string\|null | 图片 URL（通过 `/materials/<mid>/image/<id>` 单独加载，节点中为 null） |
| prompt | string | 生图 prompt（固定模板+主体名/类型/描述+要求） |
| model | enum | `banana-2` / `gpt-image-2` |
| size | string | 尺寸（如 `2k`） |
| aspect | string | 比例（如 `16:9`） |
| isGenerating | boolean | 是否生成中 |
| generationJobId | string\|null | 关联生成任务 ID |
| subjectId | string(UUID) | 关联主体 ID |
| subjectName | string | 主体名称 |
| subjectType | enum | `character`/`scene`/`prop`/`sound` |
| subjectDescription | string | 主体描述 |
| subjectEpisodeIds | array | 主体所属集 ID |
| subjectEpisodeNos | int[] | 主体所属集号 |
| subjectDesignStatus | enum | `draft` 等 |
| promptVariationIndex | int | prompt 变体序号 |
| imagesExpanded | boolean | 是否展开多图 |
| generatingStartedAt | int(毫秒时间戳) | 生成开始时间 |
| assetId | string(UUID) | 关联图片资产 |
| imageOutputs | array | 生成结果列表 |
| primaryImageIndex | int | 主选图序号 |

**imageOutputs 元素**

| 字段 | 类型 | 说明 |
|---|---|---|
| status | enum | `succeeded`/`failed`/... |
| assetId | string(UUID) | 资产 ID |
| storageKey | string | 存储路径 |

**model 枚举**

| 值 | 含义 |
|---|---|
| `banana-2` | Google Nano Banana（Gemini 2.5 Flash Image）— 主体参考图 |
| `gpt-image-2` | OpenAI GPT Image 2 |

**variant 枚举（已观察）**：`generated` / `uploaded` / `text` / `storyboard`（可能还有更多）。

**subjectDesignStatus 枚举**：`draft`（草稿）等，对应资产库 `review_status`。

### 1.12 ImageAsset（图片资产元数据）

来源：`GET /v3api/materials/<mid>/image/<image_id>`。

| 字段 | 类型 | 说明 |
|---|---|---|
| image_id | string(UUID) | 图片 ID |
| storage_key | string | 存储路径（如 `/generate_images/<mid>/<id>.png` 或 `/upload_images/...`） |
| mime_type | string | MIME（如 `image/png`） |
| size_bytes | int | 字节数 |
| source | enum | `generate_images` / `upload_images` |

实际图片 URL：`https://sapi.yangshipin.cn/api/x/cctv/zhihui/flow-app/<storage_key>`。

### 1.13 GenerationJob（图片生成任务）

来源：`GET /v3api/generation-jobs/<job_id>`。异步任务，前端轮询。

| 字段 | 类型 | 说明 |
|---|---|---|
| job_id | string(UUID) | 任务 ID |
| material_id | string(UUID) | 画布项目 |
| script_id | string(UUID)\|null | 剧本 |
| episode_id | string(UUID)\|null | 分集 |
| scene_no | int\|null | 场景号 |
| job_type | enum | `generate_images`（可能还有其他） |
| status | enum | `starting`/`processing`/`succeeded`/`failed`/`canceled` |
| progress | int | 0-100 |
| progress_stage | string\|null | 阶段 |
| progress_message | string\|null | 阶段消息 |
| error_message | string\|null | 错误 |
| input_json | string(JSON) | 输入，含 `prompt` 与 `resolved_prompts.final` |

`input_json` 内含被 LLM 扩展后的超详细 prompt（`resolved_prompts.final`），即剧本流水线"enrich 设计线索增强"阶段产物，含段落：形制描述 / 材质钉死 / 使用痕迹 / 摆放姿态 / 反悬浮锁 / 时代锚定 / 画面构图 / 画风统一锁 / 三点柔光箱白底布光 / 反水印锁 / 文字渲染降级方案 / 写实质感锚定（Phase One 中画幅+100mm 微距+f/8）。

### 1.14 Shot（镜头）

来源：`GET /v3api/scripts/<sid>/episodes/<eid>/shot-hierarchy` 的 `scenes[].groups[].shots[]`。

| 字段 | 类型 | 说明 |
|---|---|---|
| shot_id | string(UUID) | 镜头 ID |
| scene_no | int | 场景号 |
| shot_group_id | string(UUID) | 所属镜头组 |
| group_no | string | 组号（如 `G1`） |
| shot_no | string | 混淆 id 串（如 `2x2x2x2x2x`） |
| display_no | int | 显示序号 |
| content | string | 镜头画面内容（自然语言） |
| description | string | 摄影级结构化描述（见下） |

`description` 结构：在【背景】采用【机位】视角 + 画面主体【角色】+ 关键动作【】+ 整体画面【】+ 表达情绪【】+ 光影效果【】+ 叙事目的【】。

### 1.15 ShotGroup（镜头组，视频生成单元）

来源：shot-hierarchy 与 video-workbench。

shot-hierarchy 视角字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| shot_group_id | string(UUID) | 组 ID |
| scene_no | int | 场景号 |
| group_no | string | 组号 |
| prompt | string | 多镜头分时段组合 prompt（【镜头1:0~4s】【镜头2:4~7s】... + 【角色】【叙事目的】【约束】） |
| reference_frames | object | {first_frame, last_frame, reference_frame} |
| shots | Shot[] | 组内镜头 |

video-workbench 视角额外字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| unit_id | string | 生成单元 ID（如 `group:<uuid>`） |
| unit_runtime_key | string | 运行时 key（如 `<runtime>:<scene_no>:group:<uuid>`） |
| duration_seconds | int | 视频时长（秒） |
| video_duration_seconds | int | 视频时长 |
| materials | object | {character[], background[], prop[], other[]} 引用主体资产 |
| audio_elements | string[] | 需配音角色 |
| shot_ids | string[] | 组内镜头 ID |
| shot_display_nos | int[] | 显示序号 |
| shot_nos | string[] | 镜头号 |
| description_text | string | 描述文本 |

**reference_frames**

| 字段 | 类型 | 说明 |
|---|---|---|
| first_frame | string\|null | 首帧（与上一组尾帧衔接） |
| last_frame | string\|null | 尾帧 |
| reference_frame | string\|null | 参考帧 |

**分镜 prompt 结构（LLM 生成）**

- 【镜头N:起~止s】场景是X，景别·机位，运镜（固定/推轨/拉轨），光影，画面动作，情绪，【角色名】:台词
- 【角色】列出引用主体
- 【叙事目的】
- 【约束】人物特征一致/不脸崩/不畸形/切镜丝滑/4K 稳定/语音清晰/唇形动作对位/禁字幕

### 1.16 Scene（场景，按背景聚合）

来源：shot-hierarchy / video-workbench 的 `scenes[]`。

shot-hierarchy 视角：

| 字段 | 类型 | 说明 |
|---|---|---|
| scene_no | int | 场景号 |
| groups | ShotGroup[] | 镜头组 |
| ungrouped_shots | Shot[] | 未分组镜头 |

video-workbench 视角：

| 字段 | 类型 | 说明 |
|---|---|---|
| scene_key | string | 场景 key（`bg:<背景名>`，如 `bg:养鸡场办公室-内-夜`） |
| scene_no | int | 场景号 |
| background | string | 背景主体名 |
| label | string | 显示标签（如"场 1 · 养鸡场办公室-内-夜"） |
| shot_group_count | int | 镜头组数 |
| pending_shot_count | int | 待生成镜头数 |
| shot_groups | ShotGroup[] | 镜头组 |
| pending_shots | Shot[] | 待生成镜头 |

### 1.17 ShotHierarchy（分镜层级）

来源：`GET /v3api/scripts/<sid>/episodes/<eid>/shot-hierarchy`。

| 字段 | 类型 | 说明 |
|---|---|---|
| episode_id | string(UUID) | 分集 |
| episode_version | int | 版本 |
| scenes | Scene[] | 场景列表 |

层级：`Episode -> Scenes(背景场景) -> Shot Groups(镜头组) -> Shots(镜头)`。

### 1.18 VideoWorkbench（视频工作台）

来源：`GET /v3api/shots/<sid>/episodes/<eid>/video-workbench`。

| 字段 | 类型 | 说明 |
|---|---|---|
| episode_id | string(UUID) | 分集 |
| episode_version | int | 版本 |
| scenes | Scene[] | 场景列表（含 ShotGroup 带 unit_id/unit_runtime_key/materials/audio_elements） |

### 1.19 VideoGenerationJob（视频生成任务）

来源：`GET /v3api/video-generation-jobs`。异步任务，前端轮询。

| 字段 | 类型 | 说明 |
|---|---|---|
| job_id | string(UUID) | 任务 ID |
| script_id | string(UUID) | 剧本 |
| episode_id | string(UUID) | 分集 |
| scene_no | int | 场景号 |
| unit_id | string | 生成单元（`group:<shot_group_id>`） |
| unit_runtime_key | string | 运行时 key |
| provider_key | enum | `jianyu` / `modelsai` |
| job_type | enum | `all_reference`（多参考图生视频） |
| status | enum | `starting`/`processing`/`succeeded`/`failed`/`canceled` |
| progress | int | 0-100 |
| progress_stage | string\|null | 阶段（如 `succeeded`） |
| progress_message | string\|null | 阶段消息 |
| error_message | string\|null | 错误 |
| input_json | object | 输入（见下） |

`input_json`：

| 字段 | 类型 | 说明 |
|---|---|---|
| model | string | 视频模型（如 `doubao-seedance-2-0-260128`，即豆包 Seedance 2.0） |
| reference_images | array | 参考图=镜头组引用的主体设计图 |
| reference_image_names | string[] | 参考图对应主体名 |
| reference_audios | array | 参考音频 |
| reference_audio_names | string[] | 参考音频名 |
| reference_videos | array | 参考视频 |
| reference_video_names | string[] | 参考视频名 |
| prompt | string | 镜头组 prompt |

**reference_images 元素**

| 字段 | 类型 | 说明 |
|---|---|---|
| kind | string | `storage` |
| storage_key | string | 存储路径（`/upload_images|generate_images/<mid>/<id>.png`） |
| mime_type | string | MIME |
| size_bytes | int | 字节数 |
| sha256 | string | 哈希 |

**枚举**

| 字段 | 取值 | 含义 |
|---|---|---|
| provider_key | `jianyu` | 即梦（？） |
| | `modelsai` | ModelScope（？） |
| job_type | `all_reference` | 多参考图生视频 |
| model | `doubao-seedance-2-0-260128` | 豆包 Seedance 2.0 |

### 1.20 VideoTimelineArrangement（时间线编排）

来源：`GET /v3api/video-timeline-arrangements`。

| 字段 | 类型 | 说明 |
|---|---|---|
| id | string(UUID) | 编排项 ID |
| unit_runtime_key | string | 关联镜头组运行时 key |
| job_id | string(UUID) | 关联生成 job |
| row_order | int | 轨道顺序 |
| is_selected | boolean | 是否选中 |
| trim_start_seconds | float | 裁入点（秒） |
| trim_end_seconds | float\|null | 裁出点（秒，null=到结尾） |
| created_at | string(ISO) | 创建时间 |
| updated_at | string(ISO) | 更新时间 |

时间线 = 已生成视频片段（每片段=一个镜头组）按 `row_order` 顺序排列 + `trim` 裁剪，即视频编辑器轨道。

---

## 2. API 端点参考

> 路径均相对 `https://zhihui.yangshipin.cn/v3api/`。
> 标注 [异步/轮询] 的端点表示任务型，前端轮询直至终态。

### 2.1 auth（鉴权）

| 方法 | 路径 | 查询/体参数 | 响应要点 |
|---|---|---|---|
| GET | `/auth/me` | - | 当前用户 User 对象；未登录返回 401 `{"detail":"未登录"}` |

### 2.2 credits（积分）

| 方法 | 路径 | 查询参数 | 响应要点 |
|---|---|---|---|
| GET | `/credits/accounts` | - | CreditAccount[]（按 owner 维度） |
| GET | `/credits/logs` | `account_id`、`limit` | CreditLog[]（流水，按时间倒序） |

### 2.3 scripts（剧本）

| 方法 | 路径 | 查询参数 | 响应要点 |
|---|---|---|---|
| GET | `/scripts/page` | `limit`、`offset`、`scope`(`personal`/`team`) | `{items:[Script 列表项]}` |
| GET | `/scripts/<id>` | - | Script 详情 |
| GET | `/scripts/<id>/episodes` | - | Episode[] |
| GET | `/scripts/<id>/pipeline-status` | - | 5 阶段流水线状态（见下） |
| GET | `/scripts/<id>/extract-subjects-status` | - | 主体提取与合并状态（含 `merge_job_id`）[异步/轮询] |
| GET | `/scripts/<id>/episodes/<eid>/breakdown` | - | `{episode, subjects[], shots[], shot_groups[]}` |
| GET | `/scripts/<id>/canvas-project/ensure` | - | 返回/创建 materials_id（ensure 语义：不存在则创建） |
| GET | `/scripts/<id>/asset/entries` | `limit`、`offset` | 资产条目（列表页用于取计数） |
| GET | `/scripts/<id>/asset-library/subjects` | `limit`、`offset` | `{items:[Subject]}` |
| GET | `/scripts/<id>/asset-library/parent-subjects` | - | `{items:[ParentSubject]}` |
| GET | `/scripts/news-broadcast/jobs` | - | 新闻播报 jobs 列表（特殊剧本类型，返回 `[]`） |

**pipeline-status 5 阶段**

| 序 | key | 名称 | 状态字段示例 |
|---|---|---|---|
| 1 | `split` | 剧集拆分 | `done` "2 集" |
| 2 | `extract` | 提取主体 | `done` "2/2 集"（current/total） |
| 3 | `merge` | 合并主体 | `done` "44 个子主体" |
| 4 | `enrich` | 设计线索增强 | `pending` ""（扩成摄影级 prompt） |
| 5 | `parents` | 归纳父主体 | `done` "5 个父主体" |

顶层标志：`running`、`failed`、`all_done`。

**extract-subjects-status**

| 字段 | 说明 |
|---|---|
| total_episodes / extracted_episodes | 总集数/已提取集数 |
| all_extracted | 是否全部提取完成 |
| episodes[].extract_status | `succeeded` 等 |
| merge_status | `succeeded` |
| merge_job_id | 合并 job ID |
| has_active_merge | 是否有进行中的合并 |

### 2.4 materials（画布项目）

| 方法 | 路径 | 查询/体参数 | 响应要点 |
|---|---|---|---|
| GET | `/materials/<id>` | - | Materials 画布项目 |
| GET | `/materials/<id>/canvas` | - | `{material_id, nodes[], edges[]}` |
| POST | `/materials/<mid>/canvas/delta` | delta body | 增量保存（节点移动/增删/参数变更） |
| GET | `/materials/<mid>/image/<image_id>` | - | ImageAsset 元数据 |

### 2.5 generation-jobs（图片生成任务）

| 方法 | 路径 | 查询参数 | 响应要点 |
|---|---|---|---|
| GET | `/generation-jobs/<job_id>` | - | GenerationJob；前端轮询 status/progress [异步/轮询] |

`job_type` 已观察：`generate_images`。

### 2.6 shots（分镜/镜头）

| 方法 | 路径 | 查询参数 | 响应要点 |
|---|---|---|---|
| GET | `/shots/<script_id>/cover` | - | 视频项目封面（取自首个镜头首帧） |
| GET | `/scripts/<sid>/episodes/<eid>/shot-hierarchy` | - | ShotHierarchy（场景->镜头组->镜头） |
| GET | `/shots/<sid>/episodes/<eid>/video-workbench` | - | VideoWorkbench（含 unit_id/unit_runtime_key/materials/audio_elements） |

### 2.7 video-generation（视频生成）

| 方法 | 路径 | 查询参数 | 响应要点 |
|---|---|---|---|
| GET | `/video-generation/active-provider` | - | `{provider_key, job_type, model}` 当前激活供应商+模型 |
| GET | `/video-generation-jobs` | `script_id`、`episode_id`、`limit` | `{items:[VideoGenerationJob]}`；前端轮询 [异步/轮询] |

### 2.8 video-timeline-arrangements（时间线编排）

| 方法 | 路径 | 查询参数 | 响应要点 |
|---|---|---|---|
| GET | `/video-timeline-arrangements` | `script_id`、`episode_id` | `{script_id, episode_id, items:[VideoTimelineArrangement]}` |

### 2.9 scripts/news-broadcast（新闻播报）

| 方法 | 路径 | 查询参数 | 响应要点 |
|---|---|---|---|
| GET | `/scripts/news-broadcast/jobs` | - | 新闻播报 jobs 列表（特殊剧本类型/模板，可能是基于新闻稿自动生成视频） |

---

## 3. 端到端流水线（数据流转）

1. 剧本输入 -> 剧集拆分（split）
2. 每集提取主体 extract（角色/场景/道具/声音 + AI 描述）
3. 跨集合并 merge（如 44 子主体）
4. 设计线索增强 enrich（扩成摄影级生图 prompt）
5. 归纳父主体 parents（如 5 父主体，形态）
6. 资产库：生成主体图片候选（GPT Image 2 / Nano Banana） -> 审核 -> 通过 1 张为 `image_entry`；挂载声音资产 `audio_entries`
7. 创作画布：设计/排布主体图节点
8. 分镜生成：每集 LLM 拆成 场景->镜头组->镜头（含摄影级 prompt + 引用主体 + 配音元素 + 首/尾帧连续性）
9. 视频生成：每镜头组 prompt + 引用主体设计图 -> Seedance 2.0 图生视频（异步 job，积分预扣）
10. 时间线编排：片段排序 + trim 裁剪 -> 合成最终视频
11. 视频页：按剧本/集管理，查看所有视频

### 计费对象（消耗积分的操作）

- 主体提取/合并/增强（LLM）
- 主体设计图生成（GPT Image 2 / Nano Banana）
- 分镜生成（LLM）
- 视频生成（Seedance 2.0）— 主要消耗（示例每次约 784~994 积分 + 冻结 210）

---

## 4. 复刻开发要点 / 验收清单

### 实体层
- [ ] User/Organization/Department 多租户层级建模（用户->部门->企业），支持 `account_type`、`management_scope` 权限范围。
- [ ] CreditAccount 按 owner 维度（user/org/department）挂载，双字段 `balance_points`/`frozen_points`，`status` 枚举。
- [ ] CreditLog 计费三段式（reserve/settle/release/refund/grant）完整实现，`reservation_id` 串联冻结与结算。
- [ ] Script + Episode 乐观锁（`episode_version`、`client_save_seq`、`schema_version`）。
- [ ] Subject 类型 `character/scene/background/prop/sound`（`scene`=`background`），`is_parent`/`parent_id` 父子层次，`review_status` 三态。
- [ ] ParentSubject 跨集去重聚合，`child_count` 形态数。
- [ ] AssetEntry 支持 `image_entry`（单）与 `audio_entries`（多），`pending_candidate_count` 审核队列计数，`whitelist_status`。
- [ ] Materials 与 Script 一对一；CanvasNode 基于 React Flow，支持 `imageUpload` 等多 variant，`imageOutputs` 多图 + `primaryImageIndex` 主选。
- [ ] ImageAsset 独立 `sapi` 存储域名 + 鉴权代理，`storage_key` 区分 `generate_images`/`upload_images`。
- [ ] GenerationJob 异步轮询，`input_json` 含 `resolved_prompts.final` 扩展 prompt。
- [ ] Shot/ShotGroup/Scene/ShotHierarchy 层级建模；ShotGroup 带 `unit_id`/`unit_runtime_key` 作为视频生成单元；`reference_frames` 首/尾/参考帧。
- [ ] VideoWorkbench 自动汇聚 `materials`（character/background/prop/other）与 `audio_elements`。
- [ ] VideoGenerationJob 多 provider（`jianyu`/`modelsai`），`job_type=all_reference`，`input_json` 含 `model`/`reference_images`/`reference_audios`/`reference_videos`/`prompt`。
- [ ] VideoTimelineArrangement 轨道排序 + trim 裁剪。

### 接口层
- [ ] httpOnly Cookie 鉴权，401 返回 `{"detail":"未登录"}`。
- [ ] `/scripts/page` 支持 `scope` personal/team，团队项目带 `current_user_project_role`。
- [ ] `/scripts/<id>/pipeline-status` 5 阶段状态机（split/extract/merge/enrich/parents），`running`/`failed`/`all_done`。
- [ ] `/scripts/<id>/extract-subjects-status` 异步合并 job，前端轮询。
- [ ] `/scripts/<id>/canvas-project/ensure` ensure 语义（不存在则创建 materials）。
- [ ] `/materials/<mid>/canvas/delta` 增量保存（协同友好）。
- [ ] `/materials/<mid>/image/<image_id>` 返回元数据，图片实际 URL 走 `sapi` 域名。
- [ ] `/generation-jobs/<job_id>` 与 `/video-generation-jobs` 异步轮询，`status`/`progress`/`progress_stage`/`progress_message`/`error_message`。
- [ ] `/video-generation/active-provider` 全局可切换供应商+模型。
- [ ] `/video-timeline-arrangements` 按 `row_order` 排序 + `trim_start_seconds`/`trim_end_seconds` 裁剪。
- [ ] `/shots/<script_id>/cover` 取首个镜头首帧作为封面。
- [ ] `/scripts/news-broadcast/jobs` 新闻播报特殊剧本类型。

### 验收
- [ ] 端到端跑通 1 个剧本：输入剧本文本 -> 11 步流水线 -> 最终合成视频。
- [ ] 积分三段式 reserve/settle/release 在视频生成任务上正确扣减与返还。
- [ ] 资产审核流程：候选 -> 审核 -> `image_entry`，删除候选移出队列。
- [ ] 画布节点移动后 delta 保存，刷新状态一致。
- [ ] 分镜首/尾帧连续性（上一组尾帧 -> 下一组首帧）。
- [ ] 视频生成多参考图自动按 `materials` 主体汇聚 `reference_images`。
