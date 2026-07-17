# 产品需求文档 (PRD) — 智绘 (Flow App)

> 版本: 1.0 | 日期: 2026-07-14 | 状态: 草案
>
> 域名: zhihui.yangshipin.cn | 前端路由前缀: /v3/ | 后端 API 前缀: /v3api/
>
> 本文档基于平台探索笔记忠实整理, 面向"让 AI/工程师能据此从零复刻开发整个平台"的目标。

---

## 目录

1. [产品定位与愿景](#1-产品定位与愿景)
2. [目标用户与角色](#2-目标用户与角色)
3. [核心价值主张](#3-核心价值主张)
4. [功能模块全景](#4-功能模块全景)
5. [端到端用户故事](#5-端到端用户故事)
6. [业务流程总览](#6-业务流程总览)
7. [积分计费模型](#7-积分计费模型)
8. [非功能需求](#8-非功能需求)
9. [范围边界](#9-范围边界)
10. [复刻开发要点与验收清单](#10-复刻开发要点与验收清单)

---

## 1. 产品定位与愿景

### 1.1 产品概述

| 属性 | 值 |
|---|---|
| 产品名称 | 智绘 / Flow App |
| 访问地址 | https://zhihui.yangshipin.cn/v3/ |
| 定位标语 | 为 AI 影视制作搭建可复制的创作引擎 |
| 核心理念 | 用可视化画布探索创意, 用确定性流程交付作品。把灵感与产出分离, 让每一次生成都可追溯、可复用。 |
| 前端技术 | SPA 单 bundle (Vite 构建), 路由 hash/history 混合, 路径前缀 /v3/ |
| 后端技术 | IIS/1.1.1 服务器, 疑似 .NET / ASP.NET 后端 |
| API 基址 | https://zhihui.yangshipin.cn/v3api/ |
| 负载均衡 | 腾讯云 L7 网关 (tgw_l7_route cookie), yangshipin.cn 为央视频域名 |

### 1.2 愿景解读

智绘平台要解决的核心问题是: AI 影视制作中"创意探索"与"作品交付"两种工作模式的冲突。

- **用可视化画布探索创意**: 在基于 React Flow 的节点画布上, 创作者可以自由排布、试错、调整主体设计图节点, 探索视觉风格与叙事节奏, 不受线性流程的束缚。
- **用确定性流程交付作品**: 从剧本文本到最终视频成片, 有一条明确的流水线 (拆分 → 提取 → 合并 → 增强 → 归纳 → 资产生成与审核 → 分镜 → 视频生成 → 时间线合成), 每一步有明确输入输出, 确保产出质量可控。
- **把灵感与产出分离**: 画布是探索区(允许混乱和试错), 流水线是交付区(追求确定和可复现), 两者解耦。
- **让每次生成都可追溯、可复用**: 所有 AI 生成任务(主体提取、图片生成、分镜生成、视频生成)均有 job_id 记录, 输入 prompt/参数/模型/参考资产全部持久化, 可回溯、可复跑、可复用。

### 1.3 产品边界

智绘不是通用 AI 绘画工具, 也不是通用视频剪辑器。它是一个**以剧本为核心驱动的、端到端的 AI 影视资产生产与交付流水线**, 将 LLM 文本理解、AI 图片生成、AI 视频生成三大能力编排为可复制的创作引擎。

---

## 2. 目标用户与角色

### 2.1 目标用户

| 用户群体 | 典型场景 | 使用特征 |
|---|---|---|
| 独立创作者 | 个人制作短剧/动画/宣传视频 | 个人账号, 全流程单兵作战 |
| 创作团队 | 工作室/小型团队协作制作影视内容 | 团队账号, 多人分工协作(剧本/资产/画布/合成) |
| 企业/组织 | 大型机构批量生产视频内容 | 组织+部门多租户, 额度下发与审批管理 |

### 2.2 用户模型

用户信息由 `GET /v3api/auth/me` 返回:

```json
{
  "enabled": true,
  "username": "chuanbo",
  "user_id": "<uuid>",
  "is_admin": false,
  "account_type": "personal",          // personal | team
  "org_id": "<uuid>",
  "department_id": "<uuid>",
  "can_manage_accounts": false,
  "management_scope": "none",          // none | department | org
  "organizations": [
    {"id":"<uuid>","org_name":"默认企业","org_code":"default-enterprise"}
  ]
}
```

### 2.3 角色定义

| 角色 | 标识字段 | 权限范围 |
|---|---|---|
| 普通创作者 | `is_admin=false, management_scope=none` | 管理个人项目, 消耗个人积分账户 |
| 团队成员 | `account_type=team, current_user_project_role != null` | 在团队项目中按项目角色协作 |
| 部门管理员 | `management_scope=department, can_manage_accounts=true` | 管理本部门用户积分账户(下发/查看) |
| 组织管理员 | `management_scope=org, can_manage_accounts=true` | 管理本组织所有部门及用户积分 |
| 平台管理员 | `is_admin=true` | 平台级管理 |

### 2.4 多租户模型

```
企业(Organization)
  └── 部门(Department)
        └── 用户(User)
```

- 一个用户属于一个部门, 一个部门属于一个企业。
- 每个用户有个人积分账户(`owner_type=user`), 部门/组织也可持有积分账户。
- 积分由部门下发给用户(`log_type=grant`, 授予方为 `default_department` 等)。
- `management_scope` 控制账户管理权限范围: `none`(不可管理) → `department`(本部门) → `org`(本组织全部)。

### 2.5 鉴权机制

| 属性 | 说明 |
|---|---|
| 会话方式 | httpOnly Cookie 会话 |
| 安全性 | JS 不可读取 Cookie, 防 XSS 窃取 |
| 登录 | 登录端点提交后由服务端 Set-Cookie 建立 httpOnly 会话 |
| 未登录响应 | HTTP 401, `{"detail":"未登录"}` |
| 网关层 | 腾讯云 L7 网关, `tgw_l7_route` cookie 做路由 |

---

## 3. 核心价值主张

### 3.1 价值矩阵

| 价值点 | 说明 | 对应能力 |
|---|---|---|
| 剧本驱动 | 一切从剧本文本出发, AI 自动拆解为可操作的影视元素 | 剧本流水线(split/extract/merge/enrich/parents) |
| 资产可复用 | 主体设计图、声音资产一次生成, 多集/多场景复用 | 资产库 + 主体父子聚合 |
| 创意可探索 | 可视化画布自由排布节点, 风格一致性统一审视 | React Flow 画布 |
| 交付可确定 | 分镜 → 视频生成 → 时间线合成, 流程明确, 产出可控 | 分镜层级 + 视频生成 job + 时间线编排 |
| 生成可追溯 | 每个 AI 任务有 job_id, 输入/模型/prompt/参考资产全部持久化 | generation-jobs / video-generation-jobs |
| 成本可管控 | 积分预扣/结算/释放三段式计费, 部门额度下发 | 积分账户体系 |
| 多方可协作 | 个人/团队项目, 多租户权限, 部门额度分配 | 多租户 + management_scope |

### 3.2 差异化定位

与通用 AI 绘画/视频工具相比, 智绘的差异在于:

1. **不是单点工具, 是流水线**: 从文本到成片全链路覆盖, 不需要用户在多个工具间切换。
2. **不是无结构生成, 是剧本结构化驱动**: AI 不是随机生成, 而是基于剧本提取的结构化主体(角色/场景/道具/声音)确定性地生成。
3. **不是一次性消费, 是资产复用**: 主体设计图可跨集复用, 父子聚合确保同一角色在多集中视觉一致。
4. **不是黑盒, 是可追溯引擎**: 每步生成的 prompt、模型、参考图、参数全部记录, 可回溯、可复跑。

---

## 4. 功能模块全景

### 4.1 模块总览

```
顶部导航: 智绘 logo + 导航按钮 + 用户区(用户名 | 查看积分明细 | 退出登录)
           │
    ┌──────┼──────┬──────────────┬──────────┐
    │      │      │              │          │
  剧本页  资产库  创作页          视频页     积分账户
/v3/scripts /v3/asset-library /v3/materials/*/canvas /v3/videos (弹窗)
```

### 4.2 剧本页 (Scripts)

| 属性 | 值 |
|---|---|
| 路由(列表) | /v3/scripts |
| 路由(详情) | /v3/scripts/&lt;script_id&gt;/episodes/&lt;episode_id&gt; |
| 标题 | 剧本创作 |
| 副标题 | 管理与创作您的剧本故事 |
| 列表 API | GET /v3api/scripts/page?limit=30&offset=0&scope=personal |
| 项目范围 | personal(个人) / team(团队) |

**功能清单:**

| 功能 | 描述 |
|---|---|
| 剧本列表 | 搜索框 + [新建剧本]按钮 + Tab(个人/团队项目) + 项目卡片网格(标题/创建者/时间/操作) |
| 分集管理 | 左栏: 搜索/新增一集/集数列表/双击改集号/删除 |
| 剧本编辑器 | 大文本域输入剧本文本, 保存触发主体提取流水线 |
| 主体高亮 | 编辑后主体在文本中按类型(角色/场景/道具/声音)高亮, 支持类型过滤 |
| 主体面板 | 右栏: 过滤(全部N/角色N/场景N/道具N/声音N) + 主体卡片(类型标签/名称/出现集数/删除/描述) |
| 保存机制 | dirty 状态检测, [保存]按钮, episode_version 乐观锁, client_save_seq 客户端保存序号 |
| 流水线状态 | 顶部状态条显示主体提取进度/合并状态 |
| 查看资产库 | 快捷跳转资产库 |
| 新闻播报 | 特殊剧本类型, GET /v3api/scripts/news-broadcast/jobs 获取新闻播报任务列表 |

**Script 数据模型:**

```json
{"id":"<uuid>","title":"末日丧尸·小猪","scope":"personal",
 "owner_user_id":"...","org_id":null,"created_by_user_id":"...",
 "created_at":"...","updated_at":"..."}
```

**Episode 数据模型:**

```json
{"episode_id":"<uuid>","script_id":"<uuid>","episode_no":1,"title":"第 1 集",
 "episode_version":3,"updated_at":"..."}
```

**主体类型枚举:**

| 类型 | 说明 |
|---|---|
| character | 角色 |
| scene / background | 场景 |
| prop | 道具 |
| sound | 声音/配音音色 |

**Subject(主体)模型:**

```json
{"id":"<uuid>","type":"character|scene|prop|sound","name":"保安丧尸",
 "description":"<LLM 生成的详细外观/音色说明>","is_child":true,"episode_nos":[1,2]}
```

> 注: `description` 为 LLM 生成的详细外观/音色说明, 可直接用作生图/配音 prompt。

### 4.3 资产库模块 (Asset Library)

| 属性 | 值 |
|---|---|
| 路由(列表) | /v3/asset-library |
| 路由(详情) | /v3/asset-library/&lt;script_id&gt; |
| 副标题 | 管理剧本关联的资产集合 |
| 列表数据 | 复用剧本列表卡片, 封面=按标题确定性生成的 3 段线性渐变 SVG + 高斯模糊占位图 |
| 资产计数 | GET /v3api/scripts/&lt;id&gt;/asset/entries?limit=1&offset=0 取总数 |

**功能清单:**

| 功能 | 描述 |
|---|---|
| 资产主体表 | 表格列: 图片/主体名/主体类别/描述/声音/所属集数/资产审核/审核状态/操作(删除主体) |
| 父子分组 | 行按父主体分组, 标题如"安全区(2 个形态)"; 无父子主体归入"其余子主体" |
| 内联编辑 | 双击编辑主体名/描述; 下拉改主体类别(角色/场景/道具/声音)/所属集数 |
| 声音挂载 | 声音列可挂载声音资产(audio_entries) |
| 资产审核 | 弹窗: "<主体名> 的候选资产" — 图片审核([通过图片] + 候选列表) + 声音审核([通过声音] + 候选/暂无) |
| 去设计 | 跳转创作页画布 |
| 统计 | 已审核数量 / 未审核数量 / 全部主体数量 |

**Subject(资产主体)模型 — GET /v3api/scripts/&lt;id&gt;/asset-library/subjects:**

```json
{
  "subject_id":"<uuid>","subject_name":"城市废墟居民楼",
  "subject_type":"background|character|prop|sound",
  "subject_description":null,
  "episodes":[],"episode_ids":[],"episode_nos":[],
  "parent_id":null,"parent_name":null,"is_parent":true,
  "review_status":"not_designed",           // not_designed | approved | pending
  "review_status_label":"未通过",           // 未通过 | 已通过 | 未审核
  "pending_candidate_count":0,
  "image_entry":null,                       // 已通过图片资产(单个)
  "audio_entries":[],                        // 挂载的声音资产(多个)
  "whitelist_status":null,
  "created_at":"...","updated_at":"..."
}
```

**审核状态枚举:**

| review_status | label | 含义 |
|---|---|---|
| not_designed | 未通过 | 未设计或审核未通过 |
| pending | 未审核 | 已生成候选图片, 待人工审核 |
| approved | 已通过 | 已通过1张图片为 image_entry |

**主体到资产转化流程:**

1. 剧本提取出主体(character/scene/background/prop/sound) + AI 描述
2. 主体归纳父/子(形态)层次
3. 每个主体可生成图片候选 → 审核 → 通过1张为 image_entry
4. 每个主体可挂载声音资产(音频文件) audio_entries
5. "去设计" 跳转创作页, 在画布中用这些资产生成镜头/视频

### 4.4 创作页 (Canvas)

| 属性 | 值 |
|---|---|
| 路由 | /v3/materials/&lt;materials_id&gt;/canvas |
| 技术基础 | React Flow 可视化节点画布 |
| 创建触发 | 资产库 [去设计] → GET /v3api/scripts/&lt;script_id&gt;/canvas-project/ensure → 返回/创建 materials_id → 跳转 canvas |
| 理念落地 | "用可视化画布探索创意"(画布) + "用确定性流程交付作品"(流水线) |

**Materials(画布项目)模型 — GET /v3api/materials/&lt;id&gt;:**

```json
{"id":"<uuid>","name":"末日丧尸·小猪","script_id":"<uuid>","created_at":"...","updated_at":"..."}
```

> 一个剧本 → 一个 materials 画布项目。

**Canvas 状态 — GET /v3api/materials/&lt;id&gt;/canvas:**

```json
{"material_id":"<uuid>","nodes":[...],"edges":[...]}
```

**节点(Node)模型 (type="imageUpload"):**

```json
{
  "id":"<uuid>","type":"imageUpload","position":{"x":1120,"y":509.5},
  "data":{
    "title":"保安丧尸 设计图",
    "variant":"generated",                 // generated | uploaded | text | storyboard
    "imageUrl":null,                        // 图片通过 /materials/<mid>/image/<id> 单独加载
    "prompt":"<固定模板 + 主体名/类型/描述 + 要求>",
    "model":"banana-2",                     // banana-2 | gpt-image-2
    "size":"2k","aspect":"16:9",
    "isGenerating":false,"generationJobId":null,
    "subjectId":"<uuid>","subjectName":"保安丧尸",
    "subjectType":"character","subjectDescription":"...",
    "subjectEpisodeIds":[...],"subjectEpisodeNos":[1,2],
    "subjectDesignStatus":"draft",          // draft | ...
    "promptVariationIndex":0,               // prompt 变体序号(多版本prompt)
    "imagesExpanded":false,
    "generatingStartedAt":1784015617046,
    "assetId":"<uuid>",                      // 关联图片资产
    "imageOutputs":[{"status":"succeeded","assetId":"<uuid>","storageKey":"/generate_images/<mid>/<id>.png"}],
    "primaryImageIndex":0                    // 主选图序号(可生成多张选一)
  }
}
```

**功能清单:**

| 功能 | 描述 |
|---|---|
| 节点画布 | React Flow, 支持拖动(方向键移动)/删除/Delete/Esc取消 |
| 画布工具 | Zoom In/Out, Fit View, Toggle Interactivity, Mini Map |
| 顶部工具栏节点类型 | 文本(脚本/广告词/品牌文案) / 图片(GPT Image 2 — 宣传图/海报/封面) / 选择分镜(多图分镜选择与下载) / 上传(图片/视频/音频文件) / Assets |
| 主体设计队列(右栏) | 全部/未设计/可审核/已通过计数, 集数筛选下拉, [一键设计 N]批量生图, [生图设置]模型/尺寸/比例/风格, [一键提交审核 N]批量提交 |
| 主体列表 | 按类型分组(角色/道具/背景/声音), 每行: 名称+状态+描述+集数徽章+操作 |
| 主体操作 | 图片类未设计: "设计 X"→创建绑定图片节点触发生图; 可审核: "前往 X 匹配资产"→跳资产库审核; 声音类: "上传 X 声音"→创建绑定声音节点上传音频 |
| 去审核 | 跳转资产库 |
| 增量保存 | POST /v3api/materials/&lt;mid&gt;/canvas/delta, 画布节点移动/增删/参数变更以 delta 增量提交(协同友好) |

**AI 图片生成模型:**

| 模型标识 | 对应模型 | 用途 |
|---|---|---|
| banana-2 | Google Nano Banana (Gemini 2.5 Flash Image) | 主体参考图 |
| gpt-image-2 | GPT Image 2 (OpenAI) | 主体参考图 / 宣传图/海报/封面 |

**图片资产元数据 — GET /v3api/materials/&lt;mid&gt;/image/&lt;image_id&gt;:**

```json
{"image_id":"<uuid>","storage_key":"/generate_images/<mid>/<id>.png",
 "mime_type":"image/png","size_bytes":2411092,"source":"generate_images"}
```

> 图片实际 URL: `https://sapi.yangshipin.cn/api/x/cctv/zhihui/flow-app/generate_images/<mid>/<id>.png`
> 生成图存储在独立 sapi 服务域名(对象存储 + 鉴权代理)。

**生成任务 — GET /v3api/generation-jobs/&lt;job_id&gt;:**

```json
{
  "job_id":"<uuid>","material_id":"<uuid>","script_id":null,"episode_id":null,"scene_no":null,
  "job_type":"generate_images",
  "status":"succeeded","progress":100,"progress_stage":null,"progress_message":null,
  "error_message":null,
  "input_json":"{\"prompt\":\"...\",\"resolved_prompts\":{\"final\":\"...\"}}"
}
```

> `input_json` 内含被 LLM 扩展后的超详细 prompt (resolved_prompts.final)。

**Prompt 扩展模板(enrich 阶段产物):**

简单主体描述被扩展为专业摄影级 prompt, 包含以下段落:

| 段落 | 说明 |
|---|---|
| 形制描述 | 物体的形态描述 |
| 材质钉死 | 确定性材质指定 |
| 使用痕迹 | 旧化/磨损等细节 |
| 摆放姿态 | 物体在画面中的姿态 |
| 反悬浮锁 | 确保物体不悬浮 |
| 时代锚定 | 确定时代背景 |
| 画面构图 | 构图指令 |
| 画风统一锁 | 确保多图风格一致 |
| 三点柔光箱白底布光 | 布光: 三点柔光箱白底摄影棚 |
| 反水印锁 | 禁止水印 |
| 文字渲染降级方案 | 文字处理策略 |
| 写实质感锚定 | Phase One 中画幅 + 100mm 微距 + f/8 |

### 4.5 视频页 (Videos)

| 属性 | 值 |
|---|---|
| 路由(列表) | /v3/videos |
| 路由(详情) | /v3/videos/&lt;script_id&gt;?tab=storyboard-video\|all-videos&episode=&lt;episode_id&gt; |
| 副标题 | 按剧本管理视频分镜与剪辑片段 |

**功能清单:**

| 功能 | 描述 |
|---|---|
| 视频项目列表 | 按剧本管理, 封面取自首个镜头首帧 GET /v3api/shots/&lt;script_id&gt;/cover |
| Tab 切换 | [分镜视频] / [所有视频] |
| 分镜生成 | [分镜生成]按钮, 只为当前选中集生成分镜结果(触发 LLM 拆分镜头) |
| 集选择 | 左栏: 第1集/第2集... |
| 分镜结果子Tab | [分镜结果] / [镜头组合并] |
| 分镜结果展示 | 每条分镜(镜头)缩略与描述 |

**分镜层级结构:**

```
Episode(集)
  └── Scene(背景场景)
        └── Shot Group(镜头组) — 视频生成单元
              └── Shot(镜头)
```

**Shot Hierarchy — GET /v3api/scripts/&lt;sid&gt;/episodes/&lt;eid&gt;/shot-hierarchy:**

```json
{
  "episode_id":"<uuid>","episode_version":3,
  "scenes":[
    {"scene_no":1,
     "groups":[  // 镜头组
       {"shot_group_id":"<uuid>","scene_no":1,"group_no":"G1",
        "prompt":"【镜头1:0～4s】...【镜头2:4～7s】...【角色】...【叙事目的】...【约束】...",
        "reference_frames":{"first_frame":null,"last_frame":null,"reference_frame":null},
        "shots":[
          {"shot_id":"<uuid>","scene_no":1,"shot_group_id":"<uuid>","group_no":"G1",
           "shot_no":"<混淆id串>","display_no":1,
           "content":"<镜头画面内容(自然语言)>",
           "description":"在【背景】采用【机位】视角+画面主体【角色】+关键动作【】+整体画面【】+表达情绪【】+光影效果【】+叙事目的【】"}
        ]
       }
     ],
     "ungrouped_shots":[]
    }
  ]
}
```

**Shot(镜头)字段说明:**

| 字段 | 说明 |
|---|---|
| shot_id | 镜头唯一ID |
| scene_no | 所属场景序号 |
| shot_group_id | 所属镜头组ID |
| group_no | 镜头组编号(如 G1) |
| shot_no | 混淆ID串 |
| display_no | 显示序号 |
| content | 镜头画面内容(自然语言描述) |
| description | 摄影级结构化描述(背景/机位/角色/动作/画面/情绪/光影/叙事目的) |

**Shot Group(镜头组) — 视频生成单元:**

| 字段 | 说明 |
|---|---|
| prompt | 多镜头分时段组合 prompt, 格式: 【镜头N:起~止s】场景+景别+机位+运镜+光影+动作+情绪+角色台词 + 【角色】引用主体 + 【叙事目的】+ 【约束】 |
| reference_frames | {first_frame, last_frame, reference_frame} — 首帧/尾帧/参考帧, 用于片段间连续性(尾帧→首帧衔接) |
| shots | 组内镜头列表 |

**约束模板:**

> 人物特征高度一致, 角色不脸崩、肢体不畸形, 多镜头切镜丝滑平顺, 画面质量4K稳定。语音清晰, 情感表达贴合角色性格, 台词与唇形/动作精准对位。禁止出现任何形式的字幕。

**Video Workbench — GET /v3api/shots/&lt;sid&gt;/episodes/&lt;eid&gt;/video-workbench:**

```json
{
  "episode_id":"<uuid>","episode_version":3,
  "scenes":[
    {"scene_key":"bg:养鸡场办公室-内-夜","scene_no":1,
     "background":"养鸡场办公室-内-夜","label":"场 1 · 养鸡场办公室-内-夜",
     "shot_group_count":2,"pending_shot_count":0,
     "shot_groups":[{
       "shot_group_id":"<uuid>","scene_no":1,"background":"...",
       "scene_key":"bg:...","group_no":"G1",
       "unit_id":"group:<uuid>","unit_runtime_key":"<runtime>:1:group:<uuid>",
       "prompt":"...","duration_seconds":7,"video_duration_seconds":7,
       "materials":{"character":["王贵发","韩志明"],"background":["养鸡场办公室-内-夜"],"prop":[],"other":[]},
       "audio_elements":["王贵发"],
       "shot_ids":[...],"shot_display_nos":[1,2],"shot_nos":[...],
       "description_text":"...","reference_frames":{...}
     }],
     "pending_shots":[]}
  ]
}
```

**Video Workbench 关键字段:**

| 字段 | 说明 |
|---|---|
| scene_key | `bg:<背景名>`, 场景按背景主体聚合 |
| materials | 该镜头组引用的所有主体资产(character/background/prop/other) → 自动作为视频生成参考图 |
| audio_elements | 需要配音的角色(关联声音资产) |
| duration_seconds / video_duration_seconds | 视频时长(秒) |
| unit_id | 生成单元标识 `group:<shot_group_id>` |
| unit_runtime_key | 运行时键 `<runtime>:<scene_no>:group:<group_id>` |

**视频生成任务 — GET /v3api/video-generation-jobs?script_id=&episode_id=&limit=500:**

```json
{
  "items":[{
    "job_id":"<uuid>","script_id":"<uuid>","episode_id":"<uuid>","scene_no":1,
    "unit_id":"group:<shot_group_id>",
    "unit_runtime_key":"<runtime>:<scene_no>:group:<group_id>",
    "provider_key":"jianyu|modelsai",
    "job_type":"all_reference",
    "status":"succeeded","progress":100,
    "progress_stage":"succeeded","progress_message":"success",
    "error_message":null,
    "input_json":{
      "model":"doubao-seedance-2-0-260128",
      "reference_images":[
        {"kind":"storage","storage_key":"/upload_images|generate_images/<mid>/<id>.png",
         "mime_type":"image/png","size_bytes":...,"sha256":"..."}
      ],
      "reference_image_names":["手机","王贵发","郊外荒地-外-夜"],
      "reference_audios":[],"reference_audio_names":[],
      "reference_videos":[],"reference_video_names":[],
      "prompt":"【镜头1:0～3s】..."
    }
  }]
}
```

**视频生成关键说明:**

| 属性 | 说明 |
|---|---|
| 生成单元 | 镜头组(Shot Group) |
| 输入 | shot_group.prompt + 该组 materials 对应的主体设计图(多参考图) |
| 模型 | doubao-seedance-2-0-260128 (豆包 Seedance 2.0, 字节视频模型) |
| job_type | all_reference (多参考图生视频) |
| provider | jianyu(即梦) / modelsai(ModelScope) — 多供应商可切换 |
| 参考图自动汇聚 | 系统按 prompt 中引用的主体, 自动取其已通过的设计图作为 reference_images |
| 参考图来源 | upload_images(用户上传) 或 generate_images(AI 生成) |

**视频生成 Provider 配置 — GET /v3api/video-generation/active-provider:**

```json
{"provider_key":"modelsai","job_type":"all_reference","model":"Seedance 2.0"}
```

> 全局当前激活的视频生成供应商 + 模型(可切换)。

**时间线编排 — GET /v3api/video-timeline-arrangements?script_id=&episode_id=:**

```json
{
  "script_id":"<uuid>","episode_id":"<uuid>",
  "items":[
    {"id":"<uuid>","unit_runtime_key":"<runtime>:<scene>:group:<group>","job_id":"<uuid>",
     "row_order":0,"is_selected":false,
     "trim_start_seconds":0.0,"trim_end_seconds":null,
     "created_at":"...","updated_at":"..."}
  ]
}
```

> 时间线 = 已生成视频片段(每片段=一个镜头组)按 row_order 顺序排列 + trim 裁剪 → 合成最终视频。
> 即视频编辑器的轨道: 拖拽片段顺序 + 设置入出点(trim_start/trim_end) → 合成最终视频。

### 4.6 积分账户 (Credits)

详见第 7 节 [积分计费模型](#7-积分计费模型)。

**入口:** 顶部用户区 [查看积分明细] → 弹窗"我的积分明细"。

---

## 5. 端到端用户故事

### 5.1 从剧本文本到成片的完整旅程

**故事: 创作者使用智绘将一部短剧剧本转化为成片视频**

#### 第一步: 创建剧本与分集

> 创作者进入 /v3/scripts, 点击 [新建剧本], 输入标题"末日丧尸·小猪", 选择 personal(个人)或 team(团队)范围。进入剧本详情页, 在左栏 [新增一集], 为每集输入集数和标题。在主区大文本域中粘贴/编写该剧集的完整剧本文本, 点击 [保存]。

**API 交互:**
- POST 创建剧本 → 获得 script_id
- POST 创建分集 → 获得 episode_id, episode_version=1
- PUT/PATCH 保存剧本文本 → client_save_seq 递增

#### 第二步: AI 流水线自动处理剧本

> 保存后, 系统自动触发剧本处理流水线。创作者在顶部状态条看到各阶段进度: "主体提取: 2/2 集" "合并: 已完成"。完成后, 右栏主体面板展示提取出的所有主体(角色/场景/道具/声音), 每个主体带有 AI 生成的详细描述。

**流水线 5 阶段:**

| 阶段 | 名称 | 说明 | 输出 |
|---|---|---|---|
| 1. split | 剧集拆分 | 将整段剧本文本拆分为集 | 集列表 |
| 2. extract | 提取主体 | 每集 LLM 提取主体(角色/场景/道具/声音) + AI描述 | 子主体列表(含description) |
| 3. merge | 合并主体 | 跨集合并去重 | 合并后子主体(如44个) |
| 4. enrich | 设计线索增强 | 把简单主体描述扩成专业摄影级生图prompt | resolved_prompts.final |
| 5. parents | 归纳父主体 | 同类子主体聚合为父主体(如5个父主体) | 父子层次结构 |

**API 交互:**
- GET /v3api/scripts/&lt;id&gt;/pipeline-status — 轮询流水线状态
- GET /v3api/scripts/&lt;id&gt;/extract-subjects-status — 提取与合并状态(merge_job_id)
- GET /v3api/scripts/&lt;id&gt;/episodes/&lt;eid&gt;/breakdown — 分集拆解(含subjects/shots/shot_groups)

#### 第三步: 资产库管理

> 创作者进入 /v3/asset-library/&lt;script_id&gt;, 看到主体资产表, 行按父主体分组(如"安全区(2 个形态)")。统计显示: 已审核2 / 未审核35 / 全部37。创作者双击编辑主体描述, 调整主体类别, 为声音主体挂载音频文件。

**API 交互:**
- GET /v3api/scripts/&lt;id&gt;/asset-library/subjects?limit=200&offset=0 — 主体资产列表
- GET /v3api/scripts/&lt;id&gt;/asset-library/parent-subjects — 父主体列表
- PATCH/PUT 编辑主体
- POST 挂载声音资产

#### 第四步: 画布设计主体图

> 创作者点击 [去设计], 系统通过 ensure 接口创建/获取 materials_id 并跳转画布。在 React Flow 画布上, 右栏主体设计队列显示"44/44 全部 / 未设计9 / 可审核33 / 已通过2"。创作者点击 [一键设计 9] 批量为未设计主体生成设计图(消耗积分), 等待异步生成完成。生成完成后, 画布上出现图片节点, 排布节点便于统一审视风格一致性。

**API 交互:**
- GET /v3api/scripts/&lt;script_id&gt;/canvas-project/ensure → 返回 materials_id
- GET /v3api/materials/&lt;id&gt;/canvas → 画布节点和边
- POST /v3api/materials/&lt;mid&gt;/canvas/delta → 增量保存
- POST 创建生成任务 → GET /v3api/generation-jobs/&lt;job_id&gt; 轮询
- GET /v3api/materials/&lt;mid&gt;/image/&lt;image_id&gt; → 图片元数据

**生图 prompt 自动构造:**
- 固定模板 + 主体名称 + 主体类型 + 主体描述 + "要求: 主体清晰、特征稳定、背景干扰少、适合后续多图参考生成"
- 经 enrich 扩展为摄影级 prompt(材质钉死/布光/画风统一锁/写实质感锚定等)

#### 第五步: 资产审核

> 生成完成后, 主体状态变为"可审核"。创作者在画布点击"前往匹配资产"跳回资产库审核弹窗, 查看候选图片缩略图, 选择最满意的一张点击 [通过图片], 该图片成为主体的 image_entry。审核状态变为"已通过"。

**审核流程:**
1. 生成图片 → 进入审核队列(候选, pending_candidate)
2. 人工审核 → 通过则成为该主体 image_entry(review_status=approved)
3. 删除则移出队列

#### 第六步: 分镜生成

> 创作者进入 /v3/videos/&lt;script_id&gt;, 选择集, 点击 [分镜生成]。系统调用 LLM 将该集剧本拆分为 场景 → 镜头组 → 镜头, 每个镜头组包含多镜头分时段组合 prompt + 引用主体 + 配音元素 + 首/尾帧连续性参考。

**API 交互:**
- POST 触发分镜生成
- GET /v3api/scripts/&lt;sid&gt;/episodes/&lt;eid&gt;/shot-hierarchy — 分镜层级
- GET /v3api/shots/&lt;sid&gt;/episodes/&lt;eid&gt;/video-workbench — 视频工作台

**分镜 prompt 结构:**
- 【镜头N:起~止s】场景+景别+机位+运镜+光影+动作+情绪+角色台词
- 【角色】引用主体列表
- 【叙事目的】
- 【约束】人物一致/不脸崩/不畸形/切镜丝滑/4K稳定/语音清晰/唇形动作对位/禁字幕

#### 第七步: 视频生成

> 创作者在视频工作台查看镜头组列表, 系统已自动汇总每个镜头组的参考图(主体设计图)和配音元素。创作者点击生成, 系统将 shot_group.prompt + 参考图 → Seedance 2.0 图生视频(异步 job, 积分预扣)。前端轮询进度, 完成后视频片段可预览。

**API 交互:**
- POST 创建视频生成任务
- GET /v3api/video-generation-jobs?script_id=&episode_id= — 轮询任务状态
- GET /v3api/video-generation/active-provider — 当前供应商配置

**视频生成输入:**
- model: doubao-seedance-2-0-260128
- reference_images: 镜头组引用的主体设计图(自动汇聚)
- reference_image_names: 对应主体名
- prompt: 镜头组 prompt

#### 第八步: 时间线编排与合成

> 创作者在时间线视图拖拽视频片段调整顺序(row_order), 设置入出点(trim_start/trim_end), 最终合成完整集视频。所有视频片段按场景→镜头组顺序排列, 尾帧→首帧衔接确保连续性。

**API 交互:**
- GET /v3api/video-timeline-arrangements?script_id=&episode_id= — 时间线
- PATCH/PUT 更新排列顺序和裁剪点
- POST 合成最终视频

### 5.2 团队协作用户故事

> 团队账号下, 多人可同时在同一个团队项目中工作。剧本列表中团队项目带 `current_user_project_role` 字段标识当前用户角色。不同成员可分工: A 负责剧本编辑, B 负责资产审核, C 负责画布设计, D 负责视频合成。画布增量保存(delta)支持协同友好。

### 5.3 管理员额度管理故事

> 部门管理员通过 [查看积分明细] 查看部门积分流水, 将部门额度下发给团队成员(log_type=grant)。管理员可为下属账户操作积分, actor(操作者) ≠ target(被操作账户)。management_scope=department 限制管理范围为本部门。

---

## 6. 业务流程总览

### 6.1 端到端流程图

```
剧本文本输入
    │
    ▼
┌─────────────────────────────────────────────────┐
│  剧本流水线 (剧本页)                              │
│                                                   │
│  1. split    剧集拆分       → 集列表              │
│  2. extract  提取主体       → 子主体(character/   │
│                               scene/prop/sound    │
│                               + AI description)   │
│  3. merge    合并主体       → 去重后子主体(如44个) │
│  4. enrich   设计线索增强   → 摄影级生图 prompt    │
│  5. parents  归纳父主体     → 父子层次(如5父主体)  │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│  资产库                                          │
│                                                   │
│  6. 生成图片候选  (GPT Image 2 / Nano Banana)     │
│     → 异步 generation job (积分预扣)              │
│  7. 人工审核      候选 → 通过1张为 image_entry    │
│     → review_status: pending → approved           │
│  8. 挂载声音资产  audio_entries                    │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│  创作画布 (创作页)                                │
│                                                   │
│  9. 主体设计图节点排布 (React Flow 画布)          │
│     → 增量保存 delta                              │
│     → [一键设计] 批量生图                         │
│     → 统一审视风格一致性                          │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│  分镜生成 (视频页)                                │
│                                                   │
│  10. LLM 拆分镜头  Episode → Scene → ShotGroup   │
│      → Shot                                        │
│      → 每镜头组: 多镜头分时段 prompt + 引用主体   │
│      → reference_frames 首/尾帧连续性              │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│  视频生成 (视频页)                                │
│                                                   │
│  11. 每镜头组 prompt + 主体设计图(参考图)        │
│      → Seedance 2.0 图生视频                      │
│      → 异步 video-generation job (积分预扣)        │
│      → 多 provider: jianyu / modelsai             │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│  时间线合成 (视频页)                              │
│                                                   │
│  12. 视频片段按 row_order 排列                    │
│      → trim 裁剪(入出点)                          │
│      → 尾帧→首帧衔接                              │
│      → 合成最终视频                                │
└─────────────────────────────────────────────────┘
    │
    ▼
  最终成片
```

### 6.2 流程阶段与 API 映射

| 阶段 | 操作 | 关键 API | 异步/同步 |
|---|---|---|---|
| 剧本创建 | 新建剧本/分集/编辑文本 | POST /v3api/scripts, POST episodes, PUT | 同步 |
| 剧本流水线 | split/extract/merge/enrich/parents | GET pipeline-status, GET extract-subjects-status | 异步(轮询) |
| 主体查看 | 分集拆解 | GET episodes/&lt;eid&gt;/breakdown | 同步 |
| 资产管理 | 主体列表/父主体/编辑/挂载声音 | GET asset-library/subjects, GET parent-subjects, PATCH | 同步 |
| 画布项目 | ensure 创建/获取 | GET canvas-project/ensure | 同步 |
| 画布操作 | 加载/增量保存 | GET canvas, POST canvas/delta | 同步 |
| 图片生成 | 创建任务/轮询/取图 | POST generation-jobs, GET generation-jobs/&lt;id&gt;, GET image/&lt;id&gt; | 异步(轮询) |
| 资产审核 | 候选/通过/删除 | GET subjects, PATCH review_status | 同步 |
| 分镜生成 | 触发/查看层级 | POST, GET shot-hierarchy, GET video-workbench | 异步(轮询) |
| 视频生成 | 创建/轮询 | POST video-generation-jobs, GET video-generation-jobs | 异步(轮询) |
| 时间线 | 排列/裁剪/合成 | GET video-timeline-arrangements, PATCH, POST | 同步+异步合成 |

### 6.3 数据流转关系

```
Script ──1:1──> Materials (画布项目)
  │                  │
  │ 1:N              │ 1:N
  ▼                  ▼
Episode          Canvas Node (主体设计图)
  │                  │
  │ 1:N              │ 关联
  ▼                  ▼
Scene ←──引用── Subject (主体)
  │                  │
  │ 1:N              │ 0..1 image_entry
  ▼                  │ 0..N audio_entries
Shot Group ──────引用─┘ (materials)
  │
  │ 1:1 (生成单元)
  ▼
Video Generation Job
  │
  │ 1:1
  ▼
Timeline Arrangement (row_order + trim)
  │
  ▼
最终成片
```

---

## 7. 积分计费模型

### 7.1 积分账户模型

**GET /v3api/credits/accounts:**

```json
{
  "id":"<account_id>","owner_type":"user","owner_id":"<user_id>",
  "org_id":"<uuid>","department_id":"<uuid>",
  "balance_points":130015,           // 可用余额
  "frozen_points":28980,            // 冻结(预扣/进行中任务占用)
  "status":"active",                 // active | disabled
  "disabled_reason":null,"disabled_at":null,
  "created_at":"...","updated_at":"..."
}
```

| 字段 | 说明 |
|---|---|
| owner_type | 账户归属类型: user / org / department |
| owner_id | 归属实体ID |
| balance_points | 可用余额 |
| frozen_points | 冻结积分(进行中任务预扣) |
| 可用积分 | balance_points - frozen_points (本例 130015-28980=101035) |
| status | active(正常) / disabled(禁用) |

> 顶部显示"可用积分" = balance_points - frozen_points。

### 7.2 计费三段式

异步任务(图片生成/视频生成等)采用预扣-结算-释放三段式计费:

```
任务启动                    任务完成                   任务失败/取消
   │                          │                          │
   ▼                          ▼                          ▼
 reserve(冻结)             settle(结算)              release/refund(释放)
   │                          │                          │
   ├─ frozen_points += N     ├─ balance_points -= 实际   ├─ frozen_points -= N
   ├─ 可用积分 -= N          ├─ frozen_points -= N      ├─ 可用积分 += N
   └─ 余额不变               └─ 余额减少                 └─ 余额不变
```

| 阶段 | log_type | points_delta | frozen_delta | 说明 |
|---|---|---|---|---|
| 预扣 | reserve | 0 | +N(正) | 任务启动时按预估预扣, frozen 增加 |
| 结算 | settle | -实际(负) | -N(负) | 任务完成按实际消耗结算, 余额扣减, 冻结释放 |
| 释放 | release/refund | 0 | -N(负) | 任务失败/取消, 冻结返还到可用 |

### 7.3 积分流水模型

**GET /v3api/credits/logs?account_id=&lt;account_id&gt;&limit=N:**

```json
{
  "id":"<uuid>","account_id":"<account_id>",
  "actor_user_id":"<uuid>","actor_username":"chuanbo",
  "target_user_id":"<uuid>","target_username":"chuanbo",
  "reservation_id":"<uuid>",
  "source_type":"generation_job",
  "source_id":"<job_id>",
  "log_type":"settle",
  "points_delta":-784,
  "frozen_delta":-210,
  "balance_after":130015,
  "frozen_after":28980,
  "note":null,
  "created_at":"..."
}
```

| 字段 | 说明 |
|---|---|
| actor_user_id / actor_username | 操作者 |
| target_user_id / target_username | 被操作账户归属 |
| reservation_id | 预扣预约ID(冻结/结算关联) |
| source_type | 来源类型: generation_job / manual / admin_grant 等 |
| source_id | 关联任务ID |
| log_type | settle(结算) / reserve(冻结) / release(释放) / grant(下发) / refund(返还) |
| points_delta | 余额变动(负=消耗) |
| frozen_delta | 冻结变动 |
| balance_after | 操作后余额 |
| frozen_after | 操作后冻结 |

> 支持组织/部门代付: actor ≠ target (管理员为下属账户操作)。

### 7.4 积分明细 UI

**"我的积分明细"弹窗:**

| 区域 | 说明 |
|---|---|
| 标题 | 我的积分明细 |
| 当前可用积分 | balance_points - frozen_points |
| Tab: 已获取 | grant 类型流水(正向, +N) |
| Tab: 已消耗 | consume 类型流水(负向, -N) |
| Tab: 已返还 | refund 类型流水(正向, +N) |
| 表格列 | 时间 / 来自谁(授予方, 如 default_department) / 备注 / 积分值(+/-N) |

> 积分由 department(部门)下发给用户 → 部门/组织额度分配模型。

### 7.5 计费对象(消耗积分的操作)

| 操作 | 消耗模型 | 典型积分 |
|---|---|---|
| 主体提取/合并/增强 (LLM) | 按集/按主体计费 | 待定 |
| 主体设计图生成 (GPT Image 2 / Nano Banana) | 按图计费 | 待定 |
| 分镜生成 (LLM) | 按集计费 | 待定 |
| 视频生成 (Seedance 2.0) | 按镜头组/时长计费 | ~784~994 积分 + 冻结 210 |

### 7.6 部门额度下发流程

```
组织/部门积分账户
    │
    │ grant(下发)
    ▼
用户积分账户
    │
    │ reserve → settle/release
    ▼
AI 生成任务消耗
```

- 部门管理员(`management_scope=department`)将部门额度下发给团队成员
- 下发记录: log_type=grant, actor=部门管理员, target=被下发用户, points_delta=正
- 授予方显示为部门名(如 default_department)

---

## 8. 非功能需求

### 8.1 异步任务与进度轮询

| 需求 | 说明 |
|---|---|
| 异步任务模型 | 所有 AI 生成操作(主体提取/图片生成/分镜生成/视频生成)均为异步 job |
| 任务 ID | 每个 job 有唯一 job_id |
| 进度跟踪 | status(starting/processing/succeeded/failed/canceled) + progress(0-100) + progress_stage + progress_message |
| 前端轮询 | 前端定期 GET 任务状态接口, 直到 terminal 状态(succeeded/failed/canceled) |
| 任务类型 | generate_images(图片), video-generation(视频), extract-subjects(主体提取), merge(合并), storyboard(分镜) |
| 错误处理 | error_message 记录失败原因 |
| 指标记录 | terminal 状态包含 metrics.predict_time(CPU/GPU 耗时) |

**任务状态枚举:**

| 状态 | 说明 |
|---|---|
| starting | 任务启动中(如等待 worker) |
| processing | 任务执行中 |
| succeeded | 成功完成 |
| failed | 执行失败 |
| canceled | 被取消 |

### 8.2 对象存储

| 需求 | 说明 |
|---|---|
| 存储服务 | 独立 sapi 服务域名: https://sapi.yangshipin.cn/api/x/cctv/zhihui/flow-app/ |
| 存储路径 | /generate_images/&lt;mid&gt;/&lt;id&gt;.png (AI 生成图), /upload_images/&lt;mid&gt;/&lt;id&gt;.png (用户上传图) |
| 鉴权代理 | 图片通过鉴权代理访问, 非 CDN 公开链接 |
| 文件元数据 | image_id, storage_key, mime_type, size_bytes, source, sha256(视频参考图) |
| 资源类型 | 图片(PNG)、音频(声音资产)、视频(生成片段) |

### 8.3 会话鉴权

| 需求 | 说明 |
|---|---|
| 会话方式 | httpOnly Cookie 会话(JS 不可见, 防 XSS) |
| 登录流程 | 登录端点提交凭证 → 服务端 Set-Cookie 建立 httpOnly 会话 |
| 未登录响应 | HTTP 401, `{"detail":"未登录"}` |
| 网关路由 | 腾讯云 L7 网关, tgw_l7_route cookie |
| API 鉴权 | 所有 /v3api/ 请求依赖 httpOnly Cookie, 无需手动附加 token |

### 8.4 并发与乐观锁

| 需求 | 说明 |
|---|---|
| episode_version | 分集乐观锁版本号, 保存时版本校验 |
| client_save_seq | 客户端保存序号, 乐观并发控制 |
| schema_version | 数据结构版本号 |
| 画布增量保存 | delta 增量提交, 协同友好 |

### 8.5 多供应商支持

| 需求 | 说明 |
|---|---|
| 视频生成 | 多 provider: jianyu(即梦) / modelsai(ModelScope), 可切换 |
| 图片生成 | 多 model: banana-2(Nano Banana) / gpt-image-2(GPT Image 2) |
| 全局配置 | GET /v3api/video-generation/active-provider 获取当前激活供应商 |

### 8.6 封面生成

| 需求 | 说明 |
|---|---|
| 资产库列表 | 封面=按标题确定性生成的 3 段线性渐变 SVG + 高斯模糊占位图(data:image/svg+xml) |
| 视频项目 | 封面取自首个镜头首帧 GET /v3api/shots/&lt;script_id&gt;/cover |

---

## 9. 范围边界

### 9.1 本期做(在范围内)

| 范围 | 说明 |
|---|---|
| 剧本管理与编辑 | 创建/编辑/分集/剧本文本输入/主体高亮 |
| 剧本 AI 流水线 | split/extract/merge/enrich/parents 五阶段 |
| 资产库管理 | 主体列表/父子分组/内联编辑/声音挂载 |
| 资产审核 | 图片候选审核/通过/删除 |
| 创作画布 | React Flow 节点画布/主体设计队列/一键设计/批量审核 |
| 图片生成 | GPT Image 2 / Nano Banana, 异步 job |
| 分镜生成 | LLM 拆分 Episode → Scene → ShotGroup → Shot |
| 视频生成 | Seedance 2.0 图生视频, 多 provider |
| 时间线编排 | 片段排列 + trim 裁剪 + 合成 |
| 积分体系 | 余额+冻结, reserve/settle/release 三段式, 部门下发 |
| 多租户 | org-department-user, 个人/团队账号, management_scope |
| 会话鉴权 | httpOnly Cookie |

### 9.2 本期不做(不在范围内)

| 范围 | 说明 |
|---|---|
| 新闻播报功能 | 接口存在(GET /v3api/scripts/news-broadcast/jobs)但待探索, 本期不实现 |
| 画布分镜/线索面板 | 右栏某面板出现 Internal Server Error(500), 待修复后再纳入 |
| 实时协同编辑 | 画布增量保存支持协同友好, 但本期不做实时多人冲突解决 |
| 移动端适配 | 本期为桌面 Web SPA |
| 公开 API 开放平台 | 本期仅内部 /v3api/ 接口 |
| 用户注册/自服务 | 本期不做用户自助注册, 账户由管理员创建 |
| 自定义模型接入 | 本期仅支持已接入的模型(banana-2/gpt-image-2/Seedance 2.0) |
| 视频配音/TTS | 声音资产为用户上传, 本期不集成 TTS 自动生成 |
| 视频特效/转场 | 时间线仅支持排列+裁剪, 不做特效/转场 |
| 字幕生成 | 约束中明确"禁止出现任何形式的字幕" |
| 多语言支持 | 本期仅简体中文 |

---

## 10. 复刻开发要点与验收清单

### 10.1 复刻开发要点

#### 10.1.1 技术栈选型

| 层 | 推荐技术 | 理由 |
|---|---|---|
| 前端框架 | React + Vite (SPA) | 原平台技术栈, 画布依赖 React Flow |
| 画布 | React Flow | 节点画布核心, 支持自定义节点/拖拽/Mini Map |
| 后端 | .NET / ASP.NET (IIS 部署) | 原平台技术栈; 也可选 Python(FastAPI)/Go |
| 数据库 | PostgreSQL | 多租户/乐观锁/JSON 字段(input_json)支持 |
| 对象存储 | S3 兼容 + 鉴权代理 | 图片/音频/视频文件存储 |
| 异步任务 | 消息队列 + Worker | AI 生成任务异步执行, 进度追踪 |
| AI 服务 | OpenAI API + Google Gemini + 字节豆包 | 多模型多供应商 |

#### 10.1.2 核心数据模型

```
Organization 1──N Department 1──N User
User 1──1 CreditAccount(owner_type=user)
Department 1──1 CreditAccount(owner_type=department)

User 1──N Script(scope=personal|team)
Script 1──1 Materials(画布项目)
Script 1──N Episode
Episode 1──N Subject(子主体)
Subject N──1 ParentSubject(父主体)
Subject 1──0..1 AssetEntry(image_entry, 已通过图片)
Subject 1──0..N AssetEntry(audio_entries, 声音资产)

Materials 1──1 Canvas(nodes + edges)
Canvas Node ──关联── Subject(主体设计图)

Episode 1──N Scene
Scene 1──N ShotGroup
ShotGroup 1──N Shot
ShotGroup 1──1 VideoGenerationJob(生成单元)
VideoGenerationJob 1──0..1 TimelineArrangement
```

#### 10.1.3 关键 API 端点清单

| 端点 | 方法 | 用途 |
|---|---|---|
| /v3api/auth/me | GET | 当前用户信息 |
| /v3api/credits/accounts | GET | 积分账户列表 |
| /v3api/credits/logs | GET | 积分流水 |
| /v3api/scripts/page | GET | 剧本列表(分页/scope) |
| /v3api/scripts/&lt;id&gt; | GET | 剧本详情 |
| /v3api/scripts/&lt;id&gt;/episodes | GET | 分集列表 |
| /v3api/scripts/&lt;id&gt;/pipeline-status | GET | 流水线状态 |
| /v3api/scripts/&lt;id&gt;/extract-subjects-status | GET | 提取与合并状态 |
| /v3api/scripts/&lt;id&gt;/episodes/&lt;eid&gt;/breakdown | GET | 分集拆解 |
| /v3api/scripts/&lt;id&gt;/asset-library/subjects | GET | 资产主体列表 |
| /v3api/scripts/&lt;id&gt;/asset-library/parent-subjects | GET | 父主体列表 |
| /v3api/scripts/&lt;id&gt;/asset/entries | GET | 资产条目 |
| /v3api/scripts/&lt;id&gt;/canvas-project/ensure | GET | 确保画布项目存在 |
| /v3api/scripts/&lt;id&gt;/episodes/&lt;eid&gt;/shot-hierarchy | GET | 分镜层级 |
| /v3api/shots/&lt;sid&gt;/episodes/&lt;eid&gt;/video-workbench | GET | 视频工作台 |
| /v3api/shots/&lt;sid&gt;/cover | GET | 视频项目封面 |
| /v3api/materials/&lt;id&gt; | GET | 画布项目详情 |
| /v3api/materials/&lt;id&gt;/canvas | GET | 画布状态 |
| /v3api/materials/&lt;id&gt;/canvas/delta | POST | 增量保存 |
| /v3api/materials/&lt;mid&gt;/image/&lt;id&gt; | GET | 图片元数据 |
| /v3api/generation-jobs/&lt;id&gt; | GET | 图片生成任务状态 |
| /v3api/video-generation-jobs | GET | 视频生成任务列表 |
| /v3api/video-generation/active-provider | GET | 当前视频供应商 |
| /v3api/video-timeline-arrangements | GET | 时间线编排 |
| /v3api/scripts/news-broadcast/jobs | GET | 新闻播报任务(待探索) |

#### 10.1.4 核心流程实现要点

1. **剧本流水线**: 实现为异步 job 链(split → extract → merge → enrich → parents), 每阶段有独立 job, 前一阶段完成后触发下一阶段。前端轮询 pipeline-status 获取全阶段状态。

2. **主体提取**: LLM 对每集剧本文本提取 character/scene/prop/sound 四类主体, 生成 description(详细外观/音色说明, 可直接用作生图/配音 prompt)。跨集 merge 去重。

3. **Prompt 扩展(enrich)**: 将简单主体描述扩展为专业摄影级 prompt, 包含形制/材质/痕迹/姿态/构图/画风锁/布光/反水印/写实质感锚定等段落。这是生图质量的关键。

4. **画布节点系统**: React Flow 自定义节点(imageUpload/text/upload 等), 节点 data 包含 prompt/model/size/aspect/subject 关联/generationJobId/imageOutputs。增量保存 delta。

5. **资产审核**: 生成图片进入 pending 候选队列, 人工审核通过1张为 image_entry(review_status=approved)。未通过/删除的移出队列。

6. **分镜层级**: LLM 将集拆为 Scene(按背景) → ShotGroup(镜头组, 视频生成单元) → Shot。每组 prompt 为多镜头分时段组合格式。reference_frames 确保片段连续性。

7. **视频生成**: shot_group.prompt + 该组 materials 对应的主体已通过设计图(自动汇聚为 reference_images) → Seedance 2.0 all_reference 模式图生视频。

8. **时间线合成**: 视频片段按 row_order 排列 + trim_start/trim_end 裁剪 → 合成最终视频。

9. **积分三段式**: reserve(预扣冻结) → settle(结算扣余额) / release(失败释放冻结)。支持部门下发(grant)和管理员代付(actor ≠ target)。

#### 10.1.5 安全要点

- httpOnly Cookie 会话, 防 XSS
- 所有 /v3api/ 请求依赖 Cookie 鉴权, 无需手动 token
- 对象存储通过鉴权代理访问, 非公开 URL
- management_scope 限制账户管理权限范围
- 乐观锁(episode_version/client_save_seq)防止并发覆盖

### 10.2 验收清单

#### 剧本模块

- [ ] 可创建个人/团队剧本, 设置标题和 scope
- [ ] 可新增/删除/重命名分集, 双击改集号
- [ ] 可编辑剧本文本, 保存触发流水线
- [ ] 主体在文本中按类型高亮, 支持类型过滤
- [ ] 右栏主体面板展示全部/角色/场景/道具/声音
- [ ] 主体卡片显示类型/名称/出现集数/描述
- [ ] 顶部状态条显示提取进度和合并状态
- [ ] 保存使用 episode_version 乐观锁

#### 资产库模块

- [ ] 列表页每个剧本一个卡片, 封面为确定性渐变 SVG
- [ ] 详情页主体资产表按父主体分组
- [ ] 统计显示已审核/未审核/全部数量
- [ ] 可双击编辑主体名/描述/类别/所属集数
- [ ] 声音列可挂载声音资产
- [ ] 资产审核弹窗: 图片候选列表 + [通过图片] + [从审核队列删除]
- [ ] 声音审核: [通过声音] + 候选列表
- [ ] 审核状态: not_designed → pending → approved

#### 创作画布模块

- [ ] 从资产库 [去设计] 跳转, ensure 创建/获取 materials_id
- [ ] React Flow 画布: 拖动/删除/Zoom/Fit View/Mini Map
- [ ] 顶部工具栏: 文本/图片/分镜/上传/Assets 节点类型
- [ ] 右栏主体设计队列: 全部/未设计/可审核/已通过计数
- [ ] 集数筛选下拉
- [ ] [一键设计 N] 批量生图
- [ ] [生图设置] 模型/尺寸/比例/风格
- [ ] [一键提交审核 N] 批量提交
- [ ] 主体按类型分组(角色/道具/背景/声音)
- [ ] 图片类: "设计 X" / "前往 X 匹配资产"
- [ ] 声音类: "上传 X 声音"
- [ ] 增量保存 delta
- [ ] 图片生成异步 job, 前端轮询进度

#### 视频模块

- [ ] 列表页按剧本管理, 封面取自首镜头首帧
- [ ] Tab: [分镜视频] / [所有视频]
- [ ] [分镜生成] 按钮触发 LLM 拆分镜头
- [ ] 分镜层级: Episode → Scene → ShotGroup → Shot
- [ ] 视频工作台: scene_key/materials/audio_elements/duration
- [ ] 视频生成: 镜头组 prompt + 参考图 → Seedance 2.0
- [ ] 参考图自动汇聚(主体已通过设计图)
- [ ] 多 provider 可切换(jianyu/modelsai)
- [ ] 时间线: 片段排列 + trim 裁剪
- [ ] 异步视频生成 job, 前端轮询

#### 积分模块

- [ ] 顶部显示可用积分(balance - frozen)
- [ ] [查看积分明细]弹窗: 已获取/已消耗/已返还 Tab
- [ ] 积分流水: actor/target/reservation_id/source/log_type/delta
- [ ] 三段式计费: reserve/settle/release
- [ ] 部门下发: grant, 授予方为部门名
- [ ] 管理员代付: actor ≠ target

#### 多租户与权限

- [ ] 用户模型: user_id/org_id/department_id/account_type
- [ ] management_scope: none/department/org
- [ ] 个人/团队剧本 scope
- [ ] 团队项目 current_user_project_role
- [ ] httpOnly Cookie 鉴权, 401 未登录

#### 非功能

- [ ] 异步任务: job_id/status/progress/progress_stage/error_message
- [ ] 对象存储: 独立域名 + 鉴权代理
- [ ] 乐观锁: episode_version/client_save_seq
- [ ] 画布增量保存: delta
- [ ] 多模型: banana-2/gpt-image-2(图片), Seedance 2.0(视频)
- [ ] 多供应商: jianyu/modelsai(视频)
