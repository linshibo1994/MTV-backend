# AI 能力与 Prompt 工程文档

> 平台: 智绘 (Flow App) | 域名: zhihui.yangshipin.cn | 后端 API 前缀: /v3api/ | 前端路由前缀: /v3/
> 目标读者: AI 工程师、后端工程师、全栈工程师, 需据此从零复刻整个平台的 AI 能力链路与 Prompt 工程体系。

---

## 目录

1. [AI 能力总览与流水线阶段映射](#1-ai-能力总览与流水线阶段映射)
2. [LLM 能力详解](#2-llm-能力详解)
3. [文生图能力详解](#3-文生图能力详解)
4. [视频生成能力详解](#4-视频生成能力详解)
5. [主体设计图 Prompt 模板](#5-主体设计图-prompt-模板)
6. [设计线索增强扩展 Prompt 结构](#6-设计线索增强扩展-prompt-结构)
7. [分镜 Shot Prompt 结构](#7-分镜-shot-prompt-结构)
8. [视频生成约束锁](#8-视频生成约束锁)
9. [多参考图机制](#9-多参考图机制)
10. [首尾帧连续性机制](#10-首尾帧连续性机制)
11. [Provider 与模型切换机制](#11-provider-与模型切换机制)
12. [通用 LLM 提示词骨架与输入输出契约建议](#12-通用-llm-提示词骨架与输入输出契约建议)
13. [复刻开发要点与验收清单](#13-复刻开发要点与验收清单)

---

## 1. AI 能力总览与流水线阶段映射

### 1.1 端到端流水线概览

智绘平台的核心是一条从剧本文本到最终视频的端到端 AI 流水线, 共 11 个步骤, 涉及 3 大类 AI 能力 (LLM / 文生图 / 图生视频):

| 步骤 | 流水线阶段 | API 端点 (GET 状态查询) | 涉及的 AI 能力 | 输入 | 产出 |
|------|-----------|------------------------|---------------|------|------|
| 1 | split (剧集拆分) | `/v3api/scripts/<id>/pipeline-status` | LLM - 剧本拆分 | 整段剧本文本 | 多集(episode)文本 |
| 2 | extract (提取主体) | `/v3api/scripts/<id>/extract-subjects-status` | LLM - 主体提取 | 单集剧本文本 | 主体列表(character/scene/prop/sound + AI描述) |
| 3 | merge (合并主体) | 同上 (merge_job_id) | LLM - 跨集合并 | 多集主体列表 | 去重后的子主体集合 (如 44 个) |
| 4 | enrich (设计线索增强) | `/v3api/scripts/<id>/pipeline-status` | LLM - 设计线索增强 | 主体简单描述 | 摄影级扩展 prompt (resolved_prompts.final) |
| 5 | parents (归纳父主体) | 同步骤1 | LLM - 父主体归纳 | 子主体集合 | 父主体聚合 (如 5 个, 含形态数) |
| 6 | 主体设计图生成 | `/v3api/generation-jobs/<job_id>` | 文生图 (Nano Banana / GPT Image 2) | 扩展 prompt + 主体元数据 | 主体参考图 PNG |
| 7 | 创作画布排布 | `/v3api/materials/<id>/canvas` | (人工/无 AI) | 设计图节点 | 画布节点布局 |
| 8 | 分镜生成 | `/v3api/scripts/<sid>/episodes/<eid>/shot-hierarchy` | LLM - 分镜生成 | 单集剧本 + 主体列表 | 场景 -> 镜头组 -> 镜头 (含摄影级 prompt) |
| 9 | 视频生成 | `/v3api/video-generation-jobs` | 图生视频 (Seedance 2.0) | shot_group prompt + 多参考图 | 视频片段 (MP4) |
| 10 | 时间线编排 | `/v3api/video-timeline-arrangements` | (人工/无 AI) | 视频片段列表 | 最终视频序列 + trim |
| 11 | 视频管理 | `/v3/videos/<script_id>` | (人工/无 AI) | 最终视频 | 按剧本/集管理 |

### 1.2 AI 能力清单

| 能力编号 | 能力名称 | 能力类型 | 对应阶段 | 模型/Provider | 关键产出 |
|---------|---------|---------|---------|-------------|---------|
| LLM-1 | 剧本主体提取 | LLM | extract | (平台后端 LLM, 未暴露模型名) | 主体列表 + AI 描述 |
| LLM-2 | 跨集主体合并 | LLM | merge | (平台后端 LLM) | 去重子主体集合 |
| LLM-3 | 设计线索增强 | LLM | enrich | (平台后端 LLM) | resolved_prompts.final |
| LLM-4 | 父主体归纳 | LLM | parents | (平台后端 LLM) | 父主体 + child_count |
| LLM-5 | 分镜生成 | LLM | 分镜生成 (步骤8) | (平台后端 LLM) | 场景/镜头组/镜头 + 摄影 prompt |
| IMG-1 | 主体设计图生成 | 文生图 | 主体设计 (步骤6) | Nano Banana (banana-2) | 2K 16:9 主体参考图 PNG |
| IMG-2 | 宣传图/海报生成 | 文生图 | 画布自由创作 | GPT Image 2 (gpt-image-2) | 宣传图/海报/封面 |
| VID-1 | 多参考图图生视频 | 图生视频 | 视频生成 (步骤9) | Seedance 2.0 (doubao-seedance-2-0-260128) | 视频片段 MP4 |

### 1.3 流水线状态查询接口

```
GET /v3api/scripts/<script_id>/pipeline-status
```

返回 5 阶段状态数组, 每阶段含 status (done/pending/running/failed):

```json
[
  {"stage":"split",   "label":"剧集拆分",   "status":"done",    "detail":"2 集"},
  {"stage":"extract", "label":"提取主体",   "status":"done",    "detail":"2/2 集", "current":2, "total":2},
  {"stage":"merge",   "label":"合并主体",   "status":"done",    "detail":"44 个子主体"},
  {"stage":"enrich",  "label":"设计线索增强","status":"pending", "detail":""},
  {"stage":"parents", "label":"归纳父主体",  "status":"done",    "detail":"5 个父主体"}
]
```

顶层字段: `running: false`, `failed: false`, `all_done: false`。

提取/合并的异步任务状态另查:

```
GET /v3api/scripts/<script_id>/extract-subjects-status
```

```json
{
  "total_episodes": 2,
  "extracted_episodes": 2,
  "all_extracted": true,
  "episodes": [
    {"episode_id":"<uuid>","episode_no":1,"title":"第 1 集","extract_status":"succeeded","extracted":true}
  ],
  "merge_status": "succeeded",
  "merge_job_id": "<uuid>",
  "has_active_merge": false
}
```

---

## 2. LLM 能力详解

### 2.1 LLM-1: 剧本主体提取 (extract)

| 属性 | 值 |
|------|---|
| 触发时机 | 剧本保存后自动触发, 或手动重新提取 |
| 输入 | 单集剧本文本 (episode.script_text) |
| 输出 | 主体数组: `[{id, type, name, description, is_child, episode_nos}]` |
| 主体类型枚举 | character (角色) / scene (场景, 也称 background) / prop (道具) / sound (声音/配音音色) |
| description 内容 | LLM 生成的详细外观/音色说明, 可直接用作生图/配音 prompt 的基础 |
| 异步模式 | 是, 前端轮询 extract-subjects-status |

**主体 (Subject) 模型:**

```json
{
  "id": "<uuid>",
  "type": "character|scene|prop|sound",
  "name": "保安丧尸",
  "description": "男性丧尸，身穿破烂的保安服。面部狰狞，嘴角挂着不明液体，体态扭曲踉跄。",
  "is_child": true,
  "episode_nos": [1, 2]
}
```

**前端高亮**: 主体按类型在剧本原文中高亮显示, 4 种类型可过滤 (角色/场景/道具/声音)。

### 2.2 LLM-2: 跨集主体合并 (merge)

| 属性 | 值 |
|------|---|
| 触发时机 | 所有集提取完成后自动触发 |
| 输入 | 多集的主体列表 |
| 输出 | 去重后的子主体集合 (如 44 个), 跨集出现的同一主体合并并累加 episode_nos |
| 异步模式 | 是, merge_job_id 轮询 |
| 合并逻辑 | 同名/同类主体跨集去重, episode_nos 取并集 |

### 2.3 LLM-3: 设计线索增强 (enrich)

| 属性 | 值 |
|------|---|
| 触发时机 | merge 完成后, 可手动或自动触发 |
| 输入 | 主体简单描述 (如 "男性丧尸，身穿破烂的保安服...") |
| 输出 | 摄影级扩展 prompt, 存入 generation job 的 input_json.resolved_prompts.final |
| 核心价值 | 把一句话描述扩展为专业摄影棚级生图 prompt, 含形制/材质/布光/画风锁等维度 |

详见 [第 6 节](#6-设计线索增强扩展-prompt-结构)。

### 2.4 LLM-4: 父主体归纳 (parents)

| 属性 | 值 |
|------|---|
| 触发时机 | enrich 完成后 (或并行) |
| 输入 | 子主体集合 |
| 输出 | 父主体列表: `[{subject_id, name, type, description, child_count}]` |
| 归纳逻辑 | 同类子主体聚合为父主体 (如多集出现的同一角色不同形态), child_count = 形态数 |

**父主体模型:**

```json
{
  "items": [
    {"subject_id":"<uuid>","name":"安全区","type":"scene","description":"...","child_count":2,"created_at":"...","updated_at":"..."}
  ]
}
```

### 2.5 LLM-5: 分镜生成

| 属性 | 值 |
|------|---|
| 触发时机 | 视频页 [分镜生成] 按钮, 为当前选中集生成 |
| 输入 | 单集剧本 + 该集主体列表 |
| 输出 | 层级结构: 场景(Scenes) -> 镜头组(Shot Groups) -> 镜头(Shots) |
| 产出内容 | 每镜头: content (画面内容) + description (摄影级结构化描述); 每镜头组: prompt (多镜头分时段组合) + reference_frames + materials 引用 |

详见 [第 7 节](#7-分镜-shot-prompt-结构)。

---

## 3. 文生图能力详解

### 3.1 模型清单

| 模型标识 | 模型名称 | 适用场景 | 尺寸 | 比例 |
|---------|---------|---------|------|------|
| `banana-2` | Google Nano Banana (Gemini 2.5 Flash Image) | 主体参考图生成 (角色/场景/道具) | 2k | 16:9 |
| `gpt-image-2` | OpenAI GPT Image 2 | 宣传图/海报/封面/自由创作 | 2k | 16:9 |

### 3.2 主体设计图生成流程

```
主体 (Subject) 
  -> 固定模板 prompt 构造 (第 5 节)
  -> enrich 扩展为 resolved_prompts.final (第 6 节)
  -> 创建 generation job (job_type: generate_images)
  -> 异步生成 (积分预扣)
  -> 生成多张候选图 (imageOutputs[])
  -> 人工审核 (资产库审核弹窗)
  -> 通过 1 张为 image_entry
```

### 3.3 图片节点 (Canvas Node) 数据模型

```json
{
  "id": "<uuid>",
  "type": "imageUpload",
  "position": {"x":1120, "y":509.5},
  "data": {
    "title": "保安丧尸 设计图",
    "variant": "generated",
    "imageUrl": null,
    "prompt": "<固定模板 prompt>",
    "model": "banana-2",
    "size": "2k",
    "aspect": "16:9",
    "isGenerating": false,
    "generationJobId": null,
    "subjectId": "<uuid>",
    "subjectName": "保安丧尸",
    "subjectType": "character",
    "subjectDescription": "...",
    "subjectEpisodeIds": ["<uuid>"],
    "subjectEpisodeNos": [1, 2],
    "subjectDesignStatus": "draft",
    "promptVariationIndex": 0,
    "imagesExpanded": false,
    "generatingStartedAt": 1784015617046,
    "assetId": "<uuid>",
    "imageOutputs": [
      {"status":"succeeded","assetId":"<uuid>","storageKey":"/generate_images/<mid>/<id>.png"}
    ],
    "primaryImageIndex": 0
  }
}
```

关键字段说明:

| 字段 | 说明 |
|------|------|
| `variant` | generated (AI生成) / uploaded (手动上传) / text / storyboard |
| `model` | banana-2 或 gpt-image-2 |
| `size` / `aspect` | 固定 2k / 16:9 |
| `promptVariationIndex` | prompt 变体序号, 支持多版本 prompt |
| `primaryImageIndex` | 主选图序号, 可生成多张选一 |
| `imageOutputs` | 生成结果数组, 每条含 status/assetId/storageKey |
| `subjectDesignStatus` | draft (草稿) / 其他状态 |

### 3.4 图片资产元数据

```
GET /v3api/materials/<mid>/image/<image_id>
```

```json
{
  "image_id": "<uuid>",
  "storage_key": "/generate_images/<mid>/<id>.png",
  "mime_type": "image/png",
  "size_bytes": 2411098,
  "source": "generate_images"
}
```

图片实际 URL: `https://sapi.yangshipin.cn/api/x/cctv/zhihui/flow-app/generate_images/<mid>/<id>.png`
(独立 sapi 服务域名, 对象存储 + 鉴权代理)

### 3.5 批量操作

- **一键设计 N**: 批量为未设计主体生成图 (消耗积分), 创建绑定图片节点并触发生图
- **一键提交审核 N**: 批量提交可审核主体到审核队列
- **生图设置**: 模型/尺寸/比例/风格等生图参数配置面板

---

## 4. 视频生成能力详解

### 4.1 模型清单

| 模型标识 | 模型名称 | Provider | 适用场景 |
|---------|---------|----------|---------|
| `doubao-seedance-2-0-260128` | 豆包 Seedance 2.0 (字节跳动) | jianyu / modelsai | 多参考图图生视频 |
| `Seedance 2.0` | Seedance 2.0 (显示名) | modelsai (active-provider 默认) | 同上 |

### 4.2 视频生成任务模型

```
GET /v3api/video-generation-jobs?script_id=<sid>&episode_id=<eid>&limit=500
```

```json
{
  "items": [
    {
      "job_id": "<uuid>",
      "script_id": "<uuid>",
      "episode_id": "<uuid>",
      "scene_no": 1,
      "unit_id": "group:<shot_group_id>",
      "unit_runtime_key": "<runtime>:<scene_no>:group:<group_id>",
      "provider_key": "jianyu",
      "job_type": "all_reference",
      "status": "succeeded",
      "progress": 100,
      "progress_stage": "succeeded",
      "progress_message": "success",
      "error_message": null,
      "input_json": {
        "model": "doubao-seedance-2-0-260128",
        "reference_images": [
          {
            "kind": "storage",
            "storage_key": "/upload_images|generate_images/<mid>/<id>.png",
            "mime_type": "image/png",
            "size_bytes": 2411098,
            "sha256": "<hash>"
          }
        ],
        "reference_image_names": ["手机", "王贵发", "郊外荒地-外-夜"],
        "reference_audios": [],
        "reference_audio_names": [],
        "reference_videos": [],
        "reference_video_names": [],
        "prompt": "【镜头1:0～3s】..."
      }
    }
  ]
}
```

### 4.3 关键字段说明

| 字段 | 说明 |
|------|------|
| `unit_id` | 生成单元 = 镜头组 (group:<shot_group_id>) |
| `unit_runtime_key` | 运行时键, 格式 `<runtime>:<scene_no>:group:<group_id>` |
| `provider_key` | 视频生成供应商: jianyu / modelsai |
| `job_type` | all_reference (多参考图生视频) |
| `reference_images` | 参考图数组 = 镜头组引用的主体设计图 |
| `reference_image_names` | 参考图对应主体名 (与 reference_images 一一对应) |
| `reference_audios` | 参考音频 (配音素材) |
| `reference_videos` | 参考视频 |
| `prompt` | shot_group 的组合 prompt |

### 4.4 生成流程

```
镜头组 (Shot Group)
  -> 汇集 prompt 中引用的主体 (materials.character/background/prop)
  -> 自动取每个主体已通过的设计图 (image_entry) 作为 reference_images
  -> reference_image_names 对应主体名
  -> 组合 prompt + 多参考图 -> Seedance 2.0 图生视频
  -> 异步 job (积分预扣: reserve -> settle/release)
  -> 产出视频片段
```

---

## 5. 主体设计图 Prompt 模板

### 5.1 固定模板结构

平台为主体设计图生成使用一套固定 prompt 模板, 模板包含 4 个插槽 (主体名称/主体类型/主体描述/固定要求):

```
为以下主体设计一张清晰可用于短剧视觉资产库的参考图。
主体名称：{subject_name}
主体类型：{subject_type}
主体描述：{subject_description}
要求：主体清晰、特征稳定、背景干扰少、适合后续多图参考生成。
```

### 5.2 实际示例

以主体 "保安丧尸" 为例:

```
为以下主体设计一张清晰可用于短剧视觉资产库的参考图。
主体名称：保安丧尸
主体类型：角色
主体描述：男性丧尸，身穿破烂的保安服。面部狰狞，嘴角挂着不明液体，体态扭曲踉跄。
要求：主体清晰、特征稳定、背景干扰少、适合后续多图参考生成。
```

### 5.3 模板设计原则

| 原则 | 说明 |
|------|------|
| 资产库导向 | 明确声明 "用于短剧视觉资产库的参考图", 锚定用途 |
| 主体清晰 | 要求主体在画面中清晰可辨 |
| 特征稳定 | 要求特征表现稳定, 便于跨镜头一致性 |
| 背景干扰少 | 减少背景噪声, 便于后续作为多参考图使用 |
| 多图参考适配 | 明确 "适合后续多图参考生成", 为视频生成阶段铺路 |

### 5.4 变体机制

- `promptVariationIndex` 字段支持多版本 prompt 变体, 可为同一主体生成不同版本的设计图
- 变体 0 为默认版本, 可扩展多个变体供用户选择

### 5.5 模板与 enrich 的关系

```
固定模板 prompt (本节)
  |
  v
enrich 设计线索增强 (第 6 节)
  |
  v
resolved_prompts.final (写入 generation job 的 input_json)
  |
  v
文生图模型 (banana-2 / gpt-image-2) 实际调用
```

即: 固定模板是 enrich LLM 的输入, enrich 扩展后的 resolved_prompts.final 才是实际传给文生图模型的 prompt。

---

## 6. 设计线索增强扩展 Prompt 结构

### 6.1 概述

设计线索增强 (enrich) 是剧本流水线的第 4 阶段, 将主体的简单描述扩展为专业摄影级生图 prompt。扩展后的 prompt 存入 generation job 的 `input_json.resolved_prompts.final` 字段。

### 6.2 扩展 Prompt 的段落结构

扩展后的 prompt 包含以下固定段落 (维度), 每个段落对应一个视觉控制维度:

| 序号 | 段落名称 | 控制维度 | 说明 |
|------|---------|---------|------|
| 1 | 形制描述 | 形态/造型 | 主体的外形、结构、比例的精确描述 |
| 2 | 材质钉死 | 材质纹理 | 明确指定材质 (金属/布料/木质等), 防止模型自由发挥 |
| 3 | 使用痕迹 | 做旧/磨损 | 描述使用痕迹、磨损、污渍等, 增加真实感 |
| 4 | 摆放姿态 | 构图姿态 | 主体在画面中的摆放角度与姿态 |
| 5 | 反悬浮锁 | 物理接地 | 确保主体有物理支撑, 不出现悬浮感 |
| 6 | 时代锚定 | 年代风格 | 锁定时代背景 (如 21 世纪初中国城镇), 防止时代错位 |
| 7 | 画面构图 | 构图法则 | 画面构图方式 (三分法/居中/黄金比例等) |
| 8 | 画风统一锁 | 风格一致 | 统一所有主体设计图的画风, 确保资产库风格一致 |
| 9 | 三点柔光箱白底布光 | 布光方案 | 三点柔光箱 + 白底摄影棚布光, 专业产品摄影级 |
| 10 | 反水印锁 | 防水印 | 禁止生成水印/Logo/签名等 |
| 11 | 文字渲染降级方案 | 文字处理 | 对画面中文字的降级处理 (模糊/遮挡/最小化) |
| 12 | 写实质感锚定 | 写实锁定 | Phase One 中画幅 + 100mm 微距 + f/8, 锁定写实质感 |

### 6.3 布光与质感锚定细节

**三点柔光箱白底布光:**
- 主光: 柔光箱, 45 度角, 提供主照明
- 辅光: 柔光箱, 另一侧 45 度角, 补充阴影
- 轮廓光: 背后方, 分离主体与背景
- 背景: 纯白底 (白底布光)

**写实质感锚定:**
- 相机: Phase One 中画幅
- 镜头: 100mm 微距
- 光圈: f/8 (确保景深覆盖)
- 锁定写实质感, 防止插画/卡通/油画等风格漂移

### 6.4 "锁" 机制设计哲学

平台在扩展 prompt 中大量使用 "锁" (lock) 概念, 即通过显式约束锁定某个维度, 防止生成模型自由发挥导致的不一致:

| 锁名称 | 锁定目标 | 反制问题 |
|--------|---------|---------|
| 反悬浮锁 | 物理接地 | 主体悬浮在空中 |
| 画风统一锁 | 画风一致 | 不同主体画风不统一 |
| 反水印锁 | 无水印 | 画面出现水印/Logo |
| 写实质感锚定 | 写实风格 | 风格漂移为插画/卡通 |
| 材质钉死 | 材质准确 | 材质随机变化 |

### 6.5 存储位置

扩展 prompt 存储在 generation job 的 input_json 中:

```json
{
  "job_id": "<uuid>",
  "job_type": "generate_images",
  "input_json": "{\"prompt\":\"<固定模板>\",\"resolved_prompts\":{\"final\":\"<扩展后prompt>\"}}"
}
```

- `input_json.prompt`: 原始固定模板 prompt
- `input_json.resolved_prompts.final`: enrich 扩展后的最终 prompt (实际传给文生图模型)

### 6.6 道具/场景版与角色版的差异

根据探索笔记, 扩展 prompt 模板有道具/场景版 (含摆放姿态/使用痕迹等), 角色版可能略有不同但核心段落结构一致。复刻时建议按主体类型 (character/scene/prop/sound) 分别设计变体:

| 主体类型 | 差异段落 | 说明 |
|---------|---------|------|
| character (角色) | 摆放姿态 -> 人物姿态/表情; 无摆放姿态 | 角色需描述表情/姿态/服装细节 |
| scene (场景) | 形制描述 -> 场景布局; 无摆放姿态 | 场景需描述空间布局/光线氛围 |
| prop (道具) | 全段落适用 | 道具最需要摆放姿态/使用痕迹/材质钉死 |
| sound (声音) | 不生图 | 声音类型不生成图片, 仅挂载音频 |

---

## 7. 分镜 Shot Prompt 结构

### 7.1 分镜层级结构

```
Episode (集)
  -> Scene (场景, 按背景主体聚合)
    -> Shot Group (镜头组, 视频生成单元)
      -> Shot (镜头, 最小叙事单元)
```

### 7.2 Shot (镜头) Prompt 结构

每个镜头有两个字段:

**content (画面内容, 自然语言):**
```
昏暗的办公室内，王贵发坐在老旧木桌后，韩志明局促地站在对面，两人形成压抑的对峙感。
```

**description (摄影级结构化描述):**
```
在【养鸡场办公室-内-夜】中采用【平视机位】拍摄视角，画面主体【王贵发、韩志明】的关键动作是【...】，整体表现的画面是【...】，表达了【压抑且焦灼】，整体画面的光影效果是【廉价冷色调荧光灯，从上方投射下生硬的阴影】，整个镜头的叙事目的是【建立空间关系，交代人物身份与当下的紧张氛围】。
```

description 的模板骨架:

```
在【{背景场景}】中采用【{机位}】拍摄视角，画面主体【{角色名}】的关键动作是【{动作}】，整体表现的画面是【{画面描述}】，表达了【{情绪}】，整体画面的光影效果是【{光影描述}】，整个镜头的叙事目的是【{叙事目的}】。
```

| 插槽 | 说明 | 示例 |
|------|------|------|
| 背景场景 | 场景主体名 | 养鸡场办公室-内-夜 |
| 机位 | 摄影机位/视角 | 平视机位 / 俯拍特写 / 全景 |
| 角色名 | 画面中的角色 | 王贵发、韩志明 |
| 动作 | 角色关键动作 | (剧本驱动) |
| 画面描述 | 整体画面内容 | (剧本驱动) |
| 情绪 | 画面表达的情绪 | 压抑且焦灼 |
| 光影描述 | 光线/光源/阴影 | 廉价冷色调荧光灯，从上方投射下生硬的阴影 |
| 叙事目的 | 镜头的叙事功能 | 建立空间关系，交代人物身份与当下的紧张氛围 |

### 7.3 Shot Group (镜头组) Prompt 结构

镜头组是视频生成的最小单元, 其 prompt 将多个镜头按时段组合:

```
【镜头1:0～4s】场景是养鸡场办公室-内-夜，全景·平视机位，固定，廉价冷色调荧光灯...情绪压抑且焦灼。
【镜头2:4～7s】...特写·俯拍特写...
【角色】养鸡场办公室-内-夜、王贵发、韩志明
【叙事目的】建立空间关系，交代人物身份与当下的紧张氛围。
【约束】人物特征高度一致，角色不脸崩、肢体不畸形，多镜头切镜丝滑平顺，画面质量4K稳定。语音清晰，情感表达贴合角色性格，台词与唇形/动作精准对位。禁止出现任何形式的字幕。
```

**组合 prompt 模板骨架:**

```
{对每个镜头:}
【镜头{N}:{起}s～{止}s】场景是{场景描述}，{景别}·{机位}，{运镜}，{光影}，{画面动作}，{情绪}，{角色名}:{台词}

【角色】{引用主体名列表}
【叙事目的】{镜头组叙事目的}
【约束】{视频生成约束锁 (见第 8 节)}
```

### 7.4 单镜头行模板

```
【镜头{N}:{起}s～{止}s】场景是{场景}，{景别}·{机位}，{运镜(固定/推轨/拉轨)}，{光影}，{画面动作}，{情绪}，{角色名}:{台词}
```

| 字段 | 可选值/说明 |
|------|------------|
| 景别 | 全景 / 中景 / 近景 / 特写 / 俯拍特写 |
| 机位 | 平视 / 俯拍 / 仰拍 / 侧拍 |
| 运镜 | 固定 / 推轨 / 拉轨 / 摇镜 / 跟拍 |

### 7.5 Shot Group 完整数据模型

```json
{
  "shot_group_id": "<uuid>",
  "scene_no": 1,
  "group_no": "G1",
  "prompt": "<组合 prompt (见 7.3)>",
  "reference_frames": {
    "first_frame": null,
    "last_frame": null,
    "reference_frame": null
  },
  "shots": [
    {
      "shot_id": "<uuid>",
      "scene_no": 1,
      "shot_group_id": "<uuid>",
      "group_no": "G1",
      "shot_no": "2x2x2x2x2x",
      "display_no": 1,
      "content": "<画面内容>",
      "description": "<摄影级结构化描述>"
    }
  ]
}
```

### 7.6 Video Workbench 中的镜头组扩展字段

```
GET /v3api/shots/<sid>/episodes/<eid>/video-workbench
```

镜头组在 video-workbench 中额外包含:

| 字段 | 说明 |
|------|------|
| `background` | 背景主体名 (如 "养鸡场办公室-内-夜") |
| `scene_key` | 场景键, 格式 `bg:<背景名>` |
| `materials` | 该组引用的所有主体: `{character:[], background:[], prop:[], other:[]}` |
| `audio_elements` | 需配音的角色名列表 |
| `duration_seconds` | 视频时长 (秒) |
| `video_duration_seconds` | 实际视频时长 |
| `shot_ids` / `shot_display_nos` / `shot_nos` | 组内镜头 ID 列表 |
| `description_text` | 组合描述文本 |
| `unit_id` | 生成单元 ID: `group:<shot_group_id>` |
| `unit_runtime_key` | 运行时键: `<runtime>:<scene_no>:group:<group_id>` |

---

## 8. 视频生成约束锁

### 8.1 约束锁内容

每个 shot_group 的 prompt 末尾固定附带一段约束锁, 作为视频生成的质量约束:

```
【约束】人物特征高度一致，角色不脸崩、肢体不畸形，多镜头切镜丝滑平顺，画面质量4K稳定。语音清晰，情感表达贴合角色性格，台词与唇形/动作精准对位。禁止出现任何形式的字幕。
```

### 8.2 约束锁逐条解析

| 序号 | 约束条目 | 控制目标 | 反制问题 |
|------|---------|---------|---------|
| 1 | 人物特征高度一致 | 跨镜头一致性 | 同一角色在不同镜头中外貌不一致 |
| 2 | 角色不脸崩 | 面部质量 | 面部扭曲/变形/模糊 |
| 3 | 肢体不畸形 | 身体质量 | 肢体扭曲/多余手指/关节反折 |
| 4 | 多镜头切镜丝滑平顺 | 转场质量 | 切镜生硬/跳帧/不连贯 |
| 5 | 画面质量4K稳定 | 分辨率/画质 | 画面模糊/分辨率不足/画质波动 |
| 6 | 语音清晰 | 音频质量 | 语音含混/噪声/失真 |
| 7 | 情感表达贴合角色性格 | 表演质量 | 情感与角色设定不符 |
| 8 | 台词与唇形/动作精准对位 | 唇形同步 | 唇形与台词不匹配/动作与台词脱节 |
| 9 | 禁止出现任何形式的字幕 | 字幕控制 | 画面出现误生成的字幕/文字 |

### 8.3 约束锁设计原则

1. **一致性优先**: 第 1 条确保角色跨镜头一致, 这是多参考图机制的核心目标
2. **防退化**: 第 2-3 条防止 AI 视频模型常见的面部/肢体退化
3. **转场质量**: 第 4 条确保多镜头组合时的切镜质量
4. **技术规格**: 第 5-6 条锁定分辨率和音频质量
5. **表演精准**: 第 7-8 条确保角色表演与台词对位
6. **反字幕**: 第 9 条禁止误生成字幕 (视频模型常见问题)

### 8.4 约束锁与视频模型的配合

约束锁作为 shot_group prompt 的一部分传入 Seedance 2.0, 是 prompt-level 的软约束 (非参数级硬约束)。复刻时需注意:

- 约束锁是 prompt 文本的一部分, 并非独立的 API 参数
- 实际效果取决于视频模型对 prompt 的遵从度
- 建议在视频生成 API 调用时将约束锁放在 prompt 末尾, 以利用模型的 recency bias

---

## 9. 多参考图机制

### 9.1 机制概述

视频生成阶段使用多参考图 (Multi-Reference Image) 机制, 确保视频中的角色/场景/道具与主体设计图保持一致。

### 9.2 自动汇聚流程

```
1. shot_group.prompt 中【角色】段列出引用的主体名
   如: 【角色】养鸡场办公室-内-夜、王贵发、韩志明
2. 系统解析 prompt, 提取主体名列表
3. 对每个主体名, 查找其已通过审核的设计图 (image_entry)
4. 汇集为 reference_images 数组
5. reference_image_names 与 reference_images 一一对应
6. 组合 prompt + reference_images -> Seedance 2.0 多参考图图生视频
```

### 9.3 参考图数据结构

```json
{
  "reference_images": [
    {
      "kind": "storage",
      "storage_key": "/upload_images|generate_images/<mid>/<id>.png",
      "mime_type": "image/png",
      "size_bytes": 2411098,
      "sha256": "<hash>"
    }
  ],
  "reference_image_names": ["手机", "王贵发", "郊外荒地-外-夜"]
}
```

| 字段 | 说明 |
|------|------|
| `reference_images[].kind` | storage (存储引用) |
| `reference_images[].storage_key` | 图片存储路径, 前缀为 /upload_images/ (手动上传) 或 /generate_images/ (AI生成) |
| `reference_images[].mime_type` | 图片 MIME 类型 |
| `reference_images[].size_bytes` | 图片字节数 |
| `reference_images[].sha256` | 图片 SHA256 哈希 (用于去重/校验) |
| `reference_image_names` | 主体名列表, 与 reference_images 按序对应 |

### 9.4 materials 与参考图的映射

shot_group 的 materials 字段标明引用的主体, 系统据此自动汇聚参考图:

```json
{
  "materials": {
    "character": ["王贵发", "韩志明"],
    "background": ["养鸡场办公室-内-夜"],
    "prop": [],
    "other": []
  }
}
```

| materials 类别 | 对应主体类型 | 参考图来源 |
|---------------|-------------|-----------|
| character | 角色 | 角色 image_entry |
| background | 场景 | 场景 image_entry |
| prop | 道具 | 道具 image_entry |
| other | 其他 | 其他 image_entry |

### 9.5 参考图来源类型

| storage_key 前缀 | 来源 | 说明 |
|-----------------|------|------|
| `/generate_images/` | AI 生成 | 通过文生图 (banana-2/gpt-image-2) 生成 |
| `/upload_images/` | 手动上传 | 用户在画布上手动上传的图片 |

两种来源均可作为视频生成的参考图, 系统不区分来源, 统一通过 storage_key 引用。

### 9.6 参考音频/视频

除了参考图, 视频生成还支持:

```json
{
  "reference_audios": [],
  "reference_audio_names": [],
  "reference_videos": [],
  "reference_video_names": []
}
```

- `reference_audios`: 参考音频 (配音素材, 关联 audio_elements 中的角色)
- `reference_videos`: 参考视频 (用于视频风格/运动参考)

### 9.7 前置依赖

多参考图机制依赖资产库审核流程:

```
主体生成图片候选 -> 人工审核 -> 通过 1 张为 image_entry
                                    |
                                    v
                         视频生成时自动取为 reference_image
```

如果某主体尚未通过审核 (review_status != approved, image_entry == null), 则该主体不会出现在 reference_images 中, 可能导致视频中该主体特征不一致。

---

## 10. 首尾帧连续性机制

### 10.1 机制概述

为确保相邻视频片段 (镜头组) 之间的视觉连续性, 平台为首尾帧衔接设计了 reference_frames 机制。

### 10.2 reference_frames 数据结构

```json
{
  "reference_frames": {
    "first_frame": null,
    "last_frame": null,
    "reference_frame": null
  }
}
```

| 字段 | 说明 |
|------|------|
| `first_frame` | 当前镜头组的首帧参考图 (用于对齐上一组尾帧) |
| `last_frame` | 当前镜头组的尾帧参考图 (用于对齐下一组首帧) |
| `reference_frame` | 通用参考帧 (额外参考) |

### 10.3 尾帧 -> 首帧衔接逻辑

```
镜头组 A (尾帧 = last_frame_A)
     |
     v  尾帧 -> 首帧衔接
镜头组 B (首帧 = first_frame_B, 应与 last_frame_A 一致或连续)
     |
     v
镜头组 C ...
```

- 上一组的 `last_frame` 作为下一组的 `first_frame` 参考
- 视频生成模型接收首帧参考, 确保片段开头与上一组结尾连贯
- 实现切镜丝滑平顺 (约束锁第 4 条)

### 10.4 当前状态

探索笔记显示 reference_frames 字段当前为 null, 说明:
- 该机制已在数据模型中定义
- 实际生成时可能由系统自动填充 (从已生成视频提取帧)
- 或需要人工指定首/尾帧参考图

### 10.5 复刻建议

1. 在首个镜头组生成后, 从生成视频末尾提取一帧作为 `last_frame`
2. 将该 `last_frame` 赋值给下一镜头组的 `first_frame`
3. 视频生成 API 调用时传入 `first_frame` 作为首帧约束
4. 递归处理所有镜头组, 形成连续链式衔接

---

## 11. Provider 与模型切换机制

### 11.1 文生图 Provider 切换

文生图通过画布节点的 `model` 字段切换:

| model 值 | 模型 | 适用场景 |
|---------|------|---------|
| `banana-2` | Google Nano Banana (Gemini 2.5 Flash Image) | 主体参考图 (角色/场景/道具) |
| `gpt-image-2` | OpenAI GPT Image 2 | 宣传图/海报/封面/自由创作 |

**生图设置面板** (`[生图设置]` 按钮): 模型/尺寸/比例/风格等参数配置, 在画布右侧栏。

节点级配置示例:

```json
{
  "data": {
    "model": "banana-2",
    "size": "2k",
    "aspect": "16:9"
  }
}
```

### 11.2 视频生成 Provider 切换

视频生成通过全局 active-provider 配置切换:

```
GET /v3api/video-generation/active-provider
```

```json
{
  "provider_key": "modelsai",
  "job_type": "all_reference",
  "model": "Seedance 2.0"
}
```

| 字段 | 说明 | 可选值 |
|------|------|--------|
| `provider_key` | 视频生成供应商 | jianyu / modelsai |
| `job_type` | 生成类型 | all_reference (多参考图生视频) |
| `model` | 模型显示名 | Seedance 2.0 |

**Provider 说明 (推测):**

| provider_key | 推测对应 | 说明 |
|-------------|---------|------|
| `jianyu` | 即梦 (字节跳动 AI 创作平台) | 字节系视频生成 |
| `modelsai` | ModelScope 或其他视频模型平台 | 通用视频模型 |

**实际模型 (input_json.model):**

| 模型标识 | 模型名称 | 说明 |
|---------|---------|------|
| `doubao-seedance-2-0-260128` | 豆包 Seedance 2.0 | 字节跳动视频生成模型, 后缀 260128 为版本号 |

### 11.3 Provider 切换层级

| 层级 | 切换对象 | 切换方式 | 影响范围 |
|------|---------|---------|---------|
| 全局 | 视频生成 provider + 模型 | active-provider 接口 | 所有后续视频生成任务 |
| 节点级 | 文生图模型 | 画布节点 data.model | 单个设计图节点 |
| 任务级 | 视频生成实际模型 | job 的 input_json.model | 单个视频生成任务 |

### 11.4 job_type 枚举

| job_type | 说明 |
|----------|------|
| `all_reference` | 多参考图生视频 (当前唯一类型) |
| (其他类型待发现) | 可能存在纯文生视频/单参考图等模式 |

---

## 12. 通用 LLM 提示词骨架与输入输出契约建议

### 12.1 LLM-1 主体提取 - 提示词骨架与契约

**提示词骨架:**

```
你是一名短剧剧本分析师。请从以下剧本文本中提取所有视觉与声音主体, 包括角色(character)、场景(scene)、道具(prop)、声音/配音音色(sound)。

对每个主体, 生成:
- name: 主体名称 (简洁, 如 "王贵发"、"养鸡场办公室-内-夜")
- type: 类型 (character|scene|prop|sound)
- description: 详细描述 (角色: 外貌/服装/体态/表情; 场景: 空间布局/光线/氛围; 道具: 外观/材质/尺寸; 声音: 音色/语速/情感特征)
- is_child: 是否为子主体 (true)
- episode_nos: 出现的集数列表

要求:
1. description 必须足够详细, 可直接用作后续文生图/配音的基础 prompt
2. 跨集出现的同一主体必须使用相同的 name
3. 场景名格式: "地点-内/外-日/夜" (如 "养鸡场办公室-内-夜")

剧本文本:
---
{script_text}
---

输出 JSON 数组, 每个元素为一个主体。
```

**输入契约:**

```json
{
  "episode_id": "<uuid>",
  "episode_no": 1,
  "script_text": "<剧本文本>"
}
```

**输出契约:**

```json
{
  "subjects": [
    {
      "type": "character|scene|prop|sound",
      "name": "<主体名>",
      "description": "<AI 生成的详细描述>",
      "is_child": true,
      "episode_nos": [1]
    }
  ]
}
```

### 12.2 LLM-2 跨集合并 - 提示词骨架与契约

**提示词骨架:**

```
你是一名短剧剧本分析师。以下是多集提取的主体列表, 请将跨集出现的同一主体 (同名同类型) 合并, episode_nos 取并集。

输入主体列表:
---
{multi_episode_subjects_json}
---

合并规则:
1. name 相同且 type 相同的主体视为同一主体, 合并
2. description 保留最详细的版本, 或合并补充
3. episode_nos 取并集并排序
4. 保留所有不重复的主体

输出合并后的 JSON 数组。
```

**输入契约:**

```json
{
  "subjects_from_all_episodes": [
    {"name":"王贵发","type":"character","description":"...","episode_nos":[1]},
    {"name":"王贵发","type":"character","description":"...","episode_nos":[2]}
  ]
}
```

**输出契约:**

```json
{
  "merged_subjects": [
    {"name":"王贵发","type":"character","description":"<合并后描述>","episode_nos":[1,2],"is_child":true}
  ],
  "total_count": 44
}
```

### 12.3 LLM-3 设计线索增强 - 提示词骨架与契约

**提示词骨架:**

```
你是一名专业产品/角色摄影指导。请将以下主体的简单描述扩展为专业摄影级生图 prompt。

主体信息:
- 名称: {subject_name}
- 类型: {subject_type}
- 描述: {subject_description}

扩展 prompt 必须包含以下 12 个段落, 每段用小标题标注:

1. 【形制描述】主体的外形、结构、比例精确描述
2. 【材质钉死】明确指定材质 (金属/布料/木质/皮肤等), 禁止模糊
3. 【使用痕迹】描述磨损/污渍/老化等使用痕迹, 增加真实感
4. 【摆放姿态】主体在画面中的摆放角度与姿态
5. 【反悬浮锁】确保主体有物理支撑, 禁止悬浮感
6. 【时代锚定】锁定时代背景 (如 21 世纪初中国城镇), 禁止时代错位
7. 【画面构图】构图方式 (三分法/居中/黄金比例)
8. 【画风统一锁】统一写实摄影风格, 禁止插画/卡通/油画
9. 【三点柔光箱白底布光】三点柔光箱 (主光45度/辅光45度/轮廓光后方) + 纯白底
10. 【反水印锁】禁止生成水印/Logo/签名
11. 【文字渲染降级方案】画面中文字模糊/遮挡/最小化
12. 【写实质感锚定】Phase One 中画幅 + 100mm 微距 + f/8, 写实摄影质感

输出格式: JSON, 字段 final 为完整扩展 prompt 文本。
```

**输入契约:**

```json
{
  "subject_name": "保安丧尸",
  "subject_type": "character",
  "subject_description": "男性丧尸，身穿破烂的保安服。面部狰狞，嘴角挂着不明液体，体态扭曲踉跄。",
  "base_prompt": "为以下主体设计一张清晰可用于短剧视觉资产库的参考图。\n主体名称：保安丧尸\n主体类型：角色\n主体描述：男性丧尸...\n要求：主体清晰、特征稳定、背景干扰少、适合后续多图参考生成。"
}
```

**输出契约:**

```json
{
  "resolved_prompts": {
    "final": "<12 段落扩展后的完整 prompt 文本>"
  }
}
```

### 12.4 LLM-4 父主体归纳 - 提示词骨架与契约

**提示词骨架:**

```
你是一名短剧剧本分析师。以下是去重后的子主体列表, 请将同类子主体 (如同一角色的不同形态/同一场景的不同时段) 归纳为父主体。

归纳规则:
1. 同一角色不同状态/形态 -> 归为一个父主体, child_count = 形态数
2. 同一场景不同时段 (日/夜) -> 可归为一个父主体
3. 父主体 name 取代表性名称, description 合并概括
4. 无关联的独立子主体不归纳, 单独作为父主体 (child_count=1) 或留在 "其余子主体"

子主体列表:
---
{merged_subjects_json}
---

输出 JSON 数组, 每个元素为一个父主体。
```

**输入契约:**

```json
{
  "merged_subjects": [
    {"subject_id":"<uuid>","name":"保安丧尸(白天)","type":"character","description":"..."},
    {"subject_id":"<uuid>","name":"保安丧尸(夜晚)","type":"character","description":"..."}
  ]
}
```

**输出契约:**

```json
{
  "parent_subjects": [
    {
      "name": "保安丧尸",
      "type": "character",
      "description": "<合并概括描述>",
      "child_count": 2,
      "child_ids": ["<uuid>", "<uuid>"]
    }
  ]
}
```

### 12.5 LLM-5 分镜生成 - 提示词骨架与契约

**提示词骨架:**

```
你是一名短剧导演/摄影指导。请将以下集的剧本拆分为分镜, 层级为: 场景(Scenes) -> 镜头组(Shot Groups) -> 镜头(Shots)。

分镜规则:
1. 按场景 (背景地点) 划分 Scene, 同一场景的镜头归入同一 Scene
2. 同一场景内, 将时间连续、叙事紧凑的镜头归入同一 Shot Group (每组 2-5 个镜头, 总时长 3-15 秒)
3. 每个镜头需生成:
   - content: 画面内容描述 (自然语言)
   - description: 摄影级结构化描述, 格式: "在【{背景}】中采用【{机位}】拍摄视角，画面主体【{角色}】的关键动作是【{动作}】，整体表现的画面是【{画面}】，表达了【{情绪}】，整体画面的光影效果是【{光影}】，整个镜头的叙事目的是【{目的}】。"
4. 每个 Shot Group 需生成组合 prompt, 格式:
   - 对每个镜头: 【镜头{N}:{起}s～{止}s】场景是{场景}，{景别}·{机位}，{运镜}，{光影}，{画面动作}，{情绪}，{角色名}:{台词}
   - 【角色】列出引用的所有主体名
   - 【叙事目的】该组的叙事目的
   - 【约束】人物特征高度一致，角色不脸崩、肢体不畸形，多镜头切镜丝滑平顺，画面质量4K稳定。语音清晰，情感表达贴合角色性格，台词与唇形/动作精准对位。禁止出现任何形式的字幕。

可选景别: 全景/中景/近景/特写/俯拍特写
可选机位: 平视/俯拍/仰拍/侧拍
可选运镜: 固定/推轨/拉轨/摇镜/跟拍

集剧本文本:
---
{episode_script_text}
---

可用主体列表 (作为画面引用):
---
{subjects_json}
---

输出 JSON, 结构: {episode_id, scenes:[{scene_no, groups:[{shot_group_id, group_no, prompt, shots:[...]}]}]}
```

**输入契约:**

```json
{
  "episode_id": "<uuid>",
  "episode_no": 1,
  "script_text": "<集剧本文本>",
  "subjects": [
    {"name":"王贵发","type":"character","description":"..."},
    {"name":"养鸡场办公室-内-夜","type":"scene","description":"..."}
  ]
}
```

**输出契约:**

```json
{
  "episode_id": "<uuid>",
  "episode_version": 3,
  "scenes": [
    {
      "scene_no": 1,
      "groups": [
        {
          "shot_group_id": "<uuid>",
          "group_no": "G1",
          "prompt": "【镜头1:0～4s】...\n【镜头2:4～7s】...\n【角色】...\n【叙事目的】...\n【约束】...",
          "reference_frames": {"first_frame": null, "last_frame": null, "reference_frame": null},
          "shots": [
            {
              "shot_id": "<uuid>",
              "shot_no": "<混淆id串>",
              "display_no": 1,
              "content": "<画面内容>",
              "description": "<摄影级结构化描述>"
            }
          ]
        }
      ],
      "ungrouped_shots": []
    }
  ]
}
```

---

## 13. 复刻开发要点与验收清单

### 13.1 架构要点

| 要点 | 说明 |
|------|------|
| LLM 服务封装 | 后端封装 5 个 LLM 任务 (extract/merge/enrich/parents/storyboard), 每个任务有独立的提示词模板和输入输出契约 |
| 异步任务系统 | 所有 AI 生成 (LLM/文生图/视频) 均为异步 job, 需实现 job 状态轮询机制 (status/progress/progress_stage) |
| 积分预扣三段式 | reserve (冻结) -> settle (结算) / release (释放), 每次生成任务启动时预扣, 完成时结算 |
| 多 Provider 抽象 | 文生图 (banana-2/gpt-image-2) 和视频生成 (jianyu/modelsai) 均需 Provider 抽象层, 支持运行时切换 |
| 对象存储 + 鉴权代理 | 生成图片/视频存储在 sapi 独立域名, 通过 storage_key 引用, 前端通过鉴权代理 URL 访问 |

### 13.2 Prompt 工程要点

| 要点 | 说明 |
|------|------|
| 固定模板 + 插槽 | 主体设计图使用固定模板 + 4 插槽 (名称/类型/描述/要求), 不允许用户自由编辑核心模板 |
| enrich 扩展链路 | 固定模板 -> enrich LLM 扩展 -> resolved_prompts.final -> 实际模型调用, 扩展是必须步骤 |
| 12 段落结构 | enrich 扩展 prompt 必须包含 12 个固定段落 (形制/材质/痕迹/姿态/反悬浮/时代/构图/画风/布光/反水印/文字降级/写实锚定) |
| 锁机制 | 大量使用 "锁" (lock) 概念锁定视觉维度, 防止生成模型自由发挥 |
| 分镜双字段 | 每个镜头有 content (自然语言) 和 description (结构化) 两个字段, 服务于不同消费者 |
| 镜头组组合 prompt | shot_group 的 prompt 将多镜头按时段组合, 是视频生成的直接输入 |
| 约束锁固定末尾 | 约束锁作为 prompt 固定末尾段, 利用模型 recency bias 提高遵从度 |
| 场景名规范 | 场景名格式 "地点-内/外-日/夜", 作为 scene_key 和 prompt 引用的标准格式 |

### 13.3 多参考图机制要点

| 要点 | 说明 |
|------|------|
| 自动汇聚 | 系统按 prompt 中【角色】段自动解析主体名, 取其已通过设计图为参考图 |
| 名称对应 | reference_image_names 与 reference_images 一一对应, 便于调试 |
| 来源不区分 | AI 生成 (/generate_images/) 和手动上传 (/upload_images/) 均可作为参考图 |
| 审核前置 | 主体必须通过资产审核 (image_entry != null) 才能作为参考图 |
| SHA256 校验 | 每张参考图含 sha256 哈希, 用于去重和完整性校验 |

### 13.4 首尾帧连续性要点

| 要点 | 说明 |
|------|------|
| 三帧模型 | reference_frames 含 first_frame/last_frame/reference_frame 三个字段 |
| 链式衔接 | 上一组 last_frame -> 下一组 first_frame, 实现片段间视觉连续 |
| 自动填充 | 建议从已生成视频末尾自动提取帧作为 last_frame |
| 约束配合 | 与约束锁 "切镜丝滑平顺" 配合使用 |

### 13.5 验收清单

- [ ] 剧本输入后, pipeline-status 返回 5 阶段状态, 支持异步轮询
- [ ] extract 阶段: LLM 从剧本提取 4 类主体 (character/scene/prop/sound), 每个含 AI 生成的详细 description
- [ ] merge 阶段: 跨集同名同类型主体合并, episode_nos 取并集
- [ ] enrich 阶段: 主体描述扩展为 12 段落摄影级 prompt, 存入 resolved_prompts.final
- [ ] parents 阶段: 同类子主体归纳为父主体, 含 child_count
- [ ] 主体设计图: 固定模板 prompt + enrich 扩展 -> 文生图 (banana-2, 2k, 16:9) -> 多张候选
- [ ] 资产审核: 候选图进入审核队列, 人工通过 1 张为 image_entry
- [ ] 画布节点: imageUpload 类型节点含 model/size/aspect/prompt/promptVariationIndex/primaryImageIndex 等字段
- [ ] 分镜生成: LLM 产出 场景 -> 镜头组 -> 镜头 层级, 每镜头含 content + description
- [ ] 镜头组 prompt: 多镜头分时段组合 (【镜头N:起~止s】) + 【角色】+【叙事目的】+【约束】
- [ ] 约束锁: 固定 9 条 (一致性/不脸崩/不畸形/切镜丝滑/4K/语音清晰/情感贴合/唇形对位/禁字幕)
- [ ] 多参考图: 按 prompt 中主体名自动汇聚已通过设计图为 reference_images, names 一一对应
- [ ] 视频生成: Seedance 2.0 (doubao-seedance-2-0-260128), job_type=all_reference, 异步轮询
- [ ] 首尾帧: reference_frames 含 first_frame/last_frame/reference_frame, 链式衔接
- [ ] Provider 切换: 文生图节点级 (model 字段), 视频全局级 (active-provider 接口)
- [ ] 积分三段式: reserve -> settle/release, 每次生成预扣冻结, 完成结算
- [ ] 图片存储: sapi 域名 + storage_key 引用, 支持 generate_images 和 upload_images 两种来源
- [ ] 时间线编排: 视频片段按 row_order 排列 + trim 裁剪, 合成最终视频
