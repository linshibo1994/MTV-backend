# 智绘 Flow App 前后端全接口对接文档

> 文档日期：2026-07-22
> 适用版本：当前工作区后端代码契约
> 接口规模：66 个路径、80 个 HTTP 操作
> 面向读者：Web 前端、客户端、联调与测试工程师

本文是前端对接当前后端实现的主文档。接口、请求模型、响应模型、权限、幂等和并发规则均以当前 FastAPI 路由及 Pydantic Schema 为准；旧需求文档只用于解释业务背景，不作为字段契约来源。

## 0. 前后端功能与接口目录

> **使用方式**：先按“功能对接顺序”完成一个页面或工作流，再打开对应的“详细契约”章节实现请求、字段、权限、幂等和异常处理。下方“完整接口目录”是 80 个公开 HTTP 操作的唯一联调清单；每完成一个接口可在本地勾选，避免遗漏。

### 0.1 功能对接顺序

| 顺序 | 前端功能/页面 | 接口范围 | 前置依赖 | 详细契约 |
| --- | --- | --- | --- | --- |
| 1 | 应用启动、登录、组织切换 | 健康检查、鉴权与组织（5） | 无 | [第 3、4 节](#3-健康检查) |
| 2 | 积分账户与流水页 | 积分（3） | 已登录；下发操作还需账户管理权限 | [第 5 节](#5-积分账户与流水) |
| 3 | 剧本列表、创建、详情与分集编辑 | 剧本与分集（12） | 已登录；团队项目须先切换活动组织 | [第 6 节](#6-剧本分集与项目成员) |
| 4 | 团队成员管理页 | 项目成员（4） | 团队项目；当前用户为 owner 或 manager | [第 6.3 节](#63-项目成员接口) |
| 5 | 文本分析进度、重新提取与失败重试 | AI 流水线（3） | 分集已保存；运行类操作需要 RUN_AI | [第 7 节](#7-剧本-ai-流水线) |
| 6 | 主体资产库、候选素材与审核 | 资产库（14） | 流水线已产出主体，或手工创建主体 | [第 8 节](#8-资产库与主体审核) |
| 7 | 创作画布与图片生成 | 画布、图片任务（9） | 已确保画布项目；生成需 RUN_AI 与积分 | [第 9、10 节](#9-创作画布) |
| 8 | 分镜编辑、镜头层级与视频工作台 | 分镜与工作台（11） | 分集、主体及审核素材已准备 | [第 11 节](#11-分镜任务与视频工作台) |
| 9 | 视频供应商设置与镜头组视频生成 | 视频生成（8） | 镜头组存在；管理员设置供应商；生成前检查素材 | [第 12 节](#12-视频生成) |
| 10 | 视频时间线、最终合成与播放 | 时间线（9）、媒体（2） | 视频任务成功；合成完成后通过媒体代理读取 | [第 13、14 节](#13-视频时间线与最终合成) |

### 0.2 跨功能规则

- 所有需要登录的调用都使用 Cookie 会话；接入方式见 [1.2 会话鉴权](#12-会话鉴权)。
- 创建、下发、生成、流水线和合成等写入型异步操作，先检查 [1.3 通用请求头](#13-通用请求头) 的 `Idempotency-Key` 要求。
- 分集、主体、画布和时间线的保存都需要回传版本字段；处理方式见 [1.6 并发控制字段](#16-并发控制字段)。
- 所有异步任务统一持久化 `job_id` 并轮询，状态与停止条件见 [15.2 推荐轮询器](#152-推荐轮询器)。

### 0.3 完整接口目录（80 个操作）

#### 应用入口与鉴权（5）

1. `GET /v3api/health` - 服务健康检查
2. `POST /v3api/auth/login` - 登录并建立 Cookie 会话
3. `POST /v3api/auth/logout` - 退出当前会话
4. `POST /v3api/auth/switch-org` - 切换活动组织
5. `GET /v3api/auth/me` - 获取当前用户与活动组织

#### 积分账户与流水（3）

6. `GET /v3api/credits/accounts` - 查询可访问账户
7. `GET /v3api/credits/logs` - 查询账户流水
8. `POST /v3api/credits/grant` - 下发积分

#### 剧本、分集与 AI 流水线（15）

9. `GET /v3api/scripts/page` - 分页查询剧本
10. `POST /v3api/scripts` - 创建剧本
11. `GET /v3api/scripts/news-broadcast/jobs` - 新闻播报任务占位查询
12. `GET /v3api/scripts/{script_id}` - 获取剧本
13. `PATCH /v3api/scripts/{script_id}` - 修改剧本标题
14. `DELETE /v3api/scripts/{script_id}` - 删除剧本
15. `POST /v3api/scripts/{script_id}/copy` - 复制剧本和分集文本
16. `GET /v3api/scripts/{script_id}/episodes` - 获取分集列表
17. `POST /v3api/scripts/{script_id}/episodes` - 创建分集
18. `PUT /v3api/scripts/{script_id}/episodes/{episode_id}` - 保存分集并按需触发流水线
19. `DELETE /v3api/scripts/{script_id}/episodes/{episode_id}` - 删除分集
20. `GET /v3api/scripts/{script_id}/episodes/{episode_id}/breakdown` - 获取分集拆解结果
21. `GET /v3api/scripts/{script_id}/pipeline-status` - 查询五阶段流水线状态
22. `GET /v3api/scripts/{script_id}/extract-subjects-status` - 查询主体提取与合并状态
23. `POST /v3api/scripts/{script_id}/episodes/{episode_id}/pipeline-runs` - 手工重提取或重试失败流水线

#### 团队项目成员（4）

24. `GET /v3api/scripts/{script_id}/members` - 查询项目成员
25. `POST /v3api/scripts/{script_id}/members` - 添加项目成员
26. `PATCH /v3api/scripts/{script_id}/members/{member_id}` - 修改成员角色
27. `DELETE /v3api/scripts/{script_id}/members/{member_id}` - 移除项目成员

#### 主体资产库与审核（14）

28. `GET /v3api/scripts/{script_id}/asset/entries` - 查询资产条目
29. `GET /v3api/scripts/{script_id}/asset-library/subjects` - 分页查询主体
30. `POST /v3api/scripts/{script_id}/asset-library/subjects` - 手工创建主体
31. `GET /v3api/scripts/{script_id}/asset-library/parent-subjects` - 查询父主体聚合
32. `PATCH /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}` - 修改主体
33. `DELETE /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}` - 删除主体
34. `GET /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions` - 查询主体差异建议
35. `POST /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions/{suggestion_id}/resolve` - 处理差异建议
36. `GET /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates` - 查询候选图片和音频
37. `POST /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-image` - 审核正式主体图片
38. `GET /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/replace-impact` - 预检查正式图片替换影响
39. `POST /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-audio` - 审核主体音频
40. `DELETE /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates/{candidate_id}` - 删除候选素材
41. `GET /v3api/scripts/{script_id}/canvas-project/ensure` - 获取或创建创作画布项目

#### 创作画布与图片生成（9）

42. `GET /v3api/materials/{material_id}` - 获取画布素材项目
43. `GET /v3api/materials/{material_id}/canvas` - 获取完整画布状态
44. `POST /v3api/materials/{material_id}/canvas/delta` - 提交画布增量
45. `GET /v3api/materials/{material_id}/image/{image_id}` - 获取图片资产元数据
46. `POST /v3api/generation-jobs` - 创建图片生成任务
47. `GET /v3api/generation-jobs` - 查询图片生成任务列表
48. `POST /v3api/generation-jobs/batch` - 批量创建图片生成任务
49. `GET /v3api/generation-jobs/{job_id}` - 查询图片生成任务状态
50. `POST /v3api/generation-jobs/{job_id}/cancel` - 取消图片生成任务

#### 分镜、镜头与视频工作台（11）

51. `POST /v3api/scripts/{script_id}/episodes/{episode_id}/generate-storyboard` - 创建分镜生成任务
52. `POST /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}/cancel` - 取消分镜任务
53. `GET /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard-jobs` - 查询分镜任务历史
54. `GET /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}` - 查询指定分镜任务
55. `PATCH /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}` - 编辑分镜任务结果
56. `POST /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}/retry` - 重试分镜任务
57. `GET /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard-status` - 查询最新分镜状态
58. `GET /v3api/scripts/{script_id}/episodes/{episode_id}/shot-hierarchy` - 查询镜头层级
59. `POST /v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/merge` - 将本集全部小分镜自动组合为正式镜头组
60. `GET /v3api/shots/{script_id}/episodes/{episode_id}/video-workbench` - 获取视频工作台
61. `GET /v3api/shots/{script_id}/cover` - 获取剧本封面

#### 视频供应商与视频生成（8）

62. `GET /v3api/video-generation/active-provider` - 查询当前视频供应商
63. `POST /v3api/video-generation/active-provider` - 更新当前视频供应商
64. `GET /v3api/video-generation/check-materials` - 检查镜头组素材完整性
65. `POST /v3api/video-generation-jobs` - 创建单个镜头组视频任务
66. `GET /v3api/video-generation-jobs` - 查询视频任务列表
67. `GET /v3api/video-generation/{job_id}` - 查询指定视频任务
68. `POST /v3api/video-generation/{job_id}/cancel` - 取消视频任务
69. `POST /v3api/video-generation-jobs/batch` - 批量创建视频任务

#### 视频时间线与最终合成（9）

70. `GET /v3api/video-timeline-arrangements` - 获取时间线
71. `POST /v3api/video-timeline-arrangements` - 向时间线添加片段
72. `PATCH /v3api/video-timeline-arrangements/{arrangement_id}` - 修改时间线片段
73. `DELETE /v3api/video-timeline-arrangements/{arrangement_id}` - 删除时间线片段
74. `PUT /v3api/video-timeline-arrangements/reorder` - 重排时间线片段
75. `POST /v3api/video-timeline-arrangements/render` - 创建最终视频合成任务
76. `GET /v3api/video-timeline-arrangements/render/{job_id}` - 查询合成任务状态
77. `POST /v3api/video-timeline-arrangements/render/{job_id}/cancel` - 取消合成任务
78. `POST /v3api/video-timeline-arrangements/render/{job_id}/retry` - 重试合成任务

#### 媒体上传、播放与下载（2）

79. `GET /api/x/cctv/zhihui/flow-app/{storage_key:path}` - 通过鉴权代理读取媒体
80. `POST /api/x/cctv/zhihui/flow-app/upload_images/{mid}/{filename}` - 上传图片素材

## 2026-07-22 镜头组合并接口

新增同步结构编辑接口：前端提交本集分镜生成结果中的全部镜头 ID，后端按场景、叙事连续性和 3～15 秒规则自动生成多个正式镜头组；不触发视频生成或积分扣费。详细契约见第 11 节。

## 2026-07-19 本轮接口更新清单

本轮仅有一项公开 HTTP 响应契约变更；其余稳定性修复不新增、删除或改变 HTTP operation。

| 接口/范围 | 旧风险 | 新保证 | 前端修改动作 | 兼容建议 |
| --- | --- | --- | --- | --- |
| `GET /v3api/materials/{material_id}/image/{image_id}` 的 `ImageAssetOut.storage_key` | 旧服务可能返回无前导 `/` 或多个前导 `/` 的 key；若前端直接拼接，可能生成错误代理 URL，若客户端各自规范化，又会造成规则分散。 | 当前服务端总是返回**恰好一个**前导 `/` 的合法 storage key，例如 `/upload_images/<mid>/<file>.png`。 | 直接以 `MEDIA_PROXY_PREFIX + storage_key` 拼接媒体代理前缀；当前端点返回值不得再自行添加、删除、折叠或替换斜杠。 | 先部署后端再发布依赖该保证的前端。若同一前端必须临时连接旧后端，应在明确的旧后端适配层处理，而不是在当前端点的通用调用处重复规范化。 |
| merge 稳定性修复（内部流水线实现） | 供应商将阶段固定的 `is_child` 错误输出为 JSON `false` 时，可能使 merge 任务失败。 | 服务端仅将精确布尔 `false` 归一为阶段不变量 `true`；缺失、字符串等非契约值仍由严格校验拒绝。 | 无需修改任何 HTTP 调用、字段解析或轮询逻辑。 | 既有接口路径、请求/响应字段和状态码均不变。 |

> **图片 URL 规则（本轮生效）**：此端点的 `storage_key` 已是以 `/` 开头的相对媒体路径。前端只能将其原样拼到媒体代理前缀之后，不能再执行前导斜杠判断、斜杠替换、补 `/` 或去 `/` 等二次处理。

## 1. 快速接入

### 1.1 地址与协议

| 项目 | 开发环境默认值 | 说明 |
| --- | --- | --- |
| HTTP 服务 | `http://localhost:8000` | 实际域名由部署环境决定 |
| 业务 API 前缀 | `/v3api` | 除媒体接口外的所有业务接口 |
| 媒体代理前缀 | `/api/x/cctv/zhihui/flow-app` | 图片、音频、视频上传及流式读取 |
| Swagger UI | `/v3api/docs` | 开发联调时可查看 |
| OpenAPI JSON | `/v3api/openapi.json` | 可用于生成前端类型或 API Client |
| 时间格式 | ISO 8601 字符串 | 示例：`2026-07-19T10:20:30Z` |
| ID 类型 | `string` | 当作不透明标识使用，不要在前端解析或自行构造 |

业务 URL 示例：

```text
GET http://localhost:8000/v3api/auth/me
GET http://localhost:8000/v3api/scripts/page?scope=personal&limit=30&offset=0
GET http://localhost:8000/api/x/cctv/zhihui/flow-app/generate_images/<mid>/<id>.png
```

### 1.2 会话鉴权

后端使用签名的 httpOnly Cookie 会话。登录成功后浏览器保存会话 Cookie；前端 JavaScript 不读取 Cookie，只需保证每次请求携带凭据。

```ts
const API_ORIGIN = import.meta.env.VITE_API_ORIGIN ?? "http://localhost:8000";

export class ApiError extends Error {
  constructor(
    public status: number,
    public detail: string,
  ) {
    super(detail);
  }
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  const isForm = init.body instanceof FormData;
  if (init.body && !isForm && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(`${API_ORIGIN}${path}`, {
    ...init,
    headers,
    credentials: "include",
  });

  if (response.status === 204) return undefined as T;
  const contentType = response.headers.get("content-type") ?? "";
  const body = contentType.includes("application/json")
    ? await response.json()
    : await response.text();
  if (!response.ok) {
    throw new ApiError(response.status, body?.detail ?? "请求失败");
  }
  return body as T;
}
```

跨域部署时必须同时满足：前端使用 `credentials: "include"`，后端 CORS 白名单包含前端 Origin，且服务端允许凭据。上传 `FormData` 时不要手动设置 `Content-Type`，浏览器会自动补充 boundary。

### 1.3 通用请求头

| 请求头 | 是否必填 | 用途 |
| --- | --- | --- |
| `Content-Type: application/json` | JSON 请求必填 | 普通请求体 |
| `Idempotency-Key` | 生成、下发、流水线等创建类接口按接口要求必填 | 防止网络重试重复创建任务或重复扣/冻积分 |
| `Range: bytes=...` | 媒体分段读取可选 | 浏览器视频/音频播放器通常自动发送 |

推荐每次用户动作生成一个随机幂等键，并将它与待重试请求一起保存；同一请求因超时重发必须复用原键，业务入参变化后必须换新键。

```ts
const idempotencyKey = crypto.randomUUID();
```

### 1.4 通用错误契约

除媒体二进制成功响应外，错误体统一为：

```json
{ "detail": "错误说明" }
```

| 状态码 | 含义 | 前端处理建议 |
| --- | --- | --- |
| `400` | 请求在业务上不合法 | 展示 `detail`，修正输入后再提交 |
| `401` | 未登录、会话过期、用户禁用或活动组织失效 | 保留本地草稿，引导重新登录 |
| `402` | 积分不足或缺少有效积分账户 | 打开积分提示/充值或联系管理员 |
| `403` | 已登录但无资源或动作权限 | 显示无权限页，不循环重试 |
| `404` | 资源不存在 | 刷新列表或返回上一级 |
| `409` | 版本、状态、幂等键或并发冲突 | 先重新拉取权威数据，再决定合并或重试 |
| `413` | 上传文件或无 Range 媒体读取超出上限 | 提示压缩文件，或使用播放器的 Range 读取 |
| `416` | 媒体 Range 不可满足 | 让播放器重新建立请求，不要继续旧 Range |
| `422` | 字段校验失败 | Schema 校验统一返回 `{"detail":"字段校验失败"}`；少数业务主动抛出的 422 会返回更具体 `detail` |
| `500` | 服务器内部错误 | 提示稍后重试并记录请求上下文，勿展示堆栈 |

### 1.5 内容权限模型

团队项目同时校验：当前活动组织、组织成员关系、显式项目成员关系和具体动作权限。组织管理员不会自动获得项目内容权限。

| 角色 | 读 | 编辑 | 运行 AI | 审核资产 | 管理成员 | 删除项目 |
| --- | --- | --- | --- | --- | --- | --- |
| `owner` | 是 | 是 | 是 | 是 | 是 | 是 |
| `manager` | 是 | 是 | 是 | 是 | 是 | 是 |
| `editor` | 是 | 是 | 是 | 由部署配置决定，默认否 | 否 | 否 |
| `reviewer` | 是 | 否 | 否 | 是 | 否 | 否 |
| `viewer` | 是 | 否 | 否 | 否 | 否 | 否 |

个人项目只有 `owner_user_id` 对应的所有者可以访问，所有者拥有该项目的全部动作权限。

### 1.6 并发控制字段

| 资源 | 读取字段 | 写入时必须回传 | 冲突处理 |
| --- | --- | --- | --- |
| 分集 | `episode_version`、`client_save_seq` | 两者均必填 | `409` 后重新加载分集并处理文本冲突 |
| 镜头组合并 | `episode_version` | Body `expected_episode_version` | `409` 后重新获取 hierarchy/workbench；若工作台已有正式组则视为已完成，否则以新版本重提全部 ID |
| 主体 | `subject_version` | `expected_subject_version` | `409` 后刷新主体与待处理建议 |
| 画布 | `version` | Delta 中的 `version` | `409` 后重新获取完整画布再合并本地变更 |
| 时间线片段 | `version` | `expected_version` | `409` 后重新获取完整时间线 |
| 正式主体图片 | `image_entry.asset_id` | 替换时传 `expected_image_entry_id` | `409` 后刷新正式图和候选列表 |

不要在收到 `409` 后原样无限重放旧请求。`409` 表示客户端所依据的状态已经过期，必须先读后写。

## 2. 前端完整业务流程

```mermaid
flowchart LR
    A["登录/选择组织"] --> B["创建剧本与分集"]
    B --> C["保存文本并运行五阶段流水线"]
    C --> D["维护主体与处理差异建议"]
    D --> E["创建画布并生成主体图片"]
    E --> F["审核图片/音频资产"]
    F --> G["按分集生成分镜"]
    G --> H["读取视频工作台并检查素材"]
    H --> I["生成镜头组视频"]
    I --> J["加入时间线、排序与裁切"]
    J --> K["异步合成最终视频"]
    K --> L["通过鉴权媒体代理播放/下载"]
```

前端应把所有异步任务的 `job_id` 持久化在页面状态或本地恢复数据中。页面刷新后重新查询服务端状态，不要只依赖内存中的“生成中”标记。

## 3. 健康检查

### `GET /v3api/health`

- 鉴权：无。
- 请求：无。
- 成功：`200`。

```json
{ "status": "ok", "env": "dev" }
```

`env` 是服务端环境标识；前端不应据此开启管理员功能或安全策略。

## 4. 鉴权与组织

### 4.1 接口清单

| 方法与路径 | 鉴权 | 入参 | 成功响应 | 主要错误/交互 |
| --- | --- | --- | --- | --- |
| `POST /v3api/auth/login` | 无 | JSON `LoginIn` | `200 UserOut`，并写 Cookie | 用户不存在、禁用、密码错均统一 `401` |
| `POST /v3api/auth/logout` | 无 | 无 | `200 {detail:"已退出"}` | 可按幂等退出处理 |
| `GET /v3api/auth/me` | 登录 | 无 | `200 UserOut` | `401` 时清理前端敏感缓存并转登录 |
| `POST /v3api/auth/switch-org` | 登录 | JSON `SwitchOrgIn` | `200 UserOut` | 企业不存在 `404`；非成员 `403` |

### 4.2 入参与出参字段

`LoginIn`

| 字段 | 类型 | 必填 | 约束 | 含义 |
| --- | --- | --- | --- | --- |
| `username` | `string` | 是 | 1–128 字符 | 登录名 |
| `password` | `string` | 是 | 1–256 字符 | 密码；不要写入日志或持久化明文 |

`SwitchOrgIn`

| 字段 | 类型 | 必填 | 约束 | 含义 |
| --- | --- | --- | --- | --- |
| `org_id` | `string` | 是 | 1–36 字符 | 要切换到的企业 ID，必须来自 `UserOut.organizations` |

`UserOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `user_id` | `string` | 用户 ID |
| `username` | `string` | 用户名 |
| `enabled` | `boolean` | 账号是否启用 |
| `is_admin` | `boolean` | 是否平台/系统管理员；不等同于项目成员权限 |
| `account_type` | `string` | 通常为 `personal` 或 `team` |
| `org_id` | `string \| null` | 用户主企业 ID |
| `department_id` | `string \| null` | 用户部门 ID |
| `can_manage_accounts` | `boolean` | 是否具备积分账户管理能力 |
| `management_scope` | `string` | `none`、`department` 或 `org` |
| `organizations` | `OrgOut[]` | 用户可访问企业列表 |
| `active_org_id` | `string \| null` | 当前会话选中的活动企业 |

`OrgOut`：`id` 为企业 ID，`org_name` 为展示名，`org_code` 为企业编码。

登录示例：

```ts
const user = await api<UserOut>("/v3api/auth/login", {
  method: "POST",
  body: JSON.stringify({ username, password }),
});
```

切换组织成功后，应清空或重新请求团队剧本、项目成员、积分账户等带租户上下文的数据。

## 5. 积分账户与流水

### 5.1 接口清单

| 方法与路径 | 权限 | 入参 | 成功响应 | 主要错误/交互 |
| --- | --- | --- | --- | --- |
| `GET /v3api/credits/accounts` | 登录 | 无 | `200 {items: AccountOut[]}` | 返回当前用户可访问的个人/部门/企业账户 |
| `GET /v3api/credits/logs` | 账户读权限 | Query `account_id`、`limit`、`offset`、`log_type?` | `200 {items: LogOut[]}` | 无 `total`；按时间倒序 |
| `POST /v3api/credits/grant` | 账户管理权限 | Header `Idempotency-Key`；JSON `GrantIn` | `200 {detail:"下发成功"}` | 越权 `403`、余额不足 `402`、幂等冲突 `409` |

`GET /credits/logs` 查询参数：

| 参数 | 类型 | 必填 | 默认/约束 | 含义 |
| --- | --- | --- | --- | --- |
| `account_id` | `string` | 是 | — | 要查询的账户 |
| `limit` | `integer` | 否 | 默认 50，1–500 | 本页条数 |
| `offset` | `integer` | 否 | 默认 0，≥0 | 偏移量 |
| `log_type` | `string` | 否 | 常用 `grant`、`settle`、`refund` | 流水类型过滤；未知值返回空列表 |

`POST /credits/grant` 请求：

| 字段/头 | 类型 | 必填 | 约束 | 含义 |
| --- | --- | --- | --- | --- |
| `Idempotency-Key` | Header `string` | 是 | 1–36 字符 | 同一转账重试复用同一键 |
| `from_account_id` | `string` | 是 | — | 积分来源账户 |
| `to_account_id` | `string` | 是 | — | 积分目标账户 |
| `points` | `integer` | 是 | >0 | 下发数量 |
| `note` | `string \| null` | 否 | — | 备注 |

### 5.2 `AccountOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | `string` | 账户 ID |
| `owner_type` | `string` | `user`、`org`、`department` |
| `owner_id` | `string` | 归属用户/企业/部门 ID |
| `org_id` | `string \| null` | 所属企业 |
| `department_id` | `string \| null` | 所属部门 |
| `balance_points` | `integer` | 账面余额 |
| `frozen_points` | `integer` | 正在执行任务占用的冻结积分 |
| `status` | `string` | `active` 或 `disabled` |
| `disabled_reason` | `string \| null` | 禁用原因 |
| `disabled_at` | `datetime \| null` | 禁用时间 |
| `created_at` / `updated_at` | `datetime` | 创建/更新时间 |

前端展示“可用积分”时使用 `balance_points - frozen_points`。

### 5.3 `LogOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | `string` | 流水 ID |
| `account_id` | `string` | 账户 ID |
| `actor_user_id` / `actor_username` | 可空 | 操作者 |
| `target_user_id` / `target_username` | 可空 | 目标用户；组织/部门目标可能为空 |
| `target_owner_type` / `target_owner_id` | 可空 | 非用户目标的归属类型与 ID |
| `reservation_id` | `string \| null` | 预冻结、结算、释放的关联预约 |
| `source_type` / `source_id` | 可空 | 业务来源及任务/资源 ID |
| `log_type` | `string` | `reserve`、`settle`、`release`、`grant`、`refund` |
| `points_delta` | `integer` | 余额变化，消耗通常为负 |
| `frozen_delta` | `integer` | 冻结变化 |
| `balance_after` / `frozen_after` | `integer` | 变更后的余额/冻结值 |
| `note` | `string \| null` | 备注 |
| `created_at` | `datetime` | 流水时间 |

## 6. 剧本、分集与项目成员

### 6.1 剧本接口

| 方法与路径 | 权限 | 入参 | 成功响应 | 主要错误/交互 |
| --- | --- | --- | --- | --- |
| `GET /v3api/scripts/page` | 登录 | Query `scope=personal|team`、`limit=30`、`offset=0` | `ScriptPageOut` | team 只返回当前组织且有显式成员关系的项目 |
| `POST /v3api/scripts` | 登录 | `ScriptCreateIn` | `201 ScriptOut` | team 项目要求已选择活动企业 |
| `GET /v3api/scripts/news-broadcast/jobs` | 登录 | 无 | `200 []` | 当前占位能力，固定空数组 |
| `GET /v3api/scripts/{script_id}` | READ | 路径 ID | `ScriptOut` | 不存在 `404`、无权 `403` |
| `PATCH /v3api/scripts/{script_id}` | EDIT | `ScriptUpdateIn` | `ScriptOut` | 修改标题 |
| `POST /v3api/scripts/{script_id}/copy` | EDIT | 可省略 `ScriptCopyIn` | `201 ScriptOut` | 只复制剧本和分集文本，不复制派生资产/任务/媒体 |
| `DELETE /v3api/scripts/{script_id}` | DELETE | Query `confirm=false` | `LifecycleDeleteOut` | 活动任务不能强删；有依赖需确认后重试 |

请求 Schema：

| Schema | 字段 | 约束与含义 |
| --- | --- | --- |
| `ScriptCreateIn` | `title` | 必填，1–512 字符 |
|  | `scope` | 可选，`personal|team`，默认 `personal` |
| `ScriptUpdateIn` | `title` | 必填，1–512 字符 |
| `ScriptCopyIn` | `title` | 可选；缺省沿用来源标题 |
|  | `scope` | 可选；缺省沿用来源范围 |

`ScriptOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | `string` | 剧本 ID |
| `title` | `string` | 标题 |
| `scope` | `personal \| team` | 个人/团队项目 |
| `owner_user_id` | `string` | 所有者用户 ID |
| `org_id` | `string \| null` | 团队所属企业；个人项目通常为空 |
| `created_by_user_id` | `string` | 创建者 |
| `created_at` / `updated_at` | `datetime` | 创建/更新时间 |

`ScriptListItem` 在 `ScriptOut` 基础上增加 `owner_username`、`current_user_project_role`，列表项不返回 `created_at`。`ScriptPageOut` 为 `{items: ScriptListItem[], total: number}`。

复制团队剧本到另一范围时，只有 `owner`/`manager` 可以执行；复制到 team 仍需要有效活动企业。

### 6.2 分集接口

| 方法与路径 | 权限 | 入参 | 成功响应 | 主要错误/交互 |
| --- | --- | --- | --- | --- |
| `GET /v3api/scripts/{script_id}/episodes` | READ | 路径 ID | `EpisodeListItem[]` 裸数组 | 列表包装规则的例外 |
| `POST /v3api/scripts/{script_id}/episodes` | EDIT | `EpisodeCreateIn` | `201 EpisodeListItem` | 集号重复 `409` |
| `PUT /v3api/scripts/{script_id}/episodes/{episode_id}` | EDIT；写文本还需 RUN_AI | `EpisodeSaveIn` | `EpisodeListItem` | 双版本冲突 `409`；文本非 null 时自动触发流水线 |
| `DELETE /v3api/scripts/{script_id}/episodes/{episode_id}` | EDIT | Query `confirm=false` | `LifecycleDeleteOut` | 活动任务不可删除；派生资源需二次确认 |
| `GET /v3api/scripts/{script_id}/episodes/{episode_id}/breakdown` | READ | 路径 ID | `BreakdownOut` | 返回分集、主体、镜头组、镜头快照 |

`EpisodeCreateIn`

| 字段 | 类型 | 必填 | 约束/含义 |
| --- | --- | --- | --- |
| `title` | `string` | 是 | 1–255 字符 |
| `script_text` | `string \| null` | 否 | 初始剧本文本 |
| `episode_no` | `integer \| null` | 否 | ≥1；缺省取当前最大集号 + 1 |

`EpisodeSaveIn`

| 字段 | 类型 | 必填 | 含义 |
| --- | --- | --- | --- |
| `script_text` | `string \| null` | 否 | 只有非 `null` 才写入并触发流水线；传 `null` 不表示清空 |
| `title` | `string \| null` | 否 | 新标题 |
| `episode_no` | `integer \| null` | 否 | 新集号，≥1 |
| `episode_version` | `integer` | 是 | 当前内容版本 |
| `client_save_seq` | `integer` | 是 | 当前客户端保存序号 |

`EpisodeListItem`：`episode_id`、`script_id`、`episode_no`、`title`、`episode_version`、`client_save_seq`、`updated_at`。保存成功后必须用响应中的两个新版本值覆盖本地值。

`BreakdownOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `episode` | `EpisodeOut` | 完整分集，额外含 `script_text`、`script_file_storage_key`、`schema_version`、`created_at` |
| `subjects` | `BreakdownSubject[]` | `{id,type,name,description,is_child,episode_nos}`；type 为 `character|scene|prop|sound` |
| `shot_groups` | `BreakdownShotGroup[]` | `{shot_group_id,scene_no,group_no,prompt}` |
| `shots` | `BreakdownShot[]` | `{shot_id,scene_no,shot_group_id,group_no,content}` |

删除响应 `LifecycleDeleteOut`：

```ts
interface LifecycleDeleteOut {
  detail: string;
  dependencies: {
    episode_count: number;
    subject_count: number;
    asset_count: number;
    shot_group_count: number;
    shot_count: number;
    video_count: number;
    job_count: number;
    active_job_count: number;
    canvas_count: number;
  };
}
```

推荐删除交互：先以 `confirm=false` 调用；若 `409` 表示存在依赖，展示影响并由用户确认后改为 `confirm=true`。`active_job_count > 0` 时即使确认也不能删除。

当前 `409` 错误仍使用通用 `{detail}`，依赖计数被拼入提示字符串；结构化 `dependencies` 只在删除成功响应中返回。前端不要在 409 上强行按 `LifecycleDeleteOut` 解包。

### 6.3 项目成员接口

所有成员接口仅 team 项目的 `owner`/`manager` 可调用；个人项目返回 `400`。

| 方法与路径 | 入参 | 成功响应 | 主要规则 |
| --- | --- | --- | --- |
| `GET /v3api/scripts/{script_id}/members` | 路径 ID | `{items: ProjectMemberOut[], total}` | 列出项目成员 |
| `POST /v3api/scripts/{script_id}/members` | `ProjectMemberCreateIn` | `201 ProjectMemberOut` | 目标必须是当前组织有效用户；重复成员 `409` |
| `PATCH /v3api/scripts/{script_id}/members/{member_id}` | `ProjectMemberRoleUpdateIn` | `ProjectMemberOut` | 所有者保护、最后一位管理者保护 |
| `DELETE /v3api/scripts/{script_id}/members/{member_id}` | 无 | `{detail:"已移除"}` | 不可移除真实所有者或最后一位 owner/manager |

`ProjectMemberCreateIn`：`user_id` 必填、1–36 字符；`project_role` 默认为 `viewer`。`ProjectMemberRoleUpdateIn` 只含必填 `project_role`。

`project_role` 枚举：`owner | manager | editor | reviewer | viewer`。

`ProjectMemberOut`：`member_id`、`user_id`、`username`、`project_role`、`enabled`。

只有真实剧本所有者可以保持 `owner` 身份；非所有者不能被提升为 `owner`，真实所有者也不能被降级或移除。

## 7. 剧本 AI 流水线

### 7.1 状态查询接口

| 方法与路径 | 权限 | 成功响应 | 用途 |
| --- | --- | --- | --- |
| `GET /v3api/scripts/{script_id}/pipeline-status` | READ | `PipelineStatusOut` | 展示五阶段总体状态 |
| `GET /v3api/scripts/{script_id}/extract-subjects-status` | READ | `ExtractSubjectsStatusOut` | 展示逐集提取及 merge 状态 |

`PipelineStatusOut`

```ts
interface PipelineStatusOut {
  stages: Array<{
    stage: "split" | "extract" | "merge" | "enrich" | "parents";
    label: string;
    status: "pending" | "running" | "done" | "failed";
    detail: string;
    current: number | null;
    total: number | null;
  }>;
  running: boolean;
  failed: boolean;
  all_done: boolean;
}
```

阶段依赖是：`split → extract → merge → (enrich 与 parents)`。最后两个阶段可按 DAG 独立推进，不要仅按数组前后顺序推断严格串行。

`ExtractSubjectsStatusOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `total_episodes` | `integer` | 分集总数 |
| `extracted_episodes` | `integer` | 已成功提取集数 |
| `all_extracted` | `boolean` | 是否全部成功提取 |
| `episodes` | `ExtractEpisodeStatus[]` | 每集 `{episode_id,episode_no,title,extract_status,extracted}` |
| `merge_status` | `string` | `pending|running|succeeded|failed` |
| `merge_job_id` | `string \| null` | 最新 merge 任务 ID |
| `has_active_merge` | `boolean` | merge 是否正在运行 |

五阶段接口的完成态是 `done`；提取状态接口将 merge 的 `done` 映射为 `succeeded`。前端类型不要把两个接口的完成字符串混用。

### 7.2 人工重新提取/失败重试

### `POST /v3api/scripts/{script_id}/episodes/{episode_id}/pipeline-runs`

- 权限：RUN_AI。
- Header：`Idempotency-Key` 必填，1–128 字符。
- 成功：`202 Accepted`。

`PipelineRunCreateIn`

| 字段 | 类型 | 必填 | 规则 |
| --- | --- | --- | --- |
| `mode` | `manual_reextract \| retry_failed` | 是 | 手动重新提取或失败重试 |
| `episode_version` | `integer` | 是 | ≥1，必须是当前版本 |
| `client_save_seq` | `integer` | 是 | ≥0，必须是当前保存序号 |
| `failed_run_id` | `string \| null` | 条件必填 | 1–36 字符；`retry_failed` 必填；`manual_reextract` 禁止传 |

`PipelineRunOut`

| 字段 | 含义 |
| --- | --- |
| `script_id` / `episode_id` | 资源归属 |
| `run_id` | 流水线运行 ID |
| `dispatch_id` | 异步投递记录 ID |
| `reservation_id` | 积分预冻结预约 ID |
| `mode` | 运行模式 |
| `retry_of_run_id` | 被重试的历史运行，首次手动运行为空 |
| `attempt` | 尝试次数 |
| `input_version` / `input_client_save_seq` | 本次固化的输入版本 |
| `estimated` | 预估冻结积分 |
| `status` | 首次通常 `pending`；幂等重放可能返回已有状态 |
| `idempotent_replay` | 是否为安全幂等重放 |

当前没有按 `run_id` 查询详情的公开接口。收到 `202` 后使用两个状态查询接口观察总体进度。

## 8. 资产库与主体审核

### 8.1 接口清单

| 方法与路径 | 权限 | 入参 | 成功响应 | 主要错误/交互 |
| --- | --- | --- | --- | --- |
| `GET /v3api/scripts/{script_id}/asset/entries` | READ | Query `limit=30`、`offset=0` | `AssetEntryListOut` | 可用 `limit=1` 只取 `total` |
| `GET /v3api/scripts/{script_id}/asset-library/subjects` | READ | Query `limit=30`、`offset=0` | `SubjectListOut` | 主体分页 |
| `POST /v3api/scripts/{script_id}/asset-library/subjects` | EDIT | `SubjectCreateIn` | `201 SubjectAssetOut` | 重名、无效集号等按业务错误返回 |
| `GET /v3api/scripts/{script_id}/asset-library/parent-subjects` | READ | 无 | `ParentSubjectListOut` | 父主体聚合列表，无 `total` |
| `PATCH /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}` | EDIT | `SubjectUpdateIn` | `SubjectAssetOut` | 推荐带主体版本，冲突 `409` |
| `GET /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions` | READ | Query `status` | `SubjectChangeSuggestionListOut` | 人工锁字段的流水线差异建议 |
| `POST /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions/{suggestion_id}/resolve` | EDIT | `SubjectSuggestionResolveIn` | `SubjectChangeSuggestionOut` | 版本过期或已处理 `409` |
| `DELETE /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}` | EDIT | 无 | `{detail:"已删除"}` | 活动生成/清算依赖可能 `409` |
| `GET /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates` | READ | 无 | `CandidateListOut` | 图片与音频候选分开返回 |
| `POST /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-image` | REVIEW | `ApproveImageIn` | `ApproveResultOut` | 正式图并发与影响确认 |
| `GET /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/replace-impact` | REVIEW | 无 | `ImpactSummaryOut` | 替换正式图前的影响预检查 |
| `POST /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-audio` | REVIEW | `ApproveAudioIn` | `SubjectAssetOut` | 音频为追加式审核 |
| `DELETE /v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates/{candidate_id}` | REVIEW | 无 | `SubjectAssetOut` | 已批准资产不能按候选删除 |
| `GET /v3api/scripts/{script_id}/canvas-project/ensure` | EDIT | 无 | `CanvasProjectEnsureOut` | 幂等获取或创建画布项目 |

列表接口的 `limit` 范围均为 1–500，默认 30；`offset` 默认 0 且不得小于 0。

### 8.2 主体入参

`SubjectCreateIn`

| 字段 | 类型 | 必填 | 约束/含义 |
| --- | --- | --- | --- |
| `type` | `character|background|scene|prop|sound` | 是 | `scene` 与 `background` 同义 |
| `name` | `string` | 是 | trim 后 1–255 字符 |
| `description` | `string` | 是 | trim 后 1–10,000 字符 |
| `episode_nos` | `integer[]` | 是 | 1–1000 项；正整数、唯一、必须属于当前剧本 |

`SubjectUpdateIn` 全部业务字段可选：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `name` | `string \| null` | 新名称；人工修改时服务端默认锁定该字段 |
| `description` | `string \| null` | 新描述；人工修改时服务端默认锁定该字段 |
| `type` | 枚举或 `null` | 新主体类型 |
| `episode_nos` | `integer[] \| null` | 新出现集号，至少一项且唯一 |
| `name_locked` | `boolean \| null` | 是否禁止流水线直接覆盖名称 |
| `description_locked` | `boolean \| null` | 是否禁止流水线直接覆盖描述 |
| `expected_subject_version` | `integer \| null` | 推荐必传的乐观锁版本，≥1 |

资产库出参会把 `scene` 统一规范为 `background`，且 `subject_type_label` 为“场景”。前端展示层建议也统一用“场景”，避免出现两套文案。

### 8.3 `SubjectAssetOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `subject_id` | `string` | 主体 ID |
| `subject_name` | `string` | 名称 |
| `subject_type` | `string` | 规范化类型；场景输出为 `background` |
| `subject_type_label` | `string` | 中文类型标签 |
| `subject_description` | `string \| null` | 设计描述/提示词基础 |
| `name_locked` / `description_locked` | `boolean` | 人工锁定标识 |
| `subject_version` | `integer` | 主体乐观锁版本 |
| `episodes` | `string[]` | 兼容字段：把出现集号转成字符串，例如 `["1","2"]` |
| `episode_ids` | `string[]` | 按集号映射得到的真实分集 ID |
| `episode_nos` | `integer[]` | 出现的集号，例如 `[1,2]` |
| `parent_id` / `parent_name` | 可空 | 父主体归属 |
| `is_parent` | `boolean` | 是否父主体 |
| `review_status` | `string` | `not_designed`、`pending`、`approved` |
| `review_status_label` | `string` | 中文审核标签 |
| `pending_candidate_count` | `integer` | 待审候选数量 |
| `image_entry` | `AssetEntryOut \| null` | 当前正式图片 |
| `audio_entries` | `AssetEntryOut[]` | 已通过声音资产，可多条 |
| `whitelist_status` | `string \| null` | 白名单兼容状态 |
| `cover_url` | `string \| null` | 确定性占位封面，可能是 data URL |
| `created_at` / `updated_at` | `datetime \| null` | 时间 |

`ParentSubjectOut`：`subject_id`、`name`、`type`、`description`、`child_count`、`created_at`、`updated_at`。

### 8.4 资产与候选字段

`AssetEntryOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `asset_id` | `string` | 资产 ID |
| `storage_key` | `string` | 经媒体代理访问的存储键 |
| `mime_type` | `string` | 真实/已验证 MIME |
| `size_bytes` | `integer` | 字节数 |
| `source` | `string` | 常见 `generate_images`、`upload_images` |
| `sha256` | `string \| null` | 内容摘要 |
| `kind` | `string` | `image` 或 `audio` |
| `status` | `string` | 常见 `candidate`、`approved`、`superseded` |
| `is_approved` | `boolean` | 是否正式通过 |
| `superseded` | `boolean` | 是否已被新版正式资产替换 |
| `superseded_by_id` | `string \| null` | 替换它的新资产 ID |
| `created_at` / `updated_at` | `datetime \| null` | 时间 |

`CandidateOut` 字段为：`candidate_id`、`storage_key`、`mime_type`、`size_bytes`、`source`、`sha256`、`kind`、`status`。候选列表响应：

```ts
interface CandidateListOut {
  image_candidates: CandidateOut[];
  audio_candidates: CandidateOut[];
}
```

### 8.5 差异建议

查询参数 `status` 可取 `pending | accepted | ignored | all`，默认 `pending`。

`SubjectChangeSuggestionOut`

| 字段 | 含义 |
| --- | --- |
| `suggestion_id` | 建议 ID |
| `subject_id` | 主体 ID |
| `field` | `name` 或 `description` |
| `current_value` / `suggested_value` | 当前值与流水线建议值 |
| `base_subject_version` | 生成建议时依据的主体版本 |
| `source_run_id` | 来源流水线运行，可空 |
| `status` | `pending`、`accepted`、`ignored` |
| `resolved_by_user_id` / `resolved_at` | 处理人/处理时间 |
| `created_at` / `updated_at` | 时间 |

处理请求：

```json
{
  "action": "accept",
  "expected_subject_version": 3
}
```

接受会把建议写入主体并递增主体版本；忽略只改变建议状态。两种操作都必须使用当前主体版本。

### 8.6 图片与音频审核

`ApproveImageIn`

| 字段 | 必填 | 含义 |
| --- | --- | --- |
| `candidate_id` | 是 | 要通过的图片候选 |
| `expected_image_entry_id` | 替换时应必填 | 客户端当前看到的正式图 ID；首次审核可为空 |
| `confirm` | 否 | 默认 `false`；是否确认替换正在被镜头引用的正式图 |

`ApproveResultOut` 为 `{subject: SubjectAssetOut, impact: ImpactSummaryOut | null}`。

`ImpactSummaryOut`

| 字段 | 含义 |
| --- | --- |
| `referenced` | 当前正式图是否被镜头组引用 |
| `shot_group_count` | 受影响镜头组数量 |
| `details` | 影响项数组 |

每个影响项：`shot_group_id`、`scene_no`、`group_no`、`episode_id`、`materials_category`。

推荐图片替换流程：

1. 获取主体和候选，保留当前 `image_entry.asset_id`。
2. 可先调用 `replace-impact` 展示影响。
3. 调用 `approve-image`，携带 `expected_image_entry_id` 和 `confirm:false`。
4. 若响应 `impact.referenced=true` 且主体正式图未变化，弹出二次确认。
5. 用户确认后以同一候选、最新正式图 ID、`confirm:true` 再提交。
6. `409` 时刷新主体和候选，禁止盲目覆盖。

音频审核请求仅为 `{candidate_id}`，成功直接返回最新主体。音频是追加式，不走正式图替换影响确认。

删除主体会连带清理其资产条目。若存在 `starting/processing` 图片任务或待处理财务清算，服务端返回 `409`；前端应先展示不可恢复提示并刷新相关任务状态。

### 8.7 画布 Ensure

`GET /v3api/scripts/{script_id}/canvas-project/ensure` 幂等返回：

| 字段 | 含义 |
| --- | --- |
| `materials_id` | 画布项目 ID |
| `name` | 画布名称 |
| `script_id` | 剧本 ID |
| `created` | 本次是否新建；已有画布时为 `false` |

该接口虽然使用 GET，但在画布不存在时会创建数据；不要把它放入路由预取、爬虫预取或无意触发的 hover prefetch，只在用户明确进入创作页时调用。

## 9. 创作画布

### 9.1 接口清单

| 方法与路径 | 权限 | 入参 | 成功响应 |
| --- | --- | --- | --- |
| `GET /v3api/materials/{material_id}` | READ | 路径 ID | `MaterialsOut` |
| `GET /v3api/materials/{material_id}/canvas` | READ | 路径 ID | `CanvasStateOut` |
| `POST /v3api/materials/{material_id}/canvas/delta` | EDIT | `CanvasDeltaIn` | 最新 `CanvasStateOut` |
| `GET /v3api/materials/{material_id}/image/{image_id}` | READ | 路径 ID | `ImageAssetOut` |

`MaterialsOut`：`id`、`name`、`script_id`、`version`、`created_at`、`updated_at`。

`ImageAssetOut`：`image_id`、`storage_key`、`mime_type`、`size_bytes`、`source`。

其中 `storage_key` 保证为**恰好一个前导 `/`** 的合法相对媒体路径。实际图片 URL 必须经媒体代理拼接，不能把 `storage_key` 当作公开对象存储 URL，也不能由前端二次增删斜杠：

```ts
const MEDIA_PROXY_PREFIX = "/api/x/cctv/zhihui/flow-app";

function imageAssetUrl(storageKey: string): string {
  // GET /materials/{material_id}/image/{image_id} 已保证 storageKey 恰有一个前导 /。
  return `${MEDIA_PROXY_PREFIX}${storageKey}`;
}
```

### 9.2 画布状态

```ts
interface CanvasStateOut {
  material_id: string;
  nodes: CanvasNodeOut[];
  edges: CanvasEdgeOut[];
  version: number;
}

interface CanvasNodeOut {
  id: string;
  type: "imageUpload" | "text" | "generated" | "uploaded" | "storyboard" | "assets";
  position: Record<string, unknown> | null;
  data: Record<string, unknown> | null;
}

interface CanvasEdgeOut {
  id: string;
  source: string;
  target: string;
  data: Record<string, unknown> | null;
}
```

画布基于 React Flow 风格的节点和边。`position` 至少应包含前端所需的 `x`、`y`；服务端以 JSON 对象保存。

### 9.3 Delta 入参

```ts
interface CanvasDeltaIn {
  move_nodes: Array<{ id: string; position: Record<string, unknown> }>;
  upsert_nodes: Array<{
    id: string;
    type: CanvasNodeOut["type"];
    position?: Record<string, unknown> | null;
    data: Record<string, unknown>;
  }>;
  delete_nodes: string[];
  upsert_edges: Array<{
    id?: string | null;
    source: string;
    target: string;
    data?: Record<string, unknown> | null;
  }>;
  delete_edges: string[];
  version: number;
}
```

数组字段均可省略并由后端视为空数组，但 `version` 必填且 ≥0。新增/更新边的 `source` 和 `target` 必须是同一画布已存在节点；删除节点会同步删除引用它的边。

### 9.4 节点 `data` 规则

| 节点类型 | 必需字段 | 说明 |
| --- | --- | --- |
| `text` | 非空 `content` | 文本节点 |
| `storyboard` | 非空 `shotGroupId`、`prompt` | 分镜节点 |
| `generated` | 非空 `prompt`、合法 `model` | 已配置生成参数节点 |
| `uploaded` | `assetId` 或 `imageUrl` 至少一个 | 上传资产节点 |
| `imageUpload` | `prompt`，或完整 `subjectName + subjectType + subjectDescription` | 主体设计图节点 |
| `assets` | 主体图片数据 | 与图片节点共享扩展模型 |

主体图片节点常用 camelCase 字段：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `title` | `string \| null` | 节点标题 |
| `variant` | `generated|uploaded|text|storyboard|null` | 展示变体 |
| `imageUrl` | `string \| null` | 前端预览 URL |
| `prompt` | `string \| null` | 生成提示词 |
| `model` | 图片模型或 `null` | 模型名称 |
| `size` / `aspect` | `string \| null` | 尺寸和比例 |
| `isGenerating` | `boolean \| null` | UI 生成中标记 |
| `generationJobId` | `string \| null` | 关联生图任务 |
| `subjectId` / `subjectName` | 可空 | 主体 ID/名称 |
| `subjectType` | 可空枚举 | `character|scene|background|prop|sound` |
| `subjectDescription` | `string \| null` | 主体描述 |
| `subjectEpisodeIds` | `string[] \| null` | 关联分集 ID |
| `subjectEpisodeNos` | `number[] \| null` | 关联集号 |
| `subjectDesignStatus` | `string \| null` | 设计状态 |
| `promptVariationIndex` | `number \| null` | 提示词变体序号 |
| `imagesExpanded` | `boolean \| null` | 多图展开状态 |
| `generatingStartedAt` | `number \| null` | 前端毫秒时间戳 |
| `assetId` | `string \| null` | 主资产 ID |
| `imageOutputs` | `ImageOutputData[] \| null` | 生成结果 |
| `primaryImageIndex` | `number \| null` | 主选图索引 |

图片模型枚举：`banana-2 | gpt-image-2 | wan2.1-t2i-plus | wan2.1-t2i-turbo | wanx-v1 | qwen-image`。

`ImageOutputData`：`status` 必填，`assetId`、`storageKey` 可空。

服务端允许未建模的扩展字段，但核心字段仍按节点类型验证。对于绑定 `subjectId` 的主体节点，服务端会用可信主体信息回填关键字段，前端应以返回状态为准。

### 9.5 乐观保存流程

```ts
const latest = await api<CanvasStateOut>(
  `/v3api/materials/${materialId}/canvas/delta`,
  {
    method: "POST",
    body: JSON.stringify({
      move_nodes: [{ id: nodeId, position: { x, y } }],
      version: canvas.version,
    }),
  },
);
```

保存成功后立即替换本地完整 `CanvasStateOut`。若返回 `409`，重新 GET 画布，按节点 ID/边 ID 合并尚未提交的本地动作后再发新 Delta。

## 10. 图片生成任务

### 10.1 接口清单

| 方法与路径 | 权限 | 入参 | 成功响应 | 主要规则 |
| --- | --- | --- | --- | --- |
| `POST /v3api/generation-jobs` | RUN_AI | Header `Idempotency-Key`；`CreateJobIn` | `201 GenerationJobOut` | 幂等创建、积分预冻结、异步执行 |
| `POST /v3api/generation-jobs/batch` | RUN_AI | Header `Idempotency-Key`；`BatchCreateJobIn` | `201 BatchJobOut` | 允许部分成功 |
| `GET /v3api/generation-jobs/{job_id}` | READ | 路径 ID | `GenerationJobOut` | 精确任务轮询 |
| `POST /v3api/generation-jobs/{job_id}/cancel` | RUN_AI 且任务所有者 | 路径 ID | `GenerationJobOut` | 终态重复取消为 `409` |
| `GET /v3api/generation-jobs` | READ | Query `material_id?`、`limit=30`、`offset=0` | `{items: GenerationJobOut[]}` | 未传 `material_id` 固定返回空列表 |

创建和批量接口的 `Idempotency-Key` 均必填，最长 128 字符。

### 10.2 创建入参

`CreateJobIn`

| 字段 | 类型 | 必填 | 默认/约束 | 含义 |
| --- | --- | --- | --- | --- |
| `material_id` | `string` | 是 | — | 画布项目 ID |
| `subject_id` | `string` | 是 | — | 主体 ID，必须属于同一剧本 |
| `prompt` | `string` | 否（新前端必须传） | 去首尾空白后 1–32000 字符 | 用户当前确认的最终生图提示词 |
| `model` | 图片模型 | 否 | `banana-2` | 生图模型 |
| `size` | `string` | 否 | `2k` | 输出尺寸 |
| `aspect` | `string` | 否 | `16:9` | 宽高比 |
| `count` | `integer` | 否 | 默认 1，1–4 | 生成张数 |

`BatchCreateJobIn` 用 `subject_ids: string[] | null` 替代单个 `subject_id`，并用
`prompts: Record<subject_id, prompt> | null` 分别提交每个主体的提示词，其余字段相同。
`subject_ids` 为空时，服务端自动选择当前剧本下 `not_designed` 且非声音的主体。

请求体禁止额外字段。显式 `prompt`（或批量 `prompts` 中对应项）会作为最终 provider
提示词，不要求符合 `enrich` 的十二段标题格式，并参与幂等请求哈希。为兼容旧客户端，缺省项
暂时回退到服务端主体信息与 `enrich` 快照；新前端必须始终提交用户当前编辑后的 prompt。

### 10.3 `GenerationJobOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `job_id` | `string` | 任务 ID |
| `material_id` / `script_id` | 可空 | 画布/剧本归属 |
| `subject_id` | `string` | 主体 ID |
| `episode_id` / `scene_no` | 可空 | 可选分集/场景归属 |
| `job_type` | `string \| null` | 任务类型 |
| `status` | `string` | `starting|processing|succeeded|failed|canceled` |
| `progress` | `integer` | 0–100 |
| `progress_stage` / `progress_message` | 可空 | 进度阶段与展示文本 |
| `error_message` | `string \| null` | 已脱敏的公开错误信息 |
| `input_json` | `string(JSON) \| null` | 注意：HTTP 中是 JSON 字符串，只公开 prompt 相关字段 |
| `output_json` | `string(JSON) \| null` | 注意：HTTP 中是 JSON 字符串 |
| `metrics` | `object \| null` | 仅公开 `actual_points`、`count` |
| `created_at` / `updated_at` | `datetime \| null` | 时间 |

成功任务的 `output_json` 解析后常见形态：

```json
{
  "imageOutputs": [
    {
      "status": "succeeded",
      "assetId": "asset-id",
      "storageKey": "generate_images/<mid>/<file>.png"
    }
  ],
  "primaryImageIndex": 0
}
```

前端安全解析：

```ts
function parseJsonField<T>(value: string | null): T | null {
  if (!value) return null;
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}
```

### 10.4 批量响应与状态流转

```ts
interface BatchJobOut {
  created: GenerationJobOut[];
  skipped: string[]; // 被跳过的 subject_id
  failed: Array<{
    subject_id: string;
    subject_name: string;
    reason: string;
  }>;
}
```

批量接口允许部分成功，不能因为 `failed` 非空就丢弃 `created` 中已经投递的任务。声音或缺少描述的主体进入 `skipped`；创建、预扣或投递失败进入 `failed`。

状态机：`starting → processing → succeeded | failed | canceled`。前端轮询到任一终态后停止。任务绑定创建时的主体/分集输入版本；输入后来发生变化时，旧任务可能失败并提示重新发起。

## 11. 分镜任务与视频工作台

### 11.1 接口清单

| 方法与路径 | 权限 | 入参 | 成功响应 |
| --- | --- | --- | --- |
| `POST /v3api/scripts/{script_id}/episodes/{episode_id}/generate-storyboard` | RUN_AI | Query `episode_version`；Header `Idempotency-Key` | `GenerateStoryboardResponse` |
| `POST /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}/cancel` | RUN_AI | 路径 ID | `StoryboardStatusOut` |
| `GET /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard-jobs` | READ | Query `limit=50`、`offset=0` | `StoryboardJobListOut` |
| `GET /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}` | READ | 路径 ID | `StoryboardStatusOut` |
| `PATCH /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}` | RUN_AI | `StoryboardJobEditIn` | `StoryboardStatusOut` |
| `POST /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}/retry` | RUN_AI | Header `Idempotency-Key` | `GenerateStoryboardResponse` |
| `GET /v3api/scripts/{script_id}/episodes/{episode_id}/storyboard-status` | READ | 路径 ID | 最新 `StoryboardStatusOut` |
| `GET /v3api/scripts/{script_id}/episodes/{episode_id}/shot-hierarchy` | READ | 路径 ID | `ShotHierarchyOut` |
| `POST /v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/merge` | EDIT | `ShotGroupMergeIn` | 完整 `VideoWorkbenchOut` |
| `GET /v3api/shots/{script_id}/episodes/{episode_id}/video-workbench` | READ | 路径 ID | `VideoWorkbenchOut` |
| `GET /v3api/shots/{script_id}/cover` | READ | 路径 ID | `CoverOut` |

分镜创建和重试的 `Idempotency-Key` 必填，最长 128 字符。`episode_version` 是 Query 参数，不是 JSON Body：

```text
POST /v3api/scripts/<sid>/episodes/<eid>/generate-storyboard?episode_version=7
Idempotency-Key: <uuid>
```

分集没有 `script_text`、版本过期、已有 `pending/running` 分镜任务时返回 `409`；缺少有效积分账户返回 `402`。

镜头组合并为同步编辑，不需要 `Idempotency-Key`：

```json
{
  "shot_ids": ["本集分镜-1", "本集分镜-2", "本集分镜-3", "本集分镜-4"],
  "expected_episode_version": 3
}
```

前端应在分镜任务成功后，从 `shot-hierarchy.scenes[].groups[].shots[]` 收集当前分集的**全部且唯一** `shot_id`（若兼容历史未分组数据，还应并入 `ungrouped_shots[]`）。接口接受 1～1000 个 ID，请求数组顺序不参与排序；服务端使用数据库中的场景、LLM 叙事边界和原镜头顺序，为每个场景生成一个或多个连续的 3～15 秒正式镜头组，并重算组内时间轴、文生视频 Prompt、角色/背景/道具素材和音频元素。

成功响应是 `episode_version` 已递增的完整 `VideoWorkbenchOut`，前端应直接替换当前工作台状态。合并只创建可生成视频的工作台单元：此时 `GET /video-generation-jobs` 允许为空；用户点击单个或批量“生成视频”后才创建付费异步任务。已完成组合后，以当前版本重复提交同一全集 ID 会无副作用返回现有工作台。

漏传本集任一镜头或时长无法合法分区返回 `400`；镜头属于其他剧本/分集返回 `403`；ID 不存在返回 `404`；版本过期、活动分镜任务、草稿/正式状态混杂或源组已有视频任务返回 `409`；空数组、重复 ID、超过 1000 项或字段非法返回 `422`。草稿镜头组不能调用素材检查或视频生成接口，会返回 `409`“请先完成镜头组合并”。

### 11.2 分镜任务字段

`GenerateStoryboardResponse`

| 字段 | 含义 |
| --- | --- |
| `task_id` | 异步投递任务 ID |
| `job_id` | 分镜业务任务 ID，后续查询使用它 |
| `episode_id` / `script_id` | 资源归属 |
| `status` | 初始通常 `pending` |
| `accepted` | 是否已受理；幂等命中既有任务也为 true |
| `message` | “已投递”或“任务已存在”等展示信息 |

`StoryboardStatusOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `job_id` | `string` | 业务任务 ID |
| `task_id` | `string \| null` | 异步投递 ID |
| `episode_id` / `script_id` | `string` | 资源归属 |
| `status` | `string` | `pending|running|retry|succeeded|failed|canceled` |
| `progress` | `integer` | 0–100 |
| `progress_stage` / `progress_message` | 可空 | 阶段与文案 |
| `error_message` | `string \| null` | 失败原因 |
| `detail` | `object \| null` | 任务扩展详情 |

任务列表返回 `{items, total}`。`limit` 会被服务端收敛到 1–100，`offset` 小于 0 时按 0 处理。

`PATCH` 仅允许编辑展示性字段：`progress_message?: string|null`（最长 512）和 `detail?: object|null`。任务处于 `pending/running` 时不可编辑，返回 `409`。

重试仅允许 `failed/canceled`；它会重新走分镜创建逻辑。若旧任务绑定的 `input_version` 已落后于当前分集，重试会因版本冲突被拒绝。

### 11.3 分镜层级

`ShotHierarchyOut`：

```ts
interface ShotHierarchyOut {
  episode_id: string;
  episode_version: number;
  scenes: Array<{
    scene_no: number;
    groups: ShotGroupHierarchyOut[];
    ungrouped_shots: ShotOut[];
  }>;
}
```

`ShotGroupHierarchyOut`：`shot_group_id`、`scene_no`、`group_no`、`prompt`、`reference_frames`、`shots`。

`ReferenceFramesOut`：`first_frame`、`last_frame`、`reference_frame`，均为可空 storage key。

`ShotOut`

| 字段 | 含义 |
| --- | --- |
| `shot_id` | 镜头 ID |
| `scene_no` | 场景序号 |
| `shot_group_id` / `group_no` | 镜头组 ID/展示号 |
| `shot_no` / `display_no` | 镜头编号/组内展示序号 |
| `content` | 剧情内容 |
| `description` | 固定结构的摄影描述 |
| `shot_line` | 带时间段的镜头行 |
| `start_seconds` / `end_seconds` | 组内起止秒数 |
| `characters` / `props` / `sounds` | 引用主体名称数组 |
| `dialogue_character` | 台词角色，缺省空字符串 |

### 11.4 视频工作台

`VideoWorkbenchOut`：`episode_id`、`episode_version`、`scenes`。

工作台场景字段：

| 字段 | 含义 |
| --- | --- |
| `scene_key` | 场景运行键，可空 |
| `scene_no` | 场景序号 |
| `background` / `label` | 背景主体/展示标签 |
| `shot_group_count` | 镜头组数量 |
| `pending_shot_count` | 尚无成功视频任务的镜头组所包含镜头数 |
| `shot_groups` | 视频生成单元 |
| `pending_shots` | 未归组镜头 |

工作台镜头组字段：

| 字段 | 含义 |
| --- | --- |
| `shot_group_id` | 镜头组 ID，创建视频任务使用 |
| `scene_no` / `background` / `scene_key` | 场景信息 |
| `group_no` | 展示组号 |
| `unit_id` / `unit_runtime_key` | 视频生成单元标识 |
| `prompt` | 视频提示词 |
| `duration_seconds` / `video_duration_seconds` | 计划/视频时长 |
| `materials` | 按角色、背景、道具、其他分组的主体引用 |
| `audio_elements` | 规范化音频元素对象数组 |
| `shot_ids` / `shot_display_nos` / `shot_nos` | 组内镜头引用 |
| `description_text` | 聚合描述 |
| `reference_frames` | 首帧/尾帧/参考帧 |

`materials` 的 `character`、`background`、`prop`、`other` 均为数组，每项 `{name, subject_id, asset_entry_id}`，后两字段可空。

`CoverOut`：`script_id`、`cover`、`mime_type`；`cover` 是首个镜头首帧 storage key，可能为空。

## 12. 视频生成

### 12.1 接口清单

| 方法与路径 | 权限 | 入参 | 成功响应 | 主要规则 |
| --- | --- | --- | --- | --- |
| `GET /v3api/video-generation/active-provider` | 登录 | 无 | `ActiveProviderOut` | 获取实际激活供应商和模型 |
| `POST /v3api/video-generation/active-provider` | `is_admin=true` | `ActiveProviderUpdateIn` | `ActiveProviderOut` | 环境锁定模式下返回 `409` |
| `GET /v3api/video-generation/check-materials` | READ | Query `script_id`、`shot_group_id` | `{missing_subjects,missing_audio,ready}` | `ready=false` 只提示，不阻断生成 |
| `POST /v3api/video-generation-jobs` | EDIT | Header `Idempotency-Key`；`VideoJobCreateIn` | `201 VideoJobOut` | 创建单个镜头组视频任务 |
| `GET /v3api/video-generation-jobs` | READ | Query `script_id`、`episode_id?`、`limit=30` | `VideoJobListOut` | 创建时间倒序，含 `total` |
| `GET /v3api/video-generation/{job_id}` | READ | 路径 ID | `VideoJobOut` | 精确状态轮询 |
| `POST /v3api/video-generation/{job_id}/cancel` | EDIT | 路径 ID | `VideoJobOut` | 仅活动态可取消；已取消幂等返回 |
| `POST /v3api/video-generation-jobs/batch` | EDIT | Header `Idempotency-Key`；`VideoJobBatchCreateIn` | `201 VideoJobBatchOut` | 每个镜头组独立成功/失败 |

虽然单个视频创建、批量视频创建和时间线合成的 OpenAPI 请求头显示为可选，当前业务实现会在缺少或空 `Idempotency-Key` 时返回 `400/409`。前端应一律视为必填。视频幂等键最长 256 字符。

### 12.2 供应商

`ActiveProviderOut`：

| 字段 | 含义 |
| --- | --- |
| `provider_key` | 激活供应商 key |
| `job_type` | 当前任务类型，通常 `all_reference` |
| `model` | 实际模型名 |

切换请求：

```json
{
  "provider_key": "modelsai",
  "job_type": "all_reference"
}
```

`provider_key` 入参枚举：`jianyu | modelsai | bailian | openai | gemini`。环境配置模式下查询接口可能返回规范化后的 `volcengine`；前端展示时应接受未知字符串，不要把响应强制限制为切换入参枚举。

### 12.3 素材检查

```ts
interface CheckMaterialsOut {
  missing_subjects: string[];
  missing_audio: string[];
  ready: boolean;
}
```

`ready=false` 表示参考素材不完整。后端仍允许生成，缺失主体不会进入参考图；推荐前端标红缺失项并让用户确认，而不是直接禁用所有生成入口。

### 12.4 创建与批量创建

`VideoJobCreateIn`

| 字段 | 类型 | 必填 | 含义 |
| --- | --- | --- | --- |
| `script_id` | `string` | 是 | 剧本 ID |
| `episode_id` | `string` | 是 | 分集 ID |
| `shot_group_id` | `string` | 是 | 镜头组 ID |
| `prompt` | `string` | 否（新前端必须传） | 用户当前编辑的视频提示词，去首尾空白后 1–32000 字符 |

`VideoJobBatchCreateIn`：`script_id`、`episode_id`、`shot_group_ids`、
`prompts: Record<shot_group_id, prompt> | null`；其中 `shot_group_ids` 为 1–100 项。
映射中出现非目标镜头组会在创建任何任务前整体返回 `400`。

显式 prompt 优先于镜头组已有 prompt，并参与幂等请求哈希；服务端会确保最终 Provider
输入包含完整视频约束锁。未传 prompt 时继续回退镜头组 prompt，以兼容旧客户端。同一镜头组
已有活动任务时，最终提示词相同才会复用；提示词不同返回 `409`，不会新增任务或冻结积分。

批量接口用请求头作为前缀，服务端按 `<prefix>:<index>` 派生子任务键。前缀过长导致派生键超过 256 字符时返回 `400`。

`VideoJobBatchOut`

```ts
interface VideoJobBatchOut {
  success: VideoJobOut[];
  failed: Array<{
    shot_group_id: string;
    error_message: string;
  }>;
}
```

批量接口不会因单个失败回滚已成功任务。前端必须同时处理 `success` 和 `failed`，并对 `success` 中每个 `job_id` 启动轮询。

### 12.5 `VideoJobOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `job_id` | `string` | 视频任务 ID |
| `script_id` / `episode_id` | `string` | 剧本与分集归属 |
| `shot_group_id` | `string \| null` | 镜头组 ID |
| `owner_user_id` | `string \| null` | 创建者 |
| `idempotency_key` | `string \| null` | 创建幂等键 |
| `request_hash` | `string \| null` | 后端请求快照摘要；前端只读 |
| `input_version` | `integer \| null` | 固化的分集输入版本 |
| `attempt` | `integer \| null` | 不可变执行尝试编号 |
| `reservation_id` | `string \| null` | 积分预约关联 |
| `dispatch_id` | `string \| null` | 异步投递记录 |
| `scene_no` | `integer` | 场景序号 |
| `unit_id` / `unit_runtime_key` | `string` | 视频单元标识 |
| `provider_key` / `job_type` | `string` | 供应商和任务类型 |
| `status` | `string` | `starting|processing|succeeded|failed|canceled` |
| `progress` | `integer` | 0–100 |
| `progress_stage` / `progress_message` | 可空 | 阶段与进度文案 |
| `error_type` / `error_message` | 可空 | 失败类型与安全展示信息 |
| `retryable` | `boolean \| null` | 失败是否具备系统重试条件；当前无公开视频 retry 接口 |
| `input_json` | `object \| null` | 生成快照和参考素材 |
| `output_json` | `object \| null` | 视频产物与 Provider 信息 |
| `metrics` | `object \| null` | 模型、供应商、参考数、计费等指标 |
| `created_at` / `updated_at` | `datetime \| null` | 时间 |

与图片任务不同，视频任务的 `input_json`、`output_json` 是 JSON 对象，不是 JSON 字符串。

`input_json` 常见字段：`model`、`prompt`、`reference_images`、`reference_image_names`、`reference_audios`、`reference_audio_names`、`reference_videos`、`reference_video_names`、`first_frame`、`missing_subjects`、`missing_audio`、`duration_seconds`、`shot_group_version`。

成功 `output_json` 常见字段：

```json
{
  "video_storage_key": "generate_images/video/<job-attempt>.mp4",
  "reference_image_names": ["角色A", "场景B"],
  "model": "doubao-seedance-2-0-260128",
  "provider_task_id": "provider-remote-id"
}
```

前端播放产物时优先读取 `output_json.video_storage_key`。时间线后端还兼容历史形态 `output_json.storage_key` 和 `output_json.video.storage_key`，前端如需兼容旧数据可按同样顺序回退。

### 12.6 幂等、轮询与取消

- 相同剧本、相同幂等键、相同请求快照：返回既有任务，不重复冻结积分；幂等绑定不按项目成员用户 ID 隔离。
- 同一幂等键用于不同请求：`409`。
- 同一镜头组已有 `starting/processing` 任务时，服务端复用该活动任务。
- `draft:` 暂存组不是视频生成单元；素材检查和视频任务创建只接受规范的 `group:<shot_group_id>` 正式组。
- 轮询到 `succeeded`、`failed`、`canceled` 后停止。
- 取消仅接受 `starting/processing`；重复取消已 `canceled` 的任务幂等返回当前任务；其他终态返回 `409`。
- 若远端提交结果处于未知状态，取消会返回 `409` 并保持冻结，前端应显示“等待系统核对”，不要反复取消。
- 当前没有公开的视频任务重试接口。`retryable=true` 不代表前端可以调用不存在的 URL；需要重新创建时应由用户发起新动作并生成新幂等键。

## 13. 视频时间线与最终合成

### 13.1 接口清单

| 方法与路径 | 权限 | 入参 | 成功响应 |
| --- | --- | --- | --- |
| `GET /v3api/video-timeline-arrangements` | READ | Query `script_id`、`episode_id` | `TimelineOut` |
| `POST /v3api/video-timeline-arrangements` | EDIT | Query `script_id`、`episode_id`；`TimelineItemIn` | `201 TimelineItemOut` |
| `PATCH /v3api/video-timeline-arrangements/{arrangement_id}` | EDIT | `TimelineUpdateIn` | `TimelineItemOut` |
| `PUT /v3api/video-timeline-arrangements/reorder` | EDIT | Query `script_id`、`episode_id`；`ReorderIn` | 完整 `TimelineOut` |
| `DELETE /v3api/video-timeline-arrangements/{arrangement_id}` | EDIT | 路径 ID | `204 No Content` |
| `POST /v3api/video-timeline-arrangements/render` | RUN_AI | Header `Idempotency-Key`；`RenderRequestIn` | `202 RenderJobOut` |
| `GET /v3api/video-timeline-arrangements/render/{job_id}` | READ | 路径 ID | `RenderStatusOut` |
| `POST /v3api/video-timeline-arrangements/render/{job_id}/cancel` | RUN_AI | 路径 ID | `RenderStatusOut` |
| `POST /v3api/video-timeline-arrangements/render/{job_id}/retry` | RUN_AI | 路径 ID | `202 RenderJobOut`（新任务） |

注意静态路径 `/reorder`、`/render` 与动态 `/{arrangement_id}` 并存；前端必须使用表中准确 URL。

### 13.2 时间线片段

`TimelineItemIn`

| 字段 | 约束 | 含义 |
| --- | --- | --- |
| `unit_runtime_key` | 1–128 字符 | 视频单元运行键 |
| `job_id` | 1–36 字符 | 已成功的视频生成任务 ID |

添加时，视频任务必须属于相同剧本和分集、状态为 `succeeded`，且运行键匹配。新片段自动追加到末尾并默认选中。

`TimelineItemOut`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | `string` | 时间线片段 ID |
| `unit_runtime_key` | `string` | 视频单元运行键 |
| `job_id` | `string` | 视频生成任务 ID |
| `row_order` | `integer` | 排序位置 |
| `is_selected` | `boolean` | 是否进入最终合成 |
| `trim_start_seconds` | `number` | 裁入秒数 |
| `trim_end_seconds` | `number \| null` | 裁出秒数；null 表示到片尾 |
| `version` | `integer` | 乐观锁版本 |
| `created_at` / `updated_at` | `datetime` | 时间 |

`TimelineOut` 为 `{script_id, episode_id, items}`，`items` 按 `row_order` 升序。

### 13.3 更新与重排

`TimelineUpdateIn`

| 字段 | 类型 | 必填 | 规则 |
| --- | --- | --- | --- |
| `row_order` | `integer \| null` | 否 | ≥0 |
| `is_selected` | `boolean \| null` | 否 | 是否参与合成 |
| `trim_start_seconds` | `number \| null` | 否 | ≥0 |
| `trim_end_seconds` | `number \| null` | 否 | ≥0；显式 null 可清空裁出点 |
| `expected_version` | `integer` | 是 | ≥0，当前片段版本 |

裁出点必须晚于裁入点。即使只修改其中一个字段，服务端也会结合现有另一个值进行校验。

`ReorderIn`

```ts
interface ReorderIn {
  items: Array<{
    id: string;
    row_order: number;
    expected_version: number;
  }>;
}
```

`items` 至少一项，ID 和目标 `row_order` 不得重复。任一片段版本过期都会使批量操作返回 `409`；此时重新 GET 完整时间线并重建排序请求。

### 13.4 合成任务

`RenderRequestIn`：`script_id`、`episode_id` 均为必填非空字符串。`Idempotency-Key` 业务上必填，最长 256 字符。

`RenderJobOut`

| 字段 | 含义 |
| --- | --- |
| `job_id` | 新建或幂等命中的渲染任务 ID |
| `status` | `starting|processing|succeeded|failed|canceled` |
| `output_storage_key` | 预分配输出路径，可空 |

`RenderStatusOut`

| 字段 | 含义 |
| --- | --- |
| `job_id` | 渲染任务 ID |
| `status` | `starting|processing|succeeded|failed|canceled` |
| `progress` | 0–100 |
| `progress_stage` / `progress_message` | 当前阶段与文案 |
| `error_message` / `error_type` | 失败信息 |
| `retryable` | 是否允许调用 retry |
| `storage_key` | 成功后实际产物 storage key |
| `output_storage_key` | 预分配/记录的输出 storage key |

发起合成时，后端固化当前时间线快照，只选择 `is_selected=true` 的片段。时间线为空、没有选中片段返回 `404`；关联视频未成功或缺少产物返回 `409`。

同一用户的同一幂等键：快照相同返回原任务，快照不同返回 `409`。因此用户修改排序、选择或裁切后，应生成新幂等键。

取消对终态任务按幂等读取处理。重试仅允许 `status=failed && retryable=true`；成功会创建新的 `job_id`、输出路径和积分预约。前端必须切换轮询到 retry 响应中的新 ID。

## 14. 媒体上传、播放与下载

媒体接口不带 `/v3api` 前缀。

### 14.1 媒体 URL 拼接

```ts
const MEDIA_PREFIX = "/api/x/cctv/zhihui/flow-app";

export function mediaUrl(storageKey: string): string {
  const normalized = storageKey.startsWith("/") ? storageKey : `/${storageKey}`;
  return `${API_ORIGIN}${MEDIA_PREFIX}${normalized}`;
}
```

媒体路径也要求会话 Cookie 和资源级 READ 权限。不要把对象存储 key 替换成公开 bucket URL，也不要缓存可越权复用的裸对象地址。

### 14.2 `GET /api/x/cctv/zhihui/flow-app/{storage_key:path}`

| 项目 | 说明 |
| --- | --- |
| 鉴权 | 登录 + storage key 所属项目 READ 权限 |
| Path | `storage_key`，可包含多层 `/` |
| Header | 可选 `Range` |
| 成功 | 无 Range 为 `200`；有效 Range 为 `206`；响应体是二进制流 |
| 常见错误 | 非法路径 `403`、无 DB 归属/对象不存在 `404`、整文件过大 `413`、Range 无效 `416` |

后端只允许并验证与数据库资源精确绑定的以下媒体：

- `generate_images/...`：普通生成图；
- `generate_images/video/...`：视频生成产物/参考帧；
- `upload_images/...`：用户上传且已进入 ready 状态的媒体；
- `generate_outputs/...`、`render_outputs/...`：成功时间线合成产物。

成功响应头：

| 响应头 | 含义 |
| --- | --- |
| `Content-Type` | 优先使用上传/资产记录的真实 MIME，否则按扩展名 |
| `Accept-Ranges: bytes` | 支持字节范围 |
| `Content-Length` | 当前响应字节数 |
| `Content-Disposition: inline` | 浏览器内联播放/展示 |
| `Content-Range` | 仅 206，示例 `bytes 0-65535/1234567` |

Range 支持：`bytes=start-end`、`bytes=start-`、`bytes=-suffix`；不支持多段 Range。无 Range 且文件超过服务端 `max_media_read_bytes`（默认 2 GiB）会返回 `413`，Range 分段读取不受整文件读取上限限制。

视频/音频请直接把代理 URL 赋给 `<video src>`/`<audio src>`，让浏览器自行处理 Range；不要先用 Fetch 把完整大文件读入内存。

### 14.3 `POST /api/x/cctv/zhihui/flow-app/upload_images/{mid}/{filename}`

虽然路径名叫 `upload_images`，当前实现支持图片、音频和视频。

| 参数 | 位置 | 必填 | 含义 |
| --- | --- | --- | --- |
| `mid` | Path | 是 | 画布 `materials_id` |
| `filename` | Path | 是 | 原始文件名；建议使用 `encodeURIComponent` |
| `subject_id` | Query | 否 | 必须属于同一剧本；图片/音频会为该主体创建候选 AssetEntry，视频不会创建候选资产 |
| `file` | multipart | 是 | 文件二进制 |

支持且需通过真实内容探测的类型：

- 图片：PNG、JPEG、WebP；
- 音频：MP3、WAV、M4A、AAC、OGG；
- 视频：MP4、WebM、MOV/QuickTime。

后端不信任浏览器提供的 MIME 或文件扩展名，会实际解析媒体。默认上传上限 512 MiB；空文件返回 `400`，不支持或解析失败返回 `400`，超限返回 `413`。

```ts
async function uploadMedia(
  materialId: string,
  file: File,
  subjectId?: string,
) {
  const form = new FormData();
  form.append("file", file);
  const query = subjectId ? `?subject_id=${encodeURIComponent(subjectId)}` : "";
  return api<UploadMediaOut>(
    `${MEDIA_PREFIX}/upload_images/${encodeURIComponent(materialId)}/${encodeURIComponent(file.name)}${query}`,
    { method: "POST", body: form },
  );
}
```

成功响应：

```ts
interface UploadMediaOut {
  storage_key: string; // 已带前导 /
  mime_type: string;
  size_bytes: number;
  sha256: string;
  asset_id: string | null;
  upload_id: string;
}
```

`asset_id` 只有在“携带 `subject_id` 且上传内容为图片或音频”时才会生成；普通上传或视频上传返回 `null`。

上传采用“临时对象 → 晋升 → ready 记录”的流程。若服务端在有限等待时间内不能证明晋升成功，返回 `409`；前端不能把未知结果当作成功，应提示用户稍后重新上传。只有收到成功响应后才能把 `storage_key` 写入本地业务状态。

## 15. 状态枚举与轮询策略

### 15.1 状态对照

| 业务 | 活动态 | 成功态 | 失败/取消终态 |
| --- | --- | --- | --- |
| 五阶段流水线 | `pending`、`running` | `done` | `failed` |
| 提取/merge 查询 | `pending`、`running` | `succeeded` | `failed` |
| 图片任务 | `starting`、`processing` | `succeeded` | `failed`、`canceled` |
| 分镜任务 | `pending`、`running`、`retry` | `succeeded` | `failed`、`canceled` |
| 视频任务 | `starting`、`processing` | `succeeded` | `failed`、`canceled` |
| 时间线合成 | `starting`、`processing` | `succeeded` | `failed`、`canceled` |

### 15.2 推荐轮询器

下面是前端建议，不是额外后端契约：

```ts
async function pollJob<T extends { status: string }>(
  load: () => Promise<T>,
  terminal: ReadonlySet<string>,
  signal?: AbortSignal,
): Promise<T> {
  let delay = 1000;
  for (;;) {
    if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
    const value = await load();
    if (terminal.has(value.status)) return value;
    await new Promise((resolve) => setTimeout(resolve, delay));
    delay = Math.min(Math.round(delay * 1.4), 5000);
  }
}
```

轮询建议：

- 页面离开或组件卸载时用 `AbortController` 停止；
- 401 立即停止并进入重新登录；
- 403/404 停止并刷新资源导航；
- 临时 5xx 使用有限退避，不能把网络失败当作任务失败；
- 浏览器恢复可见时立即拉一次状态；
- 终态以后停止轮询，避免持续占用后端连接。

## 16. 前端数据处理注意事项

### 16.1 JSON 字段差异

| 对象 | `input_json` / `output_json` 的 HTTP 类型 |
| --- | --- |
| `GenerationJobOut`（图片） | `string(JSON) \| null`，需要 `JSON.parse` |
| `VideoJobOut`（视频） | `object \| null`，不要再次 `JSON.parse` |

这是当前兼容契约的真实差异，通用 Job 组件必须按任务类型分别处理。

### 16.2 列表包装差异

| 接口 | 返回形态 |
| --- | --- |
| 剧本分页、主体、资产、项目成员、分镜任务、视频任务 | `{items, total}` |
| 积分账户、积分流水、父主体、图片任务列表 | `{items}`；部分无 `total` |
| 分集列表 | 裸数组 `EpisodeListItem[]` |
| 新闻播报任务 | 裸数组，当前固定 `[]` |
| 候选列表 | `{image_candidates, audio_candidates}` |
| 图片批量任务 | `{created, skipped, failed}` |
| 视频批量任务 | `{success, failed}` |

不要用一个假定所有列表都有 `items/total` 的全局解包器。

### 16.3 资源缓存键

推荐 Query Cache/状态库使用以下键，避免切换组织或剧本后串数据：

```ts
["me"]
["credits", activeOrgId]
["scripts", activeOrgId, scope, limit, offset]
["script", scriptId]
["episodes", scriptId]
["subjects", scriptId, limit, offset]
["canvas", materialId]
["storyboard", scriptId, episodeId, jobId]
["video-jobs", scriptId, episodeId]
["timeline", scriptId, episodeId]
```

切换组织后应失效所有含 `activeOrgId` 或 team 内容的缓存。退出登录后必须清除包含剧本正文、主体描述和媒体地址的持久缓存。

### 16.4 媒体与 XSS

- `progress_message`、`error_message`、主体名称、描述、prompt 均按普通文本渲染，不使用未净化的 `dangerouslySetInnerHTML`。
- storage key 只传给统一 `mediaUrl()`，不要拼入任意第三方 Origin。
- 文件名进入 Path 前使用 `encodeURIComponent`。
- 不在前端日志、埋点或错误上报中记录密码、会话 Cookie、完整剧本文本或供应商字段。

## 17. 联调检查清单

- 所有 JSON 请求都使用正确前缀并携带 Cookie。
- 上传使用 `FormData`，未手动覆盖 multipart `Content-Type`。
- 创建型 AI/积分/合成请求为一次用户动作生成稳定幂等键。
- 分集、主体、画布、时间线写操作均回传最新版本字段。
- `409` 先刷新资源，不自动无限重放旧请求。
- 图片任务 JSON 字段按字符串解析；视频任务 JSON 字段按对象读取。
- 所有异步任务保存后端 `job_id`，刷新页面后可恢复轮询。
- 轮询到终态停止；页面卸载时取消轮询。
- 媒体统一走鉴权代理，并允许浏览器播放器发送 Range。
- 列表按各接口真实包装结构解包，不假定全部都有 `items/total`。
- 切换组织和退出登录时失效相关缓存。
- `news-broadcast/jobs` 当前仅为空数组，不显示为已实现的新闻生产功能。
- 前端不提供当前后端不存在的视频任务 retry 按钮。

---

如需生成 TypeScript 类型，可从 `/v3api/openapi.json` 自动生成基础类型，再用本文中的业务规则补充幂等、权限、状态机、JSON 字段差异和交互约束。OpenAPI 是结构契约，本文是结构契约加业务对接语义，两者应同时使用。
