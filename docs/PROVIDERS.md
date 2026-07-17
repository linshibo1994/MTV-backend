# 多模态模型供应商接入指南

本文说明后端如何通过环境变量为文本、对话、图片和视频能力选择国内外模型厂商。配置以 `backend/.env.example` 为模板；请复制到部署环境并填入真实值，不要提交真实 `.env`。

## 能力矩阵

| Provider | 文本 | 对话 | 图片生成 | 视频生成 | 实现协议 |
|---|---:|---:|---:|---:|---|
| `openai` / `gpt` | 是 | 是 | 是 | 是（兼容保留） | Chat Completions、Images、Videos |
| `anthropic` / `claude` | 是 | 是 | 否 | 否 | Anthropic Messages |
| `gemini` / `google` | 是 | 是 | 是 | 适配器预览 | OpenAI 兼容文本、Google GenAI 图片/Veo |
| `bailian` / `dashscope` | 是 | 是 | 是 | 是 | OpenAI 兼容文本、DashScope 原生异步媒体 API |
| `deepseek` | 是 | 是 | 否 | 否 | OpenAI 兼容 Chat Completions |
| `volcengine` / `seedance` | 否 | 否 | 否 | 是 | 火山方舟 SDK（AK/SK） |
| `openai_compatible` / `custom` | 是 | 是 | 否 | 否 | 自定义 OpenAI 兼容端点 |

“Claude Code”是编码 Agent 产品，不是独立模型推理协议。配置 `TEXT_LLM_PROVIDER=anthropic` 或 `CHAT_LLM_PROVIDER=anthropic` 时，后端调用的是 Anthropic 官方 `/v1/messages`。

## 四组统一配置

每种能力都遵循同一命名：

```dotenv
TEXT_LLM_PROVIDER=openai
TEXT_LLM_BASE_URL=
TEXT_LLM_API_KEY=
TEXT_LLM_MODEL=gpt-4o

CHAT_LLM_PROVIDER=anthropic
CHAT_LLM_BASE_URL=https://api.anthropic.com
CHAT_LLM_API_KEY=
CHAT_LLM_MODEL=

IMAGE_GEN_PROVIDER=gemini
IMAGE_GEN_BASE_URL=
IMAGE_GEN_API_KEY=
IMAGE_GEN_MODEL=gemini-3.1-flash-image

VIDEO_GEN_PROVIDER=bailian
VIDEO_PROVIDER_SELECTION_MODE=env
VIDEO_GEN_BASE_URL=https://dashscope.aliyuncs.com
VIDEO_GEN_API_KEY=
VIDEO_GEN_MODEL=
```

优先级为：调用方显式 provider（若接口提供）> 能力级 `.env` > 旧厂商字段 > 历史模型别名。`*_MODEL` 留空时按所选厂商回退默认模型，因此切换厂商时不会误用上一个厂商的示例模型。未知厂商或不支持的能力会直接报错，不会静默切换厂商产生意外费用。

应用启动时加载配置为单例；修改 `.env` 后必须重启 API 与 Celery worker。

## 常见配置

### OpenAI / GPT

```dotenv
TEXT_LLM_PROVIDER=openai
TEXT_LLM_API_KEY=your-key
TEXT_LLM_MODEL=gpt-4o

IMAGE_GEN_PROVIDER=openai
IMAGE_GEN_API_KEY=your-key
IMAGE_GEN_MODEL=gpt-image-2
```

OpenAI 视频适配继续支持现有 Sora 任务，但官方已将 Sora 2 Videos API 标记为弃用，并计划于 2026-09-24 关闭。新部署应优先选择 Gemini Veo、阿里百炼或火山方舟，并把 OpenAI 视频作为迁移期兼容路径。

### Anthropic Claude

```dotenv
TEXT_LLM_PROVIDER=anthropic
TEXT_LLM_BASE_URL=https://api.anthropic.com
TEXT_LLM_API_KEY=your-key
TEXT_LLM_MODEL=your-claude-model
```

Claude 的 `system` 指令是请求体顶层字段，结构化文本通过强制 `tool_use` 获取，不能把 OpenAI 请求体直接换 URL 发送。Anthropic 官方不提供图片或视频生成能力。

### Google Gemini / Veo

```dotenv
TEXT_LLM_PROVIDER=gemini
TEXT_LLM_API_KEY=your-key
TEXT_LLM_MODEL=gemini-2.5-flash

IMAGE_GEN_PROVIDER=gemini
IMAGE_GEN_API_KEY=your-key
IMAGE_GEN_MODEL=gemini-3.1-flash-image

VIDEO_GEN_PROVIDER=gemini
VIDEO_GEN_API_KEY=your-key
VIDEO_GEN_MODEL=veo-3.1-generate-preview
```

Veo 是长任务：提交后返回 operation name，Provider 轮询至完成并把视频立即持久化。官方说明生成视频在服务端保留 2 天。当前 Veo 适配器和无付费外呼测试已经完成，但真实业务任务还需先执行本文“视频选择的双配置源”所述数据库迁移；迁移前服务会明确拒绝，不会写入非法任务。Imagen 已弃用并计划于 2026-08-17 关闭，本项目不把 Imagen 作为默认图片模型。

### 阿里百炼

```dotenv
TEXT_LLM_PROVIDER=bailian
TEXT_LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
TEXT_LLM_API_KEY=your-key
TEXT_LLM_MODEL=qwen-plus

IMAGE_GEN_PROVIDER=bailian
IMAGE_GEN_BASE_URL=https://dashscope.aliyuncs.com
IMAGE_GEN_API_KEY=your-key
IMAGE_GEN_MODEL=wan2.1-t2i-plus

VIDEO_GEN_PROVIDER=bailian
VIDEO_GEN_BASE_URL=https://dashscope.aliyuncs.com
VIDEO_GEN_API_KEY=your-key
VIDEO_GEN_MODEL=wan2.7-t2v
```

文本/对话走 OpenAI 兼容地址，图片/视频走 DashScope 原生地址，两者不能混用。API Key、WorkspaceId 与 Base URL 必须属于同一地域；北京、新加坡、东京、法兰克福和弗吉尼亚的地址不同。图片与视频结果 URL 有效期短，Provider 会立即下载并写入对象存储。

### DeepSeek

```dotenv
TEXT_LLM_PROVIDER=deepseek
TEXT_LLM_BASE_URL=https://api.deepseek.com
TEXT_LLM_API_KEY=your-key
TEXT_LLM_MODEL=deepseek-v4-flash
```

截至 2026-07-17，DeepSeek 官方公开 API 提供文本/对话能力，未提供图片或视频生成端点；这两种组合会被能力注册表拒绝。

### 其他 OpenAI 兼容厂商

```dotenv
TEXT_LLM_PROVIDER=openai_compatible
TEXT_LLM_BASE_URL=https://vendor.example/v1
TEXT_LLM_API_KEY=your-key
TEXT_LLM_MODEL=vendor-model
```

该模式只声明协议兼容，不假设厂商提供图片或视频端点。媒体能力必须增加对应原生适配器和能力测试。

## 视频选择的双配置源

历史系统支持数据库中的 active 视频 Provider。为避免已有 `.env` 中的默认字段意外禁用后台切换，选择源由 `VIDEO_PROVIDER_SELECTION_MODE` 明确控制：新配置设为 `env` 时由 `VIDEO_GEN_*` 决定；旧部署未配置时默认为 `database`，继续使用数据库 active 行。

新增视频厂商若要贯通数据库任务，还必须同步更新 `video_provider_configs` 与 `video_generation_jobs` 的检查约束并执行 Alembic 迁移。迁移前，Provider 工厂可测试 Gemini Veo，但数据库任务不能持久化 `provider_key=gemini`。

当前对象存储接口以完整 `bytes` 接收视频，OpenAI/Google SDK 下载也会先在内存中取得完整结果，再执行 1 GiB 上限检查；这不能防止下载阶段的内存峰值。生产部署应限制并发与 worker 内存，后续需要把下载和存储协议一起升级为流式接口。

## 官方文档（核验日期：2026-07-17）

- [OpenAI API](https://platform.openai.com/docs/api-reference/introduction)、[图片生成](https://platform.openai.com/docs/guides/image-generation)、[视频生成](https://platform.openai.com/docs/guides/video-generation)
- [Anthropic Messages](https://docs.anthropic.com/en/api/messages)、[Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview)
- [Gemini Interactions](https://ai.google.dev/gemini-api/docs/interactions-overview)、[图片生成](https://ai.google.dev/gemini-api/docs/image-generation)、[Veo](https://ai.google.dev/gemini-api/docs/veo)
- [阿里百炼 OpenAI 兼容接口](https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions)、[Qwen Image](https://help.aliyun.com/zh/model-studio/qwen-image-api)、[文生视频](https://help.aliyun.com/zh/model-studio/text-to-video-api-reference)
- [DeepSeek API](https://api-docs.deepseek.com/zh-cn/)、[Chat Completion](https://api-docs.deepseek.com/zh-cn/api/create-chat-completion)
