# 多模态 Providers 统一适配设计

## 目标

让后端通过 `.env` 按能力独立选择模型厂商，不改业务调用代码即可切换文本、对话、图片和视频生成。首批覆盖 OpenAI、Anthropic Claude、Google Gemini、阿里百炼、DeepSeek，以及现有火山方舟视频兼容路径。

## 约束

- 不读取、记录或提交真实 `.env` 与密钥，只维护 `.env.example`。
- 新配置优先，旧厂商字段仅作兼容回退；已有业务接口、积分和异步任务状态机不改。
- 厂商能力必须显式声明；请求不支持的能力或未知厂商时立即报出可操作错误，不静默回退到其他厂商。
- 文本与对话允许使用厂商官方 OpenAI 兼容协议；协议不兼容的 Claude 使用原生 Messages API。
- 图片和视频保留各厂商原生异步任务语义，下载结果继续经过现有 SSRF 与体积限制。
- 官方模型标识会演进，示例给出当前可验证值，但部署者可通过环境变量覆盖。

## 配置契约

四种能力统一提供：

```text
<CAPABILITY>_PROVIDER
<CAPABILITY>_BASE_URL
<CAPABILITY>_API_KEY
<CAPABILITY>_MODEL
```

其中 `<CAPABILITY>` 为 `TEXT_LLM`、`CHAT_LLM`、`IMAGE_GEN`、`VIDEO_GEN`。通用字段优先级最高；为空时按选中厂商读取旧字段。厂商别名在进入工厂时规范化，例如 `claude -> anthropic`、`google -> gemini`、`dashscope -> bailian`、`sora -> openai`。

视频历史上还有数据库 active provider 配置。`VIDEO_PROVIDER_SELECTION_MODE=env` 时能力级环境变量优先；默认 `database` 保持旧部署的动态切换语义。

## 适配矩阵

| 厂商 | 文本 | 对话 | 图片 | 视频 |
|---|---:|---:|---:|---:|
| OpenAI | 是 | 是 | 是 | 是 |
| Anthropic Claude | 是 | 是 | 否 | 否 |
| Google Gemini | 是 | 是 | 是 | 适配器已实现，业务启用待数据库迁移 |
| 阿里百炼 | 是 | 是 | 是（通义万相） | 是（通义万相） |
| DeepSeek | 是 | 是 | 否 | 否 |
| 火山方舟 | 否 | 否 | 否 | 是（保留现有兼容） |

“Claude Code”是调用 Claude 模型 API 的开发代理产品，不作为新的推理协议；服务端适配的是 Anthropic Messages API。

## 路由与错误语义

1. 工厂首先读取能力对应的 `*_PROVIDER`，请求中的 `model` 只覆盖模型，不改变厂商。
2. 旧的前端 provider/model 别名仅在没有显式能力配置或为历史视频 provider key 时用于兼容。
3. 未知厂商抛 `UnsupportedProviderError`；厂商不支持能力抛 `UnsupportedCapabilityError`；缺配置抛包含具体变量名的 `ProviderConfigurationError`。
4. 测试可重置 LLM 单例缓存；图片和视频 Provider 保持无状态实例。

## 验收标准

- 四种能力均可仅通过 `.env.example` 展示的通用字段完成配置。
- 文本/对话工厂覆盖 OpenAI、DeepSeek、百炼、Gemini 与 Anthropic。
- 图片工厂覆盖 OpenAI、Gemini 与百炼；视频工厂覆盖 OpenAI、Gemini Veo、百炼与火山方舟。
- 显式配置优先于请求模型别名；未知或不支持组合不静默回退。
- 新增配置回退、工厂路由、请求组装、轮询和错误分支测试，且现有 provider/worker 测试不回归。
- Ruff、Mypy、compileall、锁文件检查与相关 pytest 通过；不调用真实付费 API。
