# 视频声音生成兜底 Spec

## 目标

修复 Seedance 视频任务在没有已审核参考音频时生成静音视频的问题：有参考音频时继续随请求提交参考音频；没有参考音频时仍启用模型原生声音生成，使模型依据镜头 Prompt 生成对白、环境音和音效。

## 约束

- `reference_audios` 仍只包含已审核且可读取的音频资产，不把声音主体描述伪装成音频文件。
- 是否生成声音不得再由 `reference_audios` 是否为空决定。
- 本次仅修改 Seedance Provider；不改变不支持参考音频的其他视频 Provider 契约。
- 素材检查继续返回 `missing_audio` 供提示，但缺少可选参考音频不得阻止原生声音兜底。
- 不在时间线渲染阶段额外混音；已有音轨继续由现有 FFmpeg 流程保留。
- 不补造缺失的 TTS、配音或音效资产生成能力。

## 验收标准

1. 有参考音频时，Ark `content` 包含 `reference_audio`，且 `generate_audio=true`。
2. 没有参考音频时，Ark `content` 不包含虚假的音频项，但仍设置 `generate_audio=true`。
3. 仅缺少参考音频时素材检查仍返回 `ready=true`，同时保留 `missing_audio` 提示；缺少视觉主体时仍为 `ready=false`。
4. 视频 Provider 专项测试与 Ruff 检查通过。
5. 诊断结果明确区分 `audio_elements` 声音描述和 `AssetEntry(kind=audio)` 实际参考音频文件。
