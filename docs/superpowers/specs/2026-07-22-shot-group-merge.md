# 镜头组合并实现规格

## 目标

新增 `POST /v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/merge`。前端提交本集全部小分镜 ID，后端按场景、LLM 叙事边界和服务端顺序自动生成多个 3～15 秒的视频单元，并返回更新后的 `VideoWorkbenchOut`。

## 约束

- 使用 `Action.EDIT`；请求携带 `shot_ids` 与 `expected_episode_version`。
- `shot_ids` 必须完整覆盖当前分集，服务端不使用请求数组顺序重排，也不跨场景组合。
- 分镜生成阶段使用 `draft:` 组保存小分镜；正式视频单元使用 `group:`，每组总时长 3～15 秒。
- 涉及草稿源组已有任意视频任务、分集有活动分镜任务、状态混杂或版本过期时返回 `409`。
- 在单事务内生成多个正式组并重算镜头归属、编号、时间轴、Prompt、素材、音频、时长、参考帧和场景计数；成功后 `episode_version + 1`。
- 不改变数据库结构，不触发视频生成或积分操作。

## 验收

- 成功响应、`shot-hierarchy` 与 `video-workbench` 的分组和顺序一致。
- 漏传/外部/未知 ID、旧版本、非法时长和已有视频任务均不会产生部分写入。
- 新接口进入 OpenAPI，并同步前后端接口文档。
- 相关 Pytest、Ruff 与 Mypy 通过。
