# 镜头组合并实现规格

## 目标

新增 `POST /v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/merge`，把同一场景中连续的 2～5 个镜头合并为一个新的视频生成单元，并返回更新后的 `VideoWorkbenchOut`。

## 约束

- 使用 `Action.EDIT`；请求携带 `shot_ids` 与 `expected_episode_version`。
- 服务端按现有叙事顺序处理，不使用请求数组顺序重排。
- 镜头必须同剧本、同分集、同场景且连续；新组及残余组均满足 2～5 镜头、3～15 秒。
- 涉及源组已有任意视频任务、分集有活动分镜任务或版本过期时返回 `409`。
- 在单事务内重算镜头归属、编号、时间轴、Prompt、素材、音频、时长、参考帧和场景计数；成功后 `episode_version + 1`。
- 不改变数据库结构，不触发视频生成或积分操作。

## 验收

- 成功响应、`shot-hierarchy` 与 `video-workbench` 的分组和顺序一致。
- 旧版本、跨场景、非连续、非法时长和已有视频任务均不会产生部分写入。
- 新接口进入 OpenAPI，并同步前后端接口文档。
- 相关 Pytest、Ruff 与 Mypy 通过。
