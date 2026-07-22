# 视频任务终态一致性 Spec

## 目标

修复视频供应商已返回并存储视频后，因为本地末帧提取依赖缺失而显示失败的问题；同时保证失败任务的主状态、阶段和文案一致，并保留准确的失败分类。

## 约束

- Celery Worker 镜像必须提供 `ffmpeg` 和 `ffprobe`，不依赖宿主机工具。
- 末帧提取仍是视频任务成功链路的一部分，失败时不得伪装成成功。
- 数据库回滚后不得读取可能已过期的 ORM 实例属性。
- 终态以 `status` 为权威；`progress_stage` 和 `progress_message` 必须与终态一致。
- 不修改前端的终态判定逻辑，因为它按 `status` 聚合是正确行为。
- 不覆盖或回滚当前工作区已有的无关改动。

## 验收标准

1. `finish_video_job(..., status="failed")` 会写入 `progress_stage="failed"`，并使用安全失败文案覆盖旧的“视频生成完成”。
2. 模拟末帧提取失败并在回滚后禁止访问原 ORM 对象时，任务仍准确记录为 `frame_extraction_failed`。
3. 模拟远端提交不确定或明确拒绝并在回滚后禁止访问原 ORM 对象时，仍能保存正确的 Provider 证据与终态。
4. 视频生成成功终态继续保持 `progress=100`、`progress_stage="succeeded"` 和“视频生成完成”。
5. 构建后的 Celery Worker 容器内 `ffmpeg`、`ffprobe` 均可执行。
6. 视频生成专项测试和 Ruff 检查通过。
