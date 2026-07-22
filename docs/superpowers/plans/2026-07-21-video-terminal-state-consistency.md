# 视频任务终态一致性 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 Celery 视频后处理依赖缺失和失败终态字段矛盾，使真实视频任务可以完整成功，并让失败任务暴露准确、一致的状态。

**Architecture:** 保持现有“供应商生成 -> 结算 -> 本地末帧提取 -> 成功终态”链路不变；末帧提取失败时回滚本次结算并释放预留积分。运行时依赖由 Worker 镜像提供；状态一致性集中在 CRUD 终态写入函数；Worker 在任何可能回滚之前快照后续需要的标量字段，避免异步 ORM 隐式加载。

**Tech Stack:** Python 3.12、FastAPI、SQLAlchemy 2、Celery、Pytest、Docker、FFmpeg

---

### Task 1: 固化失败终态契约

**Files:**
- Modify: `backend/tests/test_video_generation.py`
- Modify: `backend/app/crud/video.py`

- [x] **Step 1: 写失败测试**

在现有 `finish_video_job` 测试附近增加断言：任务先持有 `progress=100`、`progress_stage="succeeded"`、`progress_message="视频生成完成"`，调用失败终态后必须变为 `progress_stage="failed"` 且文案固定为“视频生成失败”。

- [x] **Step 2: 运行 RED**

Run: `.venv/bin/python -m pytest tests/test_video_generation.py -k 'finish_video_job and failed' -q -ra`

Expected: FAIL，旧实现保留 `progress_stage="succeeded"`。

- [x] **Step 3: 最小实现**

在 `finish_video_job` 的 `status == "failed"` 分支集中写入：

```python
values.update(
    progress_stage="failed",
    progress_message="视频生成失败",
)
```

取消终态同步设置 `progress_stage="canceled"` 和取消文案，避免其他终态保留旧成功阶段。

- [x] **Step 4: 运行 GREEN**

Run: `.venv/bin/python -m pytest tests/test_video_generation.py -k 'finish_video_job' -q -ra`

Expected: PASS。

### Task 2: 消除回滚后的 ORM 隐式加载

**Files:**
- Modify: `backend/tests/test_video_generation.py`
- Modify: `backend/app/workers/video_generation.py`

- [x] **Step 1: 写失败测试**

扩展末帧失败回归：在 `_best_effort_rollback` 返回后让原 `job.owner_user_id` 不可读取，验证任务仍以 `frame_extraction_failed` 结束，而不是落入外层 `provider_or_storage_error`。

- [x] **Step 2: 运行 RED**

Run: `.venv/bin/python -m pytest tests/test_video_generation.py::test_worker_frame_failure_rolls_back_settle_releases_and_deletes_video -q -ra`

Expected: FAIL，暴露回滚后读取 `job.owner_user_id`。

- [x] **Step 3: 最小实现**

任务领取并加载 ORM 对象后立即保存后续会跨回滚边界使用的标量：

```python
actor_user_id = job.owner_user_id
job_provider_key = job.provider_key
unit_id = job.unit_id
```

将该执行函数中跨回滚边界使用的用户 ID、Provider key 和单元 ID 全部替换为局部标量；提交不确定和明确拒绝分支也必须覆盖。

- [x] **Step 4: 运行 GREEN**

Run: `.venv/bin/python -m pytest tests/test_video_generation.py::test_worker_frame_failure_rolls_back_settle_releases_and_deletes_video -q -ra`

Expected: PASS，失败类型保持 `frame_extraction_failed`。

### Task 3: 补齐 Worker 媒体运行时

**Files:**
- Modify: `backend/Dockerfile`

- [x] **Step 1: 添加镜像依赖**

在 Python 依赖安装前使用 Debian 包管理器安装 `ffmpeg`，并清理 apt 索引：

```dockerfile
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
```

- [x] **Step 2: 构建并验证**

Run: `docker compose build celery-worker`

Run: `docker compose run --rm --no-deps celery-worker sh -lc 'ffmpeg -version && ffprobe -version'`

Expected: 两个命令均退出 0。

### Task 4: 集成验证

**Files:**
- Verify: `backend/app/crud/video.py`
- Verify: `backend/app/workers/video_generation.py`
- Verify: `backend/tests/test_video_generation.py`
- Verify: `backend/Dockerfile`

- [x] **Step 1: 运行专项测试**

Run: `.venv/bin/python -m pytest tests/test_video_generation.py tests/test_video_worker_streaming.py -q -ra`

- [x] **Step 2: 运行静态检查**

Run: `.venv/bin/ruff check app/crud/video.py app/workers/video_generation.py tests/test_video_generation.py`

- [x] **Step 3: 检查工作区**

Run: `git diff --check`

确认只包含本次目标相关修改，且未覆盖用户已有变更。
