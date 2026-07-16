# 后端完整修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复后端功能矩阵 T0-T10 与 H1/H2 的全部缺口，并用无真实 AI 的自动化证据完成验收。

**Architecture:** 两个并发开发波次按文件所有权隔离，主会话串行集成共享配置、模型注册、路由、Celery 与迁移。所有异步任务统一使用数据库 outbox、lease/CAS、输入版本和积分终态协议；Redis 只作缓存。

**Tech Stack:** Python 3.12、FastAPI、SQLAlchemy async、PostgreSQL 16、Redis 7、Celery 5、Alembic、pytest、Ruff、Mypy、uv。

---

## 文件所有权与波次

| 波次 | Worker | 模型 | 独占范围 |
|---|---|---|---|
| A | auth-credit | sol | T1/T2 的 core/auth、permissions、audit、credit API/CRUD/service/model/tests |
| A | pipeline-runtime | terra | T3、outbox/lease 公共实现、pipeline API/service/worker/model/tests |
| A | asset-canvas | terra | T4/T5 的 asset/material API/CRUD/service/schema/model/tests |
| B | generation-storyboard | terra | T6/T7 的 generation/storyboard/shots/provider/tests |
| B | video-media | sol | T8/T10 的 video/reference/storage/media/cover/tests |
| B | timeline | terra | T9 的 timeline API/CRUD/service/worker/schema/model/tests |
| C | contract-verifier | luna | OpenAPI、worker 注册、TASK 追踪、静态问题和 fake E2E 测试 |
| C | security-review | sol | 全 diff 事务、IDOR、SSRF、路径、敏感信息复审，只读 |

共享文件 `app/core/config.py`、`app/main.py`、`app/api/router.py`、
`app/models/__init__.py`、`app/workers/celery_app.py`、`pyproject.toml`、`uv.lock` 和 Alembic
只由主会话修改。Worker 不提交 Git；主会话每个波次核验后按指定文件提交。

覆盖映射：Task 1 -> T0.1/T0.2/T0.3；Task 2 -> T1.1/T1.2/T1.3、T2.1/T2.2/T2.3；
Task 3 -> T3.1/T3.2/T3.3/T3.4；Task 4 -> T4.1/T4.2/T4.3、T5.1/T5.2/T5.3；
Task 6 -> T6.1/T6.2/T6.3/T6.4、T7.1/T7.2/T7.3/T7.4；Task 7 ->
T8.1/T8.2/T8.3/T8.4/T8.5、T10.1/T10.2/T10.3；Task 8 -> T9.1/T9.2；
Task 9-10 -> H1/H2 和全部横向工程验收。

## Task 1：基线、公共契约与 T0

**Files:**
- Modify: `backend/app/core/config.py`
- Modify: `backend/app/core/errors.py`
- Modify: `backend/app/main.py`
- Modify: `backend/pyproject.toml`
- Create: `backend/tests/test_app_contract.py`
- Create: `backend/tests/test_error_contract.py`

- [ ] **Step 1: 写失败测试**

```python
def test_prod_rejects_default_session_secret() -> None:
    with pytest.raises(ValidationError):
        Settings(app_env="prod", session_secret="dev-only-change-me", _env_file=None)

def test_validation_error_has_only_detail(client) -> None:
    response = client.post("/v3api/auth/login", json={})
    assert response.status_code == 422
    assert set(response.json()) == {"detail"}
```

- [ ] **Step 2: 运行 RED**

Run: `.venv/bin/python -m pytest tests/test_app_contract.py tests/test_error_contract.py -q`
Expected: 默认 secret、CORS 或 422 形状至少一项失败。

- [ ] **Step 3: 最小实现**

为 Settings 增加生产 secret 校验和 CORS origin/method/header 配置；在 app factory 装配
CORSMiddleware；统一 422 为 `{detail: "字段校验失败"}`；保留 401 `未登录`。依赖锁在 Task 9 生成。

- [ ] **Step 4: 运行 GREEN**

Run: `.venv/bin/python -m pytest tests/test_app_contract.py tests/test_error_contract.py -q`
Expected: 全部通过。

## Task 2：T1 鉴权、多租户与 T2 积分

**Files:**
- Modify: `backend/app/core/auth.py`
- Modify: `backend/app/core/permissions.py`
- Modify: `backend/app/services/audit.py`
- Modify: `backend/app/api/v1/auth.py`
- Modify: `backend/app/api/v1/credits.py`
- Modify: `backend/app/crud/credit.py`
- Modify: `backend/app/services/credits.py`
- Modify: `backend/app/models/credit.py`
- Modify: `backend/app/models/credit_log.py`
- Modify: `backend/app/models/credit_reservation.py`
- Modify: `backend/tests/test_auth.py`
- Create: `backend/tests/test_permissions.py`
- Create: `backend/tests/test_credits.py`

- [ ] **Step 1: 写失败测试**

```python
@pytest.mark.parametrize("operation", ["reserve", "settle", "release", "refund", "grant"])
async def test_credit_operation_is_atomic_and_idempotent(operation, credit_context) -> None:
    await credit_context.assert_operation(operation)

@pytest.mark.parametrize("resource", ["script", "episode", "subject", "material", "shot_group", "job"])
async def test_cross_tenant_existing_resource_returns_403(resource, permission_context) -> None:
    response = await permission_context.request_other_tenant(resource)
    assert response.status_code == 403
```

补 Cookie 篡改/过期/logout/switch-org、成员移除、active_org 列表/创建/积分、审计失败仍 403、
部门向同部门用户 grant、identity map 刷新、disabled 清算、实际费用高低于预留、重复终态、
流水稳定排序和双账户锁顺序测试。

- [ ] **Step 2: 运行 RED**

Run: `.venv/bin/python -m pytest tests/test_auth.py tests/test_permissions.py tests/test_credits.py -q`
Expected: 旧 34 个失败复现，新增权限与积分测试失败。

- [ ] **Step 3: 最小实现**

权限全部使用 session `active_org_id`；平台/组织管理边界分离；审计 best-effort 且不改写拒绝状态。
积分锁查询使用 `populate_existing`，实现 Spec 4.4 五段状态机、稳定排序、actor/target 与数据库约束。

- [ ] **Step 4: 运行 GREEN**

Run: `.venv/bin/python -m pytest tests/test_auth.py tests/test_permissions.py tests/test_credits.py -q`
Expected: 全部通过且不连接非测试数据库。

## Task 3：T3 流水线与可靠异步运行时

**Files:**
- Create: `backend/app/models/task_dispatch_outbox.py`
- Create: `backend/app/services/task_dispatch.py`
- Create: `backend/app/core/job_state.py`
- Create: `backend/app/workers/outbox.py`
- Create: `backend/app/schemas/pipeline_contracts.py`
- Modify: `backend/app/models/subject.py`
- Modify: `backend/app/models/parent_subject.py`
- Modify: `backend/app/models/pipeline_job.py`
- Modify: `backend/app/models/pipeline_stage.py`
- Modify: `backend/app/api/v1/scripts.py`
- Modify: `backend/app/crud/script.py`
- Modify: `backend/app/services/pipeline/orchestrator.py`
- Modify: `backend/app/services/pipeline/split.py`
- Modify: `backend/app/services/pipeline/extract.py`
- Modify: `backend/app/services/pipeline/merge.py`
- Modify: `backend/app/services/pipeline/enrich.py`
- Modify: `backend/app/services/pipeline/parents.py`
- Modify: `backend/app/providers/llm.py`
- Modify: `backend/app/providers/llm_openai.py`
- Modify: `backend/app/workers/pipeline.py`
- Create: `backend/tests/test_pipeline.py`
- Create: `backend/tests/test_task_dispatch.py`

- [ ] **Step 1: 写失败测试**

```python
async def test_commit_before_publish_is_recovered(dispatch_context) -> None:
    job = await dispatch_context.create_without_publish()
    await dispatch_context.reconcile()
    assert await dispatch_context.publish_count(job.id) == 1

async def test_old_split_cannot_overwrite_new_episode_text(pipeline_context) -> None:
    old = await pipeline_context.start_split(version=1)
    await pipeline_context.save_new_text(version=2)
    await pipeline_context.finish(old)
    assert await pipeline_context.current_version() == 2
```

补 Celery chain/group 构造、outbox 重复发布、lease token/过期重领、stage reset、attempt 唯一、
ParentSubject FK、五个 LLM schema、12 段、sound 跳过、真实 breakdown 和结构化错误测试。

- [ ] **Step 2: 运行 RED**

Run: `.venv/bin/python -m pytest tests/test_pipeline.py tests/test_task_dispatch.py -q`
Expected: DAG API、版本覆盖、父子关系和可靠投递测试失败。

- [ ] **Step 3: 最小实现**

实现 TaskDispatchOutbox/dispatcher/reconciler 和 lease helper；使用 `celery.chain/group`；所有写回带
input_version 条件；Subject.parent_id 指向 ParentSubject；LLM 输出经 Pydantic v2 schema 验证。

- [ ] **Step 4: 运行 GREEN**

Run: `.venv/bin/python -m pytest tests/test_pipeline.py tests/test_task_dispatch.py -q`
Expected: 全部通过。

## Task 4：T4 资产审核与 T5 画布

**Files:**
- Modify: `backend/app/models/asset_entry.py`
- Modify: `backend/app/models/subject.py`（与 pipeline worker 串行交接后修改）
- Modify: `backend/app/models/materials.py`
- Modify: `backend/app/models/canvas_node.py`
- Modify: `backend/app/models/canvas_edge.py`
- Modify: `backend/app/api/v1/asset_library.py`
- Modify: `backend/app/api/v1/materials.py`
- Modify: `backend/app/crud/asset.py`
- Modify: `backend/app/crud/materials.py`
- Modify: `backend/app/services/review.py`
- Modify: `backend/app/schemas/asset.py`
- Modify: `backend/app/schemas/materials.py`
- Create: `backend/tests/test_asset_library.py`
- Create: `backend/tests/test_canvas.py`

- [ ] **Step 1: 写失败测试**

```python
async def test_second_concurrent_approval_gets_409(review_context) -> None:
    first, second = await review_context.concurrent_approve()
    assert sorted([first.status_code, second.status_code]) == [200, 409]

async def test_canvas_delta_rejects_stale_version(canvas_context) -> None:
    version = await canvas_context.version()
    await canvas_context.move_node(version)
    assert (await canvas_context.move_node(version)).status_code == 409
```

补 ensure 并发、候选 pending/superseded/delete、subject_id 影响摘要、完整节点 data round-trip、
prompt helper、孤儿边和跨 material 边拒绝测试。

- [ ] **Step 2: 运行 RED**

Run: `.venv/bin/python -m pytest tests/test_asset_library.py tests/test_canvas.py -q`
Expected: 审核竞争、ensure、版本和孤儿边测试失败。

- [ ] **Step 3: 最小实现并运行 GREEN**

Run: `.venv/bin/python -m pytest tests/test_asset_library.py tests/test_canvas.py -q`
Expected: 全部通过。

## Task 5：波次 A 集成

**Files:**
- Modify: `backend/app/models/__init__.py`
- Modify: `backend/app/workers/celery_app.py`
- Modify: `backend/app/api/router.py`（仅发现缺失聚合时）

- [ ] **Step 1: 主会话审阅三个 worker diff 和文件边界**
- [ ] **Step 2: 串行注册新模型、outbox task 和 generation/storyboard include**
- [ ] **Step 3: 运行波次 A 全测试**

Run: `.venv/bin/python -m pytest tests/test_auth.py tests/test_permissions.py tests/test_credits.py tests/test_pipeline.py tests/test_task_dispatch.py tests/test_asset_library.py tests/test_canvas.py -q`
Expected: 全部通过。

- [ ] **Step 4: 仅提交波次 A 文件**

Commit: `修复：完善基础事务与核心流水线`

## Task 6：T6 生图与 T7 分镜

**Files:**
- Modify: `backend/app/models/generation_job.py`
- Modify: `backend/app/models/storyboard_job.py`
- Modify: `backend/app/models/scene.py`
- Modify: `backend/app/models/shot_group.py`
- Modify: `backend/app/models/shot.py`
- Modify: `backend/app/api/v1/generation_jobs.py`
- Modify: `backend/app/api/v1/shots.py`
- Modify: `backend/app/crud/generation.py`
- Modify: `backend/app/crud/shot.py`
- Modify: `backend/app/crud/storyboard.py`
- Modify: `backend/app/services/generation.py`
- Modify: `backend/app/services/storyboard.py`
- Modify: `backend/app/workers/generation.py`
- Modify: `backend/app/workers/storyboard.py`
- Modify: `backend/app/providers/image_gen.py`
- Modify: `backend/app/providers/image_gen_google.py`
- Modify: `backend/app/providers/image_gen_openai.py`
- Modify: `backend/app/schemas/generation.py`
- Modify: `backend/app/schemas/shot.py`
- Create: `backend/app/schemas/storyboard_contracts.py`
- Create: `backend/tests/test_generation.py`
- Create: `backend/tests/test_storyboard.py`

- [ ] **Step 1: 写 RED 测试**

覆盖 Idempotency-Key、cancel、CAS/lease、服务端 resolved prompt、多图实际计费、候选 pending、
Storyboard episode_version、并发去重、旧任务栅栏、三层 schema、description、九条约束、
四类 materials、独立音频审核和无积分拒绝。

Run: `.venv/bin/python -m pytest tests/test_generation.py tests/test_storyboard.py -q`
Expected: 新测试失败。

- [ ] **Step 2: 最小实现并运行 GREEN**

Run: `.venv/bin/python -m pytest tests/test_generation.py tests/test_storyboard.py -q`
Expected: 全部通过。

## Task 7：T8 视频与 T10 媒体安全

**Files:**
- Modify: `backend/app/models/video_generation_job.py`
- Modify: `backend/app/models/video_provider.py`
- Modify: `backend/app/api/v1/video_generation.py`
- Modify: `backend/app/api/v1/media.py`
- Modify: `backend/app/crud/video.py`
- Modify: `backend/app/services/video_generation.py`
- Modify: `backend/app/services/reference_aggregator.py`
- Modify: `backend/app/workers/video_generation.py`
- Modify: `backend/app/providers/video_gen_volcengine.py`
- Modify: `backend/app/providers/video_gen.py`
- Modify: `backend/app/providers/storage.py`
- Modify: `backend/app/providers/storage_local.py`
- Modify: `backend/app/providers/storage_s3.py`
- Modify: `backend/app/services/cover.py`
- Create: `backend/app/services/media_security.py`
- Create: `backend/tests/test_video_generation.py`
- Create: `backend/tests/test_media.py`
- Create: `backend/tests/test_storage.py`

- [ ] **Step 1: 写 RED 测试**

```python
@pytest.mark.parametrize("key", ["../secret", "generate_images_evil/x", "upload_images/a/../../x"])
async def test_local_storage_rejects_escape(key, local_storage) -> None:
    with pytest.raises(ValueError):
        await local_storage.get(key)

async def test_nonexistent_shot_group_is_not_ready(video_client) -> None:
    response = await video_client.get_check_materials("missing")
    assert response.status_code == 404
```

补静态路由、镜头归属、数据库幂等、单活 Provider、outbox/settle/cancel、SSRF 每跳校验、
缺图/缺音/失效对象、自然序、尾帧栅栏、真实 MIME、对象清理、全 key 授权、Range 416、
signed_url、S3 fail-fast 和封面测试。

- [ ] **Step 2: 运行 RED、最小实现、运行 GREEN**

Run: `.venv/bin/python -m pytest tests/test_video_generation.py tests/test_media.py tests/test_storage.py -q`
Expected: 最终全部通过。

## Task 8：T9 时间线与持久化 RenderJob

**Files:**
- Create: `backend/app/models/render_job.py`
- Modify: `backend/app/models/video_timeline_arrangement.py`
- Modify: `backend/app/api/v1/timeline.py`
- Modify: `backend/app/crud/timeline.py`
- Modify: `backend/app/services/timeline_render.py`
- Modify: `backend/app/workers/timeline_render.py`
- Modify: `backend/app/schemas/timeline.py`
- Create: `backend/tests/test_timeline.py`

- [ ] **Step 1: 写 RED 测试**

覆盖 arrangement/job/script/episode 关联、可延迟 row_order、trim 参数、持久化 create/status、
Redis miss 回源、owner/Editor 取消、Viewer 403、终态 409、outbox 恢复、输入时间线快照、
storage async API 和 `User.user_id`。

Run: `.venv/bin/python -m pytest tests/test_timeline.py -q`
Expected: Redis-only、User.id、storage API、trim 和权限测试失败。

- [ ] **Step 2: 最小实现并运行 GREEN**

Run: `.venv/bin/python -m pytest tests/test_timeline.py -q`
Expected: 全部通过。

## Task 9：波次 B 集成、迁移与锁文件

**Files:**
- Modify: `backend/app/core/config.py`
- Modify: `backend/app/models/__init__.py`
- Modify: `backend/app/workers/celery_app.py`
- Modify: `backend/app/api/router.py`
- Modify: `backend/pyproject.toml`
- Create: `backend/uv.lock`
- Create: `backend/alembic/versions/0001_initial_schema.py`
- Create: `backend/tests/test_migrations.py`
- Create: `backend/tests/test_worker_registration.py`

- [ ] **Step 1: 串行集成共享注册和非敏感安全配置**
- [ ] **Step 2: 生成并人工审查初始 migration**

Run: `.venv/bin/alembic revision --autogenerate -m 'initial schema'`
Expected: 单个 revision 覆盖全部 ORM；将文件规范命名为 `0001_initial_schema.py`。

- [ ] **Step 3: 在任务专用 PostgreSQL/Redis 容器验证**

Run: `.venv/bin/python -m pytest tests/test_migrations.py -q -m postgres`
Expected: upgrade、metadata 对比、downgrade、再次 upgrade 全通过且零 skip。

- [ ] **Step 4: 生成锁并验证**

Run: `uv lock && uv sync --extra dev && uv lock --check`
Expected: 退出 0，`uv.lock` 与 pyproject 一致，项目 `.venv/bin/ruff` 和 `.venv/bin/mypy` 可用。

- [ ] **Step 5: 波次 B 回归并提交**

Run: `.venv/bin/python -m pytest tests/test_generation.py tests/test_storyboard.py tests/test_video_generation.py tests/test_media.py tests/test_storage.py tests/test_timeline.py tests/test_worker_registration.py -q`
Expected: 全部通过。

Commit: `修复：补全生成媒体与时间线链路`

## Task 10：契约、fake E2E 与静态清零

**Files:**
- Create: `backend/tests/test_openapi_contract.py`
- Create: `backend/tests/test_backend_e2e.py`
- Modify: `backend/tests/conftest.py`
- Modify if still reported: `backend/app/api/v1/asset_library.py`
- Modify if still reported: `backend/app/api/v1/auth.py`
- Modify if still reported: `backend/app/api/v1/credits.py`
- Modify if still reported: `backend/app/api/v1/generation_jobs.py`
- Modify if still reported: `backend/app/api/v1/media.py`
- Modify if still reported: `backend/app/api/v1/timeline.py`
- Modify if still reported: `backend/app/core/permissions.py`
- Modify if still reported: `backend/app/crud/asset.py`
- Modify if still reported: `backend/app/crud/generation.py`
- Modify if still reported: `backend/app/crud/shot.py`
- Modify if still reported: `backend/app/crud/timeline.py`
- Modify if still reported: `backend/app/crud/video.py`
- Modify if still reported: `backend/app/models/asset_entry.py`
- Modify if still reported: `backend/app/models/canvas_edge.py`
- Modify if still reported: `backend/app/models/canvas_node.py`
- Modify if still reported: `backend/app/models/credit_log.py`
- Modify if still reported: `backend/app/models/department.py`
- Modify if still reported: `backend/app/models/episode.py`
- Modify if still reported: `backend/app/models/generation_job.py`
- Modify if still reported: `backend/app/models/materials.py`
- Modify if still reported: `backend/app/models/parent_subject.py`
- Modify if still reported: `backend/app/models/pipeline_job.py`
- Modify if still reported: `backend/app/models/pipeline_stage.py`
- Modify if still reported: `backend/app/models/project_member.py`
- Modify if still reported: `backend/app/models/scene.py`
- Modify if still reported: `backend/app/models/shot.py`
- Modify if still reported: `backend/app/models/shot_group.py`
- Modify if still reported: `backend/app/models/storyboard_job.py`
- Modify if still reported: `backend/app/models/subject.py`
- Modify if still reported: `backend/app/models/user.py`
- Modify if still reported: `backend/app/models/user_organization.py`
- Modify if still reported: `backend/app/models/video_generation_job.py`
- Modify if still reported: `backend/app/models/video_timeline_arrangement.py`
- Modify if still reported: `backend/app/providers/image_gen_google.py`
- Modify if still reported: `backend/app/providers/image_gen_openai.py`
- Modify if still reported: `backend/app/providers/storage.py`
- Modify if still reported: `backend/app/providers/storage_local.py`
- Modify if still reported: `backend/app/providers/storage_s3.py`
- Modify if still reported: `backend/app/providers/video_gen.py`
- Modify if still reported: `backend/app/providers/video_gen_volcengine.py`
- Modify if still reported: `backend/app/schemas/materials.py`
- Modify if still reported: `backend/app/schemas/shot.py`
- Modify if still reported: `backend/app/services/cover.py`
- Modify if still reported: `backend/app/services/credits.py`
- Modify if still reported: `backend/app/services/pipeline/__init__.py`
- Modify if still reported: `backend/app/services/pipeline/merge.py`
- Modify if still reported: `backend/app/services/review.py`
- Modify if still reported: `backend/app/services/storyboard.py`
- Modify if still reported: `backend/app/services/timeline_render.py`
- Modify if still reported: `backend/app/services/video_generation.py`
- Modify if still reported: `backend/app/workers/generation.py`
- Modify if still reported: `backend/app/workers/pipeline.py`
- Modify if still reported: `backend/app/workers/storyboard.py`
- Modify if still reported: `backend/app/workers/timeline_render.py`
- Modify if still reported: `backend/app/workers/video_generation.py`

- [ ] **Step 1: luna 建立 TASK 路由/schema/worker 机械断言和 RED 测试**
- [ ] **Step 2: fake Provider 跑剧本到 RenderJob succeeded**

```python
async def test_full_backend_flow(fake_backend) -> None:
    result = await fake_backend.run_script_to_render()
    assert result.render_status == "succeeded"
    assert result.credit_frozen == 0
    assert result.approved_references_only is True
```

- [ ] **Step 3: 全量验证并逐项清零**

Run:

```bash
.venv/bin/python -m pytest -q -ra
.venv/bin/ruff check app tests
.venv/bin/mypy app
.venv/bin/python -m compileall -q app tests
uv lock --check
```

Expected: 全部退出 0；pytest 零 failed/skipped/xfail；Ruff/Mypy 零项目错误。

- [ ] **Step 4: sol 只读安全/事务 review，主会话修复所有有效 finding 并重跑全量验证**
- [ ] **Step 5: 更新 `BACKEND-FUNCTION-MATRIX.md` 为最终证据并提交**

Commit: `测试：完成后端全链路验收`

## Task 11：完成审计与资源清理

- [ ] 检查 `git diff --stat`、`git diff`、`git status --short`，只保留本任务增量。
- [ ] 扫描敏感信息、无关改动、未完成标记和错误吞并。
- [ ] 停止本任务启动的容器/进程，使用 `docker ps`、`lsof` 和进程状态确认释放。
- [ ] 不停止用户既有 `127.0.0.1:18888` 服务。
- [ ] 报告所有相关提交哈希、`main` 分支、真实工作区状态和未推送状态。
