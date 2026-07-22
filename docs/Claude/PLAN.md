# 智绘 (Flow App) 后端复刻 — 实现计划 (PLAN)

> 版本: 1.0 | 日期: 2026-07-15 | 配套: [TASK.md](TASK.md)
> 技术栈: Python 3.12 + FastAPI 0.115+ + SQLAlchemy 2.0(async) + Pydantic v2 + PostgreSQL 16 + Redis 7 + Celery 5
> 参考规格: `docs/Claude/01~08`

---

## 1. 技术选型定稿

| 层 | 选型 | 理由 |
|---|---|---|
| Web 框架 | FastAPI 0.115+ | async、Pydantic v2、自动 OpenAPI、文档允许 |
| ORM | SQLAlchemy 2.0 (async, AsyncSession) | 成熟、JSON 列、行级锁、迁移 |
| 迁移 | Alembic | SQLAlchemy 配套 |
| 校验 | Pydantic v2 | FastAPI 原生、性能 |
| 数据库 | PostgreSQL 16 | 多租户、JSONB (input_json/materials/reference_frames)、UUID |
| 缓存/队列 | Redis 7 | Celery broker + 会话存储 |
| 异步任务 | Celery 5 (prefetch=1, acks_late=True) | AI job 链式编排 (split→extract→merge→enrich→parents) |
| 会话 | Starlette SessionMiddleware (itsdangerous signed Cookie, httpOnly) | httpOnly Cookie 鉴权 |
| AI-LLM | OpenAI SDK (gpt-4o) | split/extract/merge/enrich/parents/storyboard |
| AI-文生图 | openai (gpt-image-2) + google-genai (gemini-3.1-flash-image = banana-2/Nano Banana) | 主体参考图 |
| AI-视频 | volcengine SDK (doubao-seedance-2-0-260128 = Seedance 2.0) | 多参考图图生视频，provider_key jianyu/modelsai |
| 对象存储 | 本地 FS (开发) / S3 兼容 MinIO (生产) | storage_key 抽象 |
| 媒体探测 | Pillow (图) / pydub (音频) / ffmpeg (视频 MIME/元数据) | 上传校验真实 MIME |
| 测试 | pytest + pytest-asyncio + httpx (AsyncClient) | API/单元测试 |
| 代码风格 | ruff + mypy | lint + 类型 |

---

## 2. 目录结构

```
backend/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app 工厂，挂载 /v3api 与媒体代理
│   ├── api/
│   │   ├── __init__.py
│   │   ├── router.py           # 聚合所有 router，prefix=/v3api
│   │   ├── v1/                 # 按资源域分文件
│   │   │   ├── auth.py         # T1: /auth/*
│   │   │   ├── credits.py      # T2: /credits/*
│   │   │   ├── scripts.py      # T3: /scripts/*
│   │   │   ├── asset_library.py# T4: /scripts/<id>/asset-library/*
│   │   │   ├── materials.py    # T5: /materials/*
│   │   │   ├── generation_jobs.py # T6: /generation-jobs/*
│   │   │   ├── shots.py        # T7: /shots/* /scripts/<sid>/episodes/<eid>/shot-*
│   │   │   ├── video_generation.py # T8: /video-generation* /video-generation-jobs
│   │   │   ├── timeline.py     # T9: /video-timeline-arrangements*
│   │   │   └── media.py        # T10: /api/x/cctv/zhihui/flow-app/*
│   ├── core/
│   │   ├── config.py          # Settings (pydantic-settings)
│   │   ├── database.py         # async engine + session
│   │   ├── redis.py
│   │   ├── deps.py            # get_db/get_current_user/get_redis
│   │   ├── auth.py            # 会话解析、401
│   │   ├── permissions.py     # 授权检查顺序
│   │   ├── errors.py          # 统一 {"detail":...} 异常
│   │   └── ids.py             # UUID v4 生成
│   ├── models/                # SQLAlchemy ORM（每实体一文件）
│   │   ├── base.py            # DeclarativeBase + timestamps mixin
│   │   ├── organization.py department.py user.py user_organization.py project_member.py
│   │   ├── credit.py credit_log.py credit_reservation.py
│   │   ├── script.py episode.py subject.py parent_subject.py
│   │   ├── pipeline_stage.py pipeline_job.py
│   │   ├── asset_entry.py
│   │   ├── materials.py canvas_node.py canvas_edge.py
│   │   ├── generation_job.py
│   │   ├── scene.py shot_group.py shot.py
│   │   ├── video_generation_job.py video_provider.py
│   │   └── video_timeline_arrangement.py
│   ├── schemas/               # Pydantic（与 models 对应，分入参/出参）
│   ├── crud/                  # 数据访问层（每资源域一文件）
│   ├── services/              # 业务逻辑
│   │   ├── credits.py         # reserve/settle/release/refund/grant
│   │   ├── pipeline/          # split/extract/merge/enrich/parents
│   │   ├── storyboard.py
│   │   ├── reference_aggregator.py # 视频生成多参考图汇聚
│   │   └── cover.py           # SVG 确定性封面
│   ├── providers/             # AI 供应商抽象
│   │   ├── llm.py llm_openai.py
│   │   ├── image_gen.py image_gen_google.py image_gen_openai.py
│   │   ├── video_gen.py video_gen_volcengine.py
│   │   └── storage.py storage_local.py storage_s3.py
│   └── workers/
│       ├── celery_app.py
│       ├── pipeline.py        # split→extract→merge→enrich→parents 链
│       ├── generation.py      # generate_images worker
│       ├── video_generation.py
│       └── timeline_render.py # 合成最终视频
├── alembic/                   # 迁移
├── tests/                     # 测试
├── storage/                   # 本地存储根 (.gitignore)
├── .env.example
├── docker-compose.yml         # postgres + redis
├── pyproject.toml
├── TASK.md
└── PLAN.md
```

---

## 3. 数据模型映射 (ORM 设计要点)

### 3.1 公共 Mixin
```python
class TimestampMixin:
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at = mapped_column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
```

### 3.2 关键表设计

| 表 | 主键 | 关键索引/约束 |
|---|---|---|
| users | user_id (UUID PK) | username unique |
| organizations | id | org_code unique |
| departments | id | org_id FK |
| credit_accounts | id | unique(owner_type, owner_id) |
| credit_logs | id | idx(account_id, created_at desc), reservation_id idx |
| credit_reservations | id | unique(source_type, source_id) 幂等 |
| scripts | id | idx(owner_user_id, scope) |
| episodes | episode_id | unique(script_id, episode_no), episode_version |
| subjects | id | idx(script_id), parent_id FK self |
| pipeline_stages | id | unique(script_id, stage) |
| generation_jobs | job_id | idx(material_id), idx(status) |
| video_generation_jobs | job_id | idx(script_id, episode_id) |
| canvas_nodes | id | idx(material_id), data JSONB |
| shots | shot_id | idx(shot_group_id) |

### 3.3 JSONB 字段
- `episodes.script_text`: Text（大文本）
- `pipeline_stages.detail/current/total`: JSONB
- `generation_jobs.input_json`: JSONB (含 resolved_prompts.final)
- `canvas_nodes.data`: JSONB (完整节点 data)
- `shot_groups.prompt/reference_frames/materials/audio_elements`: JSONB
- `video_generation_jobs.input_json`: JSONB (reference_images/names/prompt)

### 3.4 乐观锁
- Episode 保存：`UPDATE ... SET episode_version=episode_version+1, client_save_seq=client_save_seq+1 WHERE episode_id=? AND episode_version=?`，affected_rows==0 则 409 冲突。
- PipelineJob：input_version 绑定，旧任务成功不覆盖新文本。

---

## 4. API 实现契约

### 4.1 全局
- prefix: `/v3api`
- 错误体: 所有 HTTPException detail 统一 `{"detail": "<msg>"}`
- 401: `{"detail":"未登录"}`
- 409: 乐观锁冲突 `{"detail":"版本冲突，请刷新"}`
- 列表: `{"items":[...], "total":N}` 或 `{"items":[...]}`
- 分页: `limit`(默认 30, 最大 500) + `offset`

### 4.2 会话
- `SessionMiddleware`: secret_key=SESSION_SECRET, same_site=lax, httponly=True, https_only=生产
- 登录: `POST /v3api/auth/login {username,password}` → 校验 bcrypt → session["user_id"]=... → Set-Cookie + 额外下发 tgw_l7_route (兼容标识)
- `get_current_user` 依赖: 读 session，无 user_id → 401

### 4.3 端点清单 (40+ 端点，见 TASK 各模块)
实现顺序遵循依赖：T1→T2→T3→T4→T5→T6→T7→T8→T9，T10 与 T5 并行。

---

## 5. AI 供应商集成方案

### 5.1 Provider 抽象
```python
class LLMProvider(Protocol):
    async def complete_json(self, system: str, user: str, schema: dict) -> dict: ...

class ImageGenProvider(Protocol):
    async def generate(self, prompt: str, model: str, size: str, aspect: str) -> bytes: ...

class VideoGenProvider(Protocol):
    async def generate_all_reference(self, prompt, reference_images, reference_audios, model) -> str: ...  # 返回 video storage_key
```

### 5.2 模型映射
| 文档标识 | 实际模型 | SDK | 端点 |
|---|---|---|---|
| LLM (未暴露) | gpt-4o | openai | chat.completions (response_format=json) |
| banana-2 | gemini-3.1-flash-image (Nano Banana) | google-genai | generate_image |
| gpt-image-2 | gpt-image-2 | openai | images.generate |
| doubao-seedance-2-0-260128 | 豆包 Seedance 2.0 | volcengine | 视频生成 2.0 all_reference |

### 5.3 Provider 路由
- 文生图：按节点 `data.model` 字段路由 (banana-2 → google, gpt-image-2 → openai)
- 视频：按全局 active-provider (provider_key jianyu/modelsai) 路由，两者均底层走 Seedance (volcengine)，可切换
- LLM：单一 OpenAI (可配置)

### 5.4 密钥管理
- 全部走 env var，不入库不入 git
- Provider 客户端在 `providers/__init__.py` 单例初始化，按需懒加载
- 缺 key 时启动告警，对应任务失败并 release 积分 (不卡死 worker)

### 5.5 错误处理
- AI 调用失败 → job.status=failed, error_message 记录 → release 积分 → 不重试 (用户手动重试创建新 attempt)
- 超时 → Celery task time_limit → failed → release

---

## 6. 积分三段式实现要点

```python
# services/credits.py
async def reserve(db, account_id, source_type, source_id, estimated):
    acc = await db.execute(select(Account).where(Account.id==account_id).with_for_update())
    if acc.balance_points - acc.frozen_points < estimated: raise InsufficientCredits
    acc.frozen_points += estimated
    reservation = CreditReservation(id=uuid4(), account_id, source_type, source_id, reserved_points=estimated, status="reserved")
    log = CreditLog(log_type="reserve", frozen_delta=+estimated, balance_after=acc.balance_points, frozen_after=acc.frozen_points, reservation_id=reservation.id, ...)
    db.add_all([reservation, log]); await db.commit()

async def settle(db, reservation_id, actual):
    r = await db.get(reservation); acc = lock(r.account_id)
    acc.balance_points -= actual; acc.frozen_points -= r.reserved_points
    log(log_type="settle", points_delta=-actual, frozen_delta=-r.reserved_points, balance_after, frozen_after, reservation_id=r.id)
    r.status="captured"; commit()

async def release(db, reservation_id, reason):
    r = ...; acc = lock(r.account_id)
    acc.frozen_points -= r.reserved_points
    log(log_type="release", frozen_delta=-r.reserved_points, ..., note=reason)
    r.status="released"; commit()
```

- 行级锁 `with_for_update()` 保证并发安全
- reservation_id 串联 reserve/settle/release
- 幂等: unique(source_type, source_id) 防重复冻结

---

## 7. 多代理并发编排策略 (Workflow)

采用分阶段 pipeline，最小化跨模块等待：

### Phase 1: 基础设施 (串行，单 agent)
- agent: 搭建 app 骨架 (main/config/database/redis/deps/errors/ids)、models/base、docker-compose、pyproject、.env.example、alembic 初始化
- 输出: 可启动的空壳 + 公共基础

### Phase 2: 鉴权 + 积分 + 资产基础模型 (并行 3 agent，worktree 隔离)
- agent-A: T1 鉴权多租户 (models: org/dep/user/user_org/project_member + api/auth + core/auth + permissions)
- agent-B: T2 积分 (models: account/log/reservation + services/credits + api/credits)
- agent-C: T3+T4 共享 ORM 模型 (script/episode/subject/parent_subject/asset_entry + pipeline_stage/job)
  - 模型先行，避免 worktree 合并冲突；API/services 在 Phase 3

### Phase 3: 剧本流水线 + 资产库 API + LLM Provider (并行 3 agent)
- agent-D: T3 API (scripts/episodes/breakdown/pipeline-status/extract-status) + workers/pipeline (split/extract/merge/enrich/parents 链) + providers/llm + services/pipeline/*
- agent-E: T4 API (asset-library/subjects/parent-subjects/candidates/approve-image/approve-audio/entries/ensure) + 审核状态机
- agent-F: T5 画布 (materials/canvas_node/canvas_edge + api/materials + delta 增量保存) + T10 媒体代理 (providers/storage + api/media + SVG 封面)

### Phase 4: 生成任务 + 分镜 + 文生图 Provider (并行 3 agent)
- agent-G: T6 generation-jobs (models + api + providers/image_gen google/openai + workers/generation + 与资产库候选队列联动)
- agent-H: T7 shots (scene/shot_group/shot + api/shots + services/storyboard + LLM-5)
- agent-I: providers/video_gen volcengine (Seedance all_reference) + T8 models (video_generation_job/provider) + api/video-generation + workers/video_generation + reference_aggregator + 首尾帧

### Phase 5: 时间线 + 集成 (并行 2 agent)
- agent-J: T9 timeline (models + api + workers/timeline_render)
- agent-K: 横向集成 (main router 聚合、CORS、异常处理、健康检查、端到端冒烟测试)

### 并发与冲突控制
- 每个开发 agent 用 `isolation:"worktree"` 在独立 git worktree 写代码，避免文件冲突
- Phase 2 的共享模型先由 agent-C 在主 worktree 落地，Phase 3+ 基于合并后的主干
- 各 agent 只写自己模块文件，公共文件 (main.py/router.py) 由 Phase 5 的 agent-K 统一聚合
- 每个 agent 完成后自测 (import + 基本路由) 并返回完成的文件清单

---

## 8. 实现顺序与里程碑

| 里程碑 | 内容 | 标志 |
|---|---|---|
| M1 | 骨架可启动 | `GET /v3api/health` 200, 401 统一 |
| M2 | 鉴权+积分+模型 | 登录/me/credits 端点可用，三段式单元测试通过 |
| M3 | 剧本+资产+画布 | 创建剧本→保存→流水线触发→资产列表→画布 ensure 全链路 |
| M4 | 生成+分镜+视频 | 生图 job 轮询、分镜层级、视频生成多参考图汇聚 |
| M5 | 时间线+集成 | 时间线合成 + 端到端 1 剧本跑通 |

---

## 9. 测试策略

- **单元**: 积分三段式 (reserve/settle/release 并发)、乐观锁冲突、审核状态机、参考图汇聚
- **API 集成**: pytest-asyncio + httpx AsyncClient，覆盖 T1~T9 主要端点
- **Provider**: 用 `@pytest.mark.asyncio` mock SDK，验证 prompt 构造与错误处理
- **端到端**: 1 剧本全流程 (mock 文生图/视频返回占位，真实 LLM 可选)

---

## 10. 配置与启动

```bash
# 1. 启动依赖
docker compose up -d postgres redis

# 2. 安装依赖
pip install -e ".[dev]"

# 3. 迁移
alembic upgrade head

# 4. 配置 .env (填入真实 AI key)

# 5. 启 API
uvicorn app.main:app --reload --port 8000

# 6. 启 worker
celery -A app.workers.celery_app worker -l info
```

API 文档: `http://localhost:8000/v3api/docs` (FastAPI 自动生成 OpenAPI)

---

## 11. 风险与缓解

| 风险 | 缓解 |
|---|---|
| AI key 缺失致 worker 卡死 | 缺 key 时任务直接 failed + release，不阻塞队列 |
| 多代理 worktree 合并冲突 | 共享模型先于 API 落地；公共聚合文件单点收口 |
| Seedance all_reference 参数不明确 | 先按 volcengine 视频生成 2.0 文档实现，预留 provider 切换层；真实调试由用户提供 key |
| async + Celery 混用 | Celery task 内用同步 DB session (同步引擎) 或 `asyncio.run`；API 用 async session |
| 大文本 input_json | 用 JSONB，PG 索引按需 |
| 积分并发竞态 | 行级锁 + reservation 幂等 unique 约束 |

---

> 本计划与 TASK.md 配套，作为多代理开发的执行蓝图。各 agent 的 prompt 以本文件对应模块章节为约束。
