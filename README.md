# MTV Backend（智绘 Flow App 后端）

AI 影视制作流水线后端服务，覆盖「剧本与分集 → 主体及资产库 → 创作画布 → 分镜 → 视频片段 → 时间线成片」的完整链路。后端基于 FastAPI 提供 HTTP API，使用 Celery 承载耗时的生成与渲染任务，并通过 PostgreSQL、Redis 持久化业务数据与任务状态。

本仓库只包含后端工程，不包含前端。

## 核心能力

| 模块 | 说明 |
| --- | --- |
| 鉴权与积分 | 会话登录、团队项目成员权限、积分冻结/结算/释放/退款的完整闭环 |
| 剧本流水线 | 拆分、抽取、合并、补充、父子关系推导、定稿的后台 DAG |
| 资产库与主体 | 主体候选、场景与角色素材的沉淀与复用 |
| 创作画布 | 素材编排与画布节点管理 |
| 分镜与试镜 | 分集分镜生成、镜头调整、场景试镜 |
| 视频生成 | 多供应商视频生成任务、下载与媒体落库 |
| 时间线成片 | 片段时间线编排与成片渲染 |
| 媒体代理 | 资源级鉴权 + Range 流式读取的媒体访问通道 |

## 技术栈

- Python 3.12、FastAPI、Pydantic v2
- SQLAlchemy 2.0（异步）、Alembic、PostgreSQL 16
- Celery、Redis
- uv（依赖与虚拟环境管理）
- Pytest、Ruff、Mypy

## 目录结构

```text
.
├── backend/                 # 后端工程
│   ├── app/
│   │   ├── api/v1/          # HTTP 路由
│   │   ├── core/            # 配置、安全等基础设施
│   │   ├── crud/            # 数据库访问
│   │   ├── models/          # SQLAlchemy 模型
│   │   ├── schemas/         # Pydantic 数据结构
│   │   ├── services/        # 业务编排
│   │   ├── providers/       # AI / 存储供应商适配与注册表
│   │   └── workers/         # Celery 任务、Outbox 调度、流水线
│   ├── alembic/             # 数据库迁移
│   ├── tests/               # 测试用例
│   ├── deploy/              # 部署环境配置示例
│   ├── docker-compose.yml   # 本地基础设施与 Worker
│   └── pyproject.toml
├── scripts/                 # 部署脚本
├── start.sh                 # 本地一键启动入口
├── local.sh                 # 单机离线部署打包入口
└── deployremote.md          # 远程部署操作手册
```

## 快速开始

前置条件：Python 3.12、[uv](https://docs.astral.sh/uv/)、Docker 与 Docker Compose。

```bash
cd backend
uv sync --extra dev
cp .env.example .env        # 按需填写数据库、Redis 与模型供应商配置
```

启动本地完整服务栈（PostgreSQL、Redis、Celery Worker/Beat 与后端热重载）：

```bash
./start.sh start     # 启动
./start.sh status    # 查看状态
./start.sh logs      # 查看后端日志
./start.sh stop      # 停止，保留数据卷
```

也可以手动启动：

```bash
cd backend
docker compose up -d postgres redis celery-worker celery-beat
.venv/bin/alembic upgrade head
.venv/bin/uvicorn app.main:app --reload
```

启动后：

- 业务接口前缀为 `/v3api`，健康检查为 `GET /v3api/health`
- 本地接口文档：http://127.0.0.1:8000/v3api/docs
- 媒体代理前缀默认为 `/api/x/cctv/zhihui/flow-app`，可通过 `MEDIA_PROXY_PREFIX` 覆盖

## 配置说明

配置由 `backend/app/core/config.py` 统一读取，字段说明见 `backend/.env.example`。主要包括：

- 基础运行参数：`APP_ENV`、`APP_PORT`、`CORS_ORIGINS`
- 数据与任务：`DATABASE_URL`、`REDIS_URL`、`CELERY_BROKER_URL`
- 会话安全：`SESSION_SECRET` 等
- 存储：`STORAGE_DRIVER`（本地或阿里云 OSS）
- 模型供应商：文本、对话、图片、视频各自独立的 `*_BASE_URL`、`*_API_KEY`、`*_MODEL`
- 计费：各能力对应的积分价格项

模型能力通过 `backend/app/providers/registry.py` 注册与校验，请求可显式指定供应商与模型，未指定时按「厂商保底模型 → 代码内置兜底」回退。

请勿提交任何真实的 `.env`、密钥、证书或生产配置；仓库中的 `.env.example` 与 `backend/deploy/*.env` 仅包含非敏感默认值。

## 测试与代码检查

```bash
cd backend
.venv/bin/python -m pytest -q -ra
.venv/bin/ruff check app tests
.venv/bin/mypy app
```

测试会隔离 `.env`、数据库、Redis 与外部服务，外部 AI 与对象存储调用均需 mock，不应访问真实付费接口。

## 部署

- 远程部署流程、迁移与日志命令：见 [deployremote.md](deployremote.md)
- 部署脚本：`scripts/deploy-production.sh`、`scripts/deploy-test.sh`、`scripts/deploy-remote.sh`
- 单机离线打包：`local.sh`
- 环境配置模板：`backend/deploy/production.env.example`、`backend/deploy/test.env`

## 工程约定

- 异步任务统一经 `TaskDispatchOutbox` 事务投递到 Celery，不直接发布任务
- 可计费能力需同时处理任务状态与积分闭环
- 新增 AI、存储能力时接入 `providers/` 的抽象接口与注册表，不在路由或 Worker 中绑定具体供应商
- 数据库结构变更必须附带 Alembic 迁移，且不修改已发布的历史迁移
- 提交信息使用简洁中文动宾短语，例如 `feat(video): 支持新的视频生成供应商`
