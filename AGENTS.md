# Repository Guidelines

## 项目结构与模块组织

本仓库当前以 `backend/` 为核心，使用 Python 3.12、FastAPI、SQLAlchemy、Celery 与 Alembic。HTTP 路由位于 `backend/app/api/v1/`，业务逻辑放在 `services/`，数据库访问与模型分别放在 `crud/`、`models/`，第三方 AI/存储适配器位于 `providers/`，异步任务位于 `workers/`。测试集中在 `backend/tests/`，迁移脚本位于 `backend/alembic/versions/`，产品与架构资料见 `docs/`。新增功能应遵循现有分层，避免在路由中堆积业务逻辑。

## 项目定位与核心链路

这是「智绘 Flow App」的 AI 影视制作流水线后端；仓库不包含前端工程。业务 API 统一挂载在 `/v3api`，媒体代理单独挂载在 `/api/x/cctv/zhihui/flow-app`，本地接口文档位于 `/v3api/docs`。

主链路为：剧本与分集 → 主体及资产库 → 创作画布 → 分镜 → 视频片段 → 时间线成片。剧本流水线的 DAG 为 `split → extract → merge → (enrich 与 parents 并行) → finalize`。涉及 AI 生成、分镜、视频、时间线或媒体存储的工作，通常都对应持久化 Job、积分记录与后台任务，不能仅从 HTTP 请求的同步路径判断其完整行为。

## 关键设计约束

- 异步任务必须通过 `TaskDispatchOutbox` 的事务性投递链路进入 Celery；不要在 API 或 Service 中绕开 Outbox 直接发布任务。实现或修改任务时，要同时审查幂等键、租约、重试、取消和最终状态回写。
- 积分操作遵循冻结、结算、释放或退款的闭环。涉及可计费能力时，沿用 `services/credits.py` 和现有 Job 的事务边界，避免只更新任务状态或只更新积分余额。
- 新增或修改 AI、图片、视频、存储能力时，应接入 `providers/` 的抽象接口与注册表；不要在路由、Worker 或业务 Service 中直接绑定某个供应商 SDK。默认测试必须 mock 外部供应商。
- 媒体文件经 `StorageProvider` 管理，并通过媒体代理进行资源级鉴权和 Range 流式读取。不得为方便调试绕过权限校验直接暴露本地路径、对象存储地址或上传凭证。
- 变更持久化模型必须同步评估 Alembic 迁移、现有历史数据兼容、Job 状态机和 OpenAPI 契约；迁移文件按仓库既有中文命名风格编写，且不得修改已经发布的历史迁移。
- `backend/tests/conftest.py` 会隔离 `.env`、数据库、Redis 与外部服务。测试失败时优先补齐依赖覆盖或 mock，而不是连接开发机的真实基础设施。

## 关键入口

| 位置 | 职责 |
| --- | --- |
| `backend/app/main.py` | 应用工厂、CORS、Session、异常处理、业务与媒体路由挂载 |
| `backend/app/api/router.py` | `/v3api` 路由聚合与健康检查 |
| `backend/app/services/` | 业务编排、积分、审计、存储引用与任务投递 |
| `backend/app/workers/` | Celery 注册、Outbox 调度、Pipeline、生成、渲染与媒体后台任务 |
| `backend/app/providers/registry.py` | 模型与存储 Provider 的能力校验、归一化和注册 |
| `backend/app/core/config.py` | 环境配置、供应商回退与安全校验；不要读取或提交真实 `.env` |
| `docs/项目架构图.md` | 当前实现对应的架构总览和关键代码定位 |

## 构建、测试与本地开发

以下命令均从 `backend/` 执行：

```bash
uv sync --extra dev                         # 安装并锁定开发依赖
docker compose up -d postgres redis         # 启动本地基础设施
.venv/bin/alembic upgrade head              # 应用数据库迁移
.venv/bin/uvicorn app.main:app --reload     # 启动 API（默认 8000 端口）
.venv/bin/python -m pytest -q -ra            # 运行全部测试
.venv/bin/ruff check app tests               # 检查格式、导入与常见错误
.venv/bin/mypy app                           # 执行静态类型检查
```

提交前至少运行与改动相关的专项测试；依赖或公共契约变更需运行完整测试、Ruff 和 Mypy。

## 编码风格与命名

使用 4 空格缩进、完整类型标注和简短中文注释。模块、函数与变量采用 `snake_case`，类采用 `PascalCase`，常量采用 `UPPER_SNAKE_CASE`。Ruff 目标版本为 Python 3.12，行宽 110；不要手工调整导入顺序，使用 `ruff check --fix` 处理安全的机械修改。Provider 应通过抽象接口和注册表接入，配置集中在 `app/core/`。

## 测试规范

项目使用 Pytest、pytest-asyncio 与 HTTPX。测试文件命名为 `test_<feature>.py`，测试函数命名为 `test_<behavior>`。异步模式已自动启用。新增行为须覆盖成功、权限拒绝、错误契约及并发/幂等边界；仓库暂无硬性覆盖率阈值。外部 AI、对象存储和付费 API 必须 mock，不得在默认测试中访问真实服务。

## 提交与拉取请求

Git 历史以简洁中文动宾短语为主，例如 `实现：统一多模态模型供应商适配`；文档提交也可使用 `docs(scope): 摘要`。每个提交只包含一个清晰目标。拉取请求需说明背景、行为变化、迁移或配置影响、已执行的验证命令，并关联相关 issue；API 契约变化附请求/响应示例，界面变化再附截图。不得提交 `.env`、密钥、证书、生成文件或本地存储内容。
