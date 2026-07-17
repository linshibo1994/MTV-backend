# Repository Guidelines

## 项目结构与模块组织

本仓库当前以 `backend/` 为核心，使用 Python 3.12、FastAPI、SQLAlchemy、Celery 与 Alembic。HTTP 路由位于 `backend/app/api/v1/`，业务逻辑放在 `services/`，数据库访问与模型分别放在 `crud/`、`models/`，第三方 AI/存储适配器位于 `providers/`，异步任务位于 `workers/`。测试集中在 `backend/tests/`，迁移脚本位于 `backend/alembic/versions/`，产品与架构资料见 `docs/`。新增功能应遵循现有分层，避免在路由中堆积业务逻辑。

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
