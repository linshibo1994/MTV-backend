"""测试公共 fixtures。

不依赖真实 PostgreSQL / Redis：端点测试通过 dependency_overrides 注入 mock
会话，权限矩阵与 check_script_access 测试用 mock db 直接断言逻辑。
"""

import os

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic_settings.sources.providers.dotenv import DotEnvSettingsSource

_ORIGINAL_DOTENV_READER = DotEnvSettingsSource._read_env_files


def _empty_dotenv_source(self) -> dict[str, str | None]:
    """测试收集期彻底禁用 dotenv source，避免读取宿主配置。"""
    return {}


DotEnvSettingsSource._read_env_files = _empty_dotenv_source

_TEST_ENV = {
    "APP_ENV": "test",
    "SESSION_SECRET": "test-only-session-secret-at-least-32-chars",
    "CORS_ORIGINS": '["http://localhost:3000", "http://localhost:5173"]',
    "CORS_METHODS": '["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]',
    "CORS_HEADERS": (
        '["Accept", "Authorization", "Content-Type", "Idempotency-Key", '
        '"Range", "X-Requested-With"]'
    ),
    "DATABASE_URL": (
        "postgresql+asyncpg://test:test@/test?"
        "host=/private/tmp/mtv-tests-no-db.sock"
    ),
    "DATABASE_SYNC_URL": (
        "postgresql+psycopg2://test:test@/test?"
        "host=/private/tmp/mtv-tests-no-db.sock"
    ),
    "REDIS_URL": "redis+unix:///private/tmp/mtv-tests-no-redis.sock?db=0",
    "CELERY_BROKER_URL": "memory://",
    "CELERY_RESULT_BACKEND": "cache+memory://",
    "STORAGE_DRIVER": "local",
    "ALIYUN_OSS_ENDPOINT": "",
    "ALIYUN_OSS_ACCESS_KEY_ID": "",
    "ALIYUN_OSS_ACCESS_KEY_SECRET": "",
    "ALIYUN_OSS_BUCKET": "flowapp",
    "ALIYUN_OSS_REGION": "",
    "OPENAI_API_KEY": "",
    "GOOGLE_API_KEY": "",
    "VOLCENGINE_ACCESS_KEY": "",
    "VOLCENGINE_SECRET_KEY": "",
    "VOLCENGINE_API_KEY": "",
    "VOLCENGINE_ARK_BASE_URL": "https://ark.cn-beijing.volces.com/api/v3",
    "MGROUTER_API_KEY": "",
    "MGROUTER_BASE_URL": "https://mgrouter.com/api/v3",
    "MGROUTER_VIDEO_MODEL": "AICC-Doubao-Seedance-2.0",
    "MGROUTER_RESOLUTION": "720p",
    "MGROUTER_RATIO": "adaptive",
    "MGROUTER_GENERATE_AUDIO": "true",
    "MGROUTER_WATERMARK": "false",
    "VIDEO_GEN_API_KEY": "",
    "VIDEO_GEN_BASE_URL": "",
    "EDITOR_CAN_REVIEW": "false",
}
_ORIGINAL_ENV = {key: os.environ.get(key) for key in _TEST_ENV}


def _apply_test_environment() -> None:
    os.environ.update(_TEST_ENV)


# 普通 fixture 在测试收集后才执行，无法阻止宿主环境污染模块导入时创建 Settings。
# 在 conftest 导入和 pytest_configure 阶段覆盖，确保收集期间也使用稳定的测试配置。
_apply_test_environment()


def pytest_configure(config) -> None:
    _apply_test_environment()


def pytest_unconfigure(config) -> None:
    DotEnvSettingsSource._read_env_files = _ORIGINAL_DOTENV_READER
    for key, value in _ORIGINAL_ENV.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest_asyncio.fixture
async def client(monkeypatch):
    """提供 httpx AsyncClient 直连 ASGI app，并屏蔽 lifespan 中的 redis 关闭。"""
    async def _noop_close() -> None:
        return None

    monkeypatch.setattr("app.main.close_redis", _noop_close, raising=True)

    from app.core.database import get_db
    from app.main import create_app

    class _BlockedDatabaseSession:
        def __getattr__(self, name):
            raise AssertionError(
                f"测试路由未覆盖 get_db，禁止访问默认会话：{name}"
            )

    async def _blocked_db():
        yield _BlockedDatabaseSession()

    app = create_app()
    app.dependency_overrides[get_db] = _blocked_db
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            # 暴露 app 以便测试用 dependency_overrides
            c._app = app  # type: ignore[attr-defined]
            yield c
    app.dependency_overrides.clear()
