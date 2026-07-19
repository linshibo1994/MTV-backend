"""SQLAlchemy 2.0 async 数据库引擎与会话工厂。"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.models.storage_reference_guard import StorageReferenceGuardedSession

engine = create_async_engine(
    settings.database_url,
    echo=settings.is_dev,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    sync_session_class=StorageReferenceGuardedSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI 依赖：提供一个异步 DB 会话，请求结束自动回滚/关闭。"""
    async with async_session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
