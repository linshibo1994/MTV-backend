"""Alembic 迁移环境 — 异步配置，自动导入 app.models 聚合表元数据。"""

import asyncio
from logging.config import fileConfig
from typing import Any

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

import app.models  # noqa: F401
from alembic import context
from app.core.config import settings
from app.models import Base  # noqa: F401  (触发各模型注册)
from app.models.storage_reference_guard import STORAGE_REFERENCE_GUARD_BYPASS

config = context.config
# Alembic 的 ConfigParser 会把 ``%`` 解释为插值语法；URL 中的百分号编码必须
# 先转义为 ``%%``，读取配置时会还原为原始 ``%``，不会解码或暴露凭据。
config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata
_LEGACY_TABLES_EXCLUDED_FROM_AUTOGENERATE = frozenset({"meizi_storage_objects"})


def include_object(
    _object: Any,
    name: str | None,
    type_: str,
    reflected: bool,
    _compare_to: Any,
) -> bool:
    """保留尚待数据确认的遗留表，避免 autogenerate 意外生成 DROP TABLE。"""
    return not (
        reflected
        and type_ == "table"
        and name in _LEGACY_TABLES_EXCLUDED_FROM_AUTOGENERATE
    )


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    connection = connection.execution_options(**{STORAGE_REFERENCE_GUARD_BYPASS: True})
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
