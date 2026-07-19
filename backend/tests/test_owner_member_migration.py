"""团队项目 owner 成员存量修复的 PostgreSQL 迁移测试。"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.engine import make_url

_BACKEND_DIR = Path(__file__).resolve().parents[1]
_PYTHON = _BACKEND_DIR / ".venv" / "bin" / "python"
_ALEMBIC_CONFIG = _BACKEND_DIR / "alembic.ini"
_DATABASE_URL_ENV = "MTV_TEST_POSTGRES_URL"
_TEMP_DATABASE_PREFIX = "mtv_owner_member_migration_"
_TEMP_DATABASE_PATTERN = re.compile(rf"{_TEMP_DATABASE_PREFIX}[0-9a-f]{{32}}")
_IDENTITY_QUERY_KEYS = frozenset(
    {
        "database",
        "dbname",
        "dsn",
        "host",
        "passfile",
        "password",
        "port",
        "service",
        "servicefile",
        "user",
        "username",
    }
)
_NO_DOTENV_ALEMBIC_RUNNER = """
import sys
from pydantic_settings.sources.providers.dotenv import DotEnvSettingsSource
DotEnvSettingsSource._read_env_files = lambda self: {}
from alembic.config import CommandLine
CommandLine(prog="alembic").main(argv=["-c", sys.argv[1], *sys.argv[2:]])
"""


@dataclass(frozen=True)
class _DatabaseTarget:
    database: str
    host: str
    port: str
    user: str


@dataclass(frozen=True)
class _OwnerMigrationDatabase:
    url: str = field(repr=False)
    management_database: str
    temporary_database: str


_ACTIVE_TEMP_TARGETS: set[_DatabaseTarget] = set()
_ASYNCPG_DIALECT = PGDialect_asyncpg()


def _dialect_connect_kwargs(url: str) -> dict[str, object]:
    parsed = make_url(url)
    if parsed.drivername != "postgresql+asyncpg":
        raise AssertionError(f"{_DATABASE_URL_ENV} 必须使用 postgresql+asyncpg 驱动")
    positional, keyword = _ASYNCPG_DIALECT.create_connect_args(parsed)
    if positional:
        raise AssertionError("asyncpg 连接目标必须完全由具名参数表达")
    return dict(keyword)


def _database_target(url: str) -> _DatabaseTarget:
    keyword = _dialect_connect_kwargs(url)
    database = keyword.get("database")
    if not isinstance(database, str) or not database:
        raise AssertionError(f"{_DATABASE_URL_ENV} 必须显式指定数据库名")
    return _DatabaseTarget(
        database=database,
        host=repr(keyword.get("host")),
        port=repr(keyword.get("port")),
        user=repr(keyword.get("user")),
    )


async def _connect_database(url: str) -> asyncpg.Connection:
    keyword = _dialect_connect_kwargs(url)
    accepted = {
        "command_timeout",
        "database",
        "direct_tls",
        "gsslib",
        "host",
        "krbsrvname",
        "max_cacheable_statement_size",
        "max_cached_statement_lifetime",
        "passfile",
        "password",
        "port",
        "server_settings",
        "ssl",
        "statement_cache_size",
        "target_session_attrs",
        "timeout",
        "user",
    }
    return await asyncpg.connect(
        **{key: value for key, value in keyword.items() if key in accepted}
    )


def _database_url() -> str:
    url = os.environ.get(_DATABASE_URL_ENV)
    if not url:
        pytest.skip(f"未配置 {_DATABASE_URL_ENV}，跳过真实 PostgreSQL 迁移测试")
    parsed = make_url(url)
    overridden = sorted(
        key for key in parsed.query if key.lower() in _IDENTITY_QUERY_KEYS
    )
    if overridden:
        raise AssertionError(
            "禁止在 query 中覆盖数据库连接身份: " + ", ".join(overridden)
        )
    normalized = parsed.render_as_string(hide_password=False)
    _database_target(normalized)
    return normalized


def _temporary_database_url(management_url: str, database_name: str) -> str:
    parsed = make_url(management_url).difference_update_query(_IDENTITY_QUERY_KEYS)
    return parsed.set(database=database_name).render_as_string(hide_password=False)


def _head_revision() -> str:
    config = Config(str(_ALEMBIC_CONFIG))
    config.set_main_option("script_location", str(_BACKEND_DIR / "alembic"))
    revision = ScriptDirectory.from_config(config).get_current_head()
    assert isinstance(revision, str)
    return revision


async def _database_revision(connection: asyncpg.Connection) -> str:
    revision = await connection.fetchval("SELECT version_num FROM alembic_version")
    assert isinstance(revision, str)
    return revision


async def _public_schema_fingerprint(
    connection: asyncpg.Connection,
) -> tuple[tuple[object, ...], ...]:
    rows = await connection.fetch(
        """
        SELECT target.relname,
               target.relkind::text,
               attribute.attname,
               format_type(attribute.atttypid, attribute.atttypmod),
               attribute.attnotnull,
               pg_get_expr(default_value.adbin, default_value.adrelid)
        FROM pg_class AS target
        JOIN pg_namespace AS namespace ON namespace.oid = target.relnamespace
        LEFT JOIN pg_attribute AS attribute
          ON attribute.attrelid = target.oid
         AND attribute.attnum > 0
         AND NOT attribute.attisdropped
        LEFT JOIN pg_attrdef AS default_value
          ON default_value.adrelid = attribute.attrelid
         AND default_value.adnum = attribute.attnum
        WHERE namespace.nspname = 'public'
        ORDER BY target.relname, target.relkind, attribute.attnum
        """
    )
    return tuple(tuple(row) for row in rows)


async def _database_ddl(
    connection: asyncpg.Connection,
    *,
    operation: str,
    database_name: str,
) -> None:
    if _TEMP_DATABASE_PATTERN.fullmatch(database_name) is None or len(database_name) > 63:
        raise AssertionError(f"拒绝不安全的临时数据库名: {database_name!r}")
    if operation == "create":
        statement = await connection.fetchval(
            "SELECT format('CREATE DATABASE %I', $1::text)",
            database_name,
        )
    elif operation == "drop":
        statement = await connection.fetchval(
            "SELECT format('DROP DATABASE %I', $1::text)",
            database_name,
        )
    else:
        raise AssertionError(f"不支持的数据库 DDL 操作: {operation}")
    assert isinstance(statement, str)
    await connection.execute(statement)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def owner_migration_database() -> AsyncIterator[_OwnerMigrationDatabase]:
    management_url = _database_url()
    management_target = _database_target(management_url)
    temporary_database = f"{_TEMP_DATABASE_PREFIX}{uuid4().hex}"
    temporary_url = _temporary_database_url(management_url, temporary_database)
    temporary_target = _database_target(temporary_url)
    if temporary_target.database == management_target.database:
        raise AssertionError("临时数据库不得与管理数据库同名")

    management = await _connect_database(management_url)
    sentinel = uuid4().hex
    sentinel_created = False
    expected_head: str | None = None
    baseline_fingerprint: tuple[tuple[object, ...], ...] | None = None
    try:
        can_create_database = await management.fetchval(
            """
            SELECT role.rolsuper OR role.rolcreatedb
            FROM pg_roles AS role
            WHERE role.rolname = current_user
            """
        )
        if can_create_database is not True:
            pytest.fail(
                f"{_DATABASE_URL_ENV} 的连接角色必须具有 CREATEDB 或超级用户权限"
            )
        expected_head = _head_revision()
        assert await _database_revision(management) == expected_head
        baseline_fingerprint = await _public_schema_fingerprint(management)
        await management.execute(
            """
            CREATE TEMP TABLE owner_migration_management_sentinel (
                marker varchar(64) PRIMARY KEY
            ) ON COMMIT PRESERVE ROWS
            """
        )
        await management.execute(
            "INSERT INTO owner_migration_management_sentinel (marker) VALUES ($1)",
            sentinel,
        )
        sentinel_created = True
        await _database_ddl(
            management,
            operation="create",
            database_name=temporary_database,
        )
        _ACTIVE_TEMP_TARGETS.add(temporary_target)
        yield _OwnerMigrationDatabase(
            url=temporary_url,
            management_database=management_target.database,
            temporary_database=temporary_database,
        )
    finally:
        _ACTIVE_TEMP_TARGETS.discard(temporary_target)
        try:
            exists = await management.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
                temporary_database,
            )
            if exists:
                await management.execute(
                    """
                    SELECT pg_terminate_backend(activity.pid)
                    FROM pg_stat_activity AS activity
                    WHERE activity.datname = $1
                      AND activity.pid <> pg_backend_pid()
                    """,
                    temporary_database,
                )
                await _database_ddl(
                    management,
                    operation="drop",
                    database_name=temporary_database,
                )
            assert not await management.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
                temporary_database,
            )
            if sentinel_created:
                assert (
                    await management.fetchval(
                        "SELECT marker FROM owner_migration_management_sentinel"
                    )
                    == sentinel
                )
            if baseline_fingerprint is not None:
                assert await _public_schema_fingerprint(management) == baseline_fingerprint
            if expected_head is not None:
                assert await _database_revision(management) == expected_head
        finally:
            await management.close()


def _run_alembic(url: str, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    if _database_target(url) not in _ACTIVE_TEMP_TARGETS:
        raise AssertionError("拒绝在非本模块注册的临时数据库上执行 Alembic")
    environment = os.environ.copy()
    environment.update(
        {
            "APP_ENV": "test",
            "DATABASE_URL": url,
            "SESSION_SECRET": "test-only-session-secret-at-least-32-chars",
        }
    )
    result = subprocess.run(
        [
            str(_PYTHON),
            "-c",
            _NO_DOTENV_ALEMBIC_RUNNER,
            str(_ALEMBIC_CONFIG),
            *arguments,
        ],
        cwd=_BACKEND_DIR,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        pytest.fail(f"Alembic {' '.join(arguments)} 失败:\n{result.stdout}\n{result.stderr}")
    return result


def _expected_owner_member_id(script_id: str) -> str:
    digest = hashlib.md5(
        f"mtv-team-owner:{script_id}".encode(),
        usedforsecurity=False,
    ).hexdigest()
    return f"own-{digest}"


def _reset_to_revision(url: str, revision: str) -> None:
    _run_alembic(url, "downgrade", "base")
    _run_alembic(url, "upgrade", revision)


async def _seed_owner_repair_cases(url: str) -> None:
    connection = await _connect_database(url)
    try:
        await connection.execute(
            """
            INSERT INTO organizations (id, org_name, org_code)
            VALUES ('org-owner-migration', '迁移测试组织', 'owner-migration')
            """
        )
        await connection.execute(
            """
            INSERT INTO users (
                user_id, username, password_hash, enabled, is_admin, account_type,
                org_id, department_id, can_manage_accounts, management_scope
            )
            VALUES
                ('owner-missing', 'owner-missing', 'hash', true, false, 'team',
                 'org-owner-migration', NULL, false, 'none'),
                ('owner-demoted', 'owner-demoted', 'hash', true, false, 'team',
                 'org-owner-migration', NULL, false, 'none'),
                ('owner-real', 'owner-real', 'hash', false, false, 'team',
                 'org-owner-migration', NULL, false, 'none'),
                ('owner-fake', 'owner-fake', 'hash', true, false, 'team',
                 'org-owner-migration', NULL, false, 'none')
            """
        )
        await connection.execute(
            """
            INSERT INTO scripts (
                id, title, scope, owner_user_id, org_id, created_by_user_id
            )
            VALUES
                ('script-missing', '缺失 owner 成员', 'team', 'owner-missing',
                 'org-owner-migration', 'owner-missing'),
                ('script-demoted', '真实 owner 被降级', 'team', 'owner-demoted',
                 'org-owner-migration', 'owner-demoted'),
                ('script-fake-owner', '存在虚假 owner', 'team', 'owner-real',
                 'org-owner-migration', 'owner-real')
            """
        )
        await connection.execute(
            """
            INSERT INTO project_members (id, script_id, user_id, project_role)
            VALUES
                ('member-demoted', 'script-demoted', 'owner-demoted', 'viewer'),
                ('member-real', 'script-fake-owner', 'owner-real', 'editor'),
                ('member-fake', 'script-fake-owner', 'owner-fake', 'owner')
            """
        )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_owner_migration_uses_only_registered_temporary_database(
    owner_migration_database: _OwnerMigrationDatabase,
) -> None:
    assert owner_migration_database.temporary_database != (
        owner_migration_database.management_database
    )
    assert (
        _TEMP_DATABASE_PATTERN.fullmatch(owner_migration_database.temporary_database)
        is not None
    )
    with pytest.raises(AssertionError, match="注册的临时数据库"):
        _run_alembic(_database_url(), "current")


@pytest.mark.asyncio
async def test_owner_member_migration_repairs_legacy_rows_and_round_trips(
    owner_migration_database: _OwnerMigrationDatabase,
) -> None:
    url = owner_migration_database.url
    _reset_to_revision(url, "d4b7f9c2a610")
    await _seed_owner_repair_cases(url)

    _run_alembic(url, "upgrade", "head")

    connection = await _connect_database(url)
    try:
        rows = await connection.fetch(
            """
            SELECT script.id AS script_id, member.id, member.user_id, member.project_role
            FROM scripts AS script
            JOIN project_members AS member ON member.script_id = script.id
            WHERE script.id IN ('script-missing', 'script-demoted', 'script-fake-owner')
            ORDER BY script.id, member.user_id
            """
        )
        by_script = {
            script_id: [row for row in rows if row["script_id"] == script_id]
            for script_id in {row["script_id"] for row in rows}
        }

        missing_members = by_script["script-missing"]
        assert len(missing_members) == 1
        assert dict(missing_members[0]) == {
            "script_id": "script-missing",
            "id": _expected_owner_member_id("script-missing"),
            "user_id": "owner-missing",
            "project_role": "owner",
        }
        assert by_script["script-demoted"][0]["project_role"] == "owner"
        fake_owner_roles = {
            row["user_id"]: row["project_role"] for row in by_script["script-fake-owner"]
        }
        assert fake_owner_roles == {"owner-fake": "manager", "owner-real": "owner"}
        assert all(len(row["id"]) <= 36 for row in rows)
    finally:
        await connection.close()

    # downgrade 明确为 no-op：数据保持修复后状态；再升级不得重复插入。
    _run_alembic(url, "downgrade", "d4b7f9c2a610")
    _run_alembic(url, "upgrade", "head")
    connection = await _connect_database(url)
    try:
        owner_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM project_members
            WHERE script_id = 'script-missing'
              AND user_id = 'owner-missing'
              AND project_role = 'owner'
            """
        )
        assert owner_count == 1
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_owner_member_migration_fails_closed_for_missing_user(
    owner_migration_database: _OwnerMigrationDatabase,
) -> None:
    url = owner_migration_database.url
    _reset_to_revision(url, "d4b7f9c2a610")
    connection = await _connect_database(url)
    try:
        await connection.execute(
            """
            INSERT INTO scripts (id, title, scope, owner_user_id, org_id, created_by_user_id)
            VALUES (
                'script-invalid-owner', '无效 owner', 'team', 'missing-user', NULL, 'missing-user'
            )
            """
        )
    finally:
        await connection.close()

    result = _run_alembic(url, "upgrade", "head", check=False)
    assert result.returncode != 0
    assert "团队项目 owner_user_id 未对应有效 users 行，拒绝迁移" in result.stderr

    connection = await _connect_database(url)
    try:
        revision = await connection.fetchval("SELECT version_num FROM alembic_version")
        assert revision == "d4b7f9c2a610"
    finally:
        await connection.close()
