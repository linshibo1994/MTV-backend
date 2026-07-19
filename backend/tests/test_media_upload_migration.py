"""媒体上传状态机迁移的真实 PostgreSQL 往返测试。"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import CheckConstraint
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.engine import make_url

from app.models.media_upload import MediaUpload

_BACKEND_DIR = Path(__file__).resolve().parents[1]
_PYTHON = _BACKEND_DIR / ".venv" / "bin" / "python"
_ALEMBIC_CONFIG = _BACKEND_DIR / "alembic.ini"
_DATABASE_URL_ENV = "MTV_TEST_POSTGRES_URL"
_TEMP_DATABASE_PREFIX = "mtv_media_upload_migration_"
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
_PREVIOUS_REVISION = "e2a7c4d9b615"
_MEDIA_REVISION = "f4a8c2e1d907"
_LEGACY_JOB_ID = "media-upload-migration-legacy-job"
_BACKFILL_SCRIPT_ID = "media-upload-migration-script"
_BACKFILL_MATERIAL_ID = "media-upload-migration-material"
_BACKFILL_SUBJECT_ID = "media-upload-migration-subject"
_BACKFILL_ASSET_ID = "media-upload-migration-asset"
_AMBIGUOUS_ASSET_ID = "media-upload-migration-duplicate"
_BACKFILL_JOB_ID = "media-upload-migration-linked-job"
_BACKFILL_UPLOAD_ID = "1db3426a097bdb40e347a44218df2d07"
_BACKFILL_SHA256 = "a3" * 32
_BACKFILL_SIZE_BYTES = 12_345
_BACKFILL_STORAGE_KEY = f"upload_images/{_BACKFILL_MATERIAL_ID}/legacy.png"
_BACKFILL_CREATED_AT = datetime(2026, 7, 18, 8, 30, tzinfo=UTC)
_BACKFILL_UPDATED_AT = datetime(2026, 7, 18, 9, 45, tzinfo=UTC)
_DIGEST_CASES = (
    (
        "uppercase",
        "media-mig-digest-uppercase",
        "media-mig-job-uppercase",
        "AB" * 32,
        "pending",
    ),
    ("null", "media-mig-digest-null", "media-mig-job-null", None, "pending"),
    ("empty", "media-mig-digest-empty", "media-mig-job-empty", "", "retry"),
    ("nonhex", "media-mig-digest-nonhex", "media-mig-job-nonhex", "g" * 64, "pending"),
    ("short", "media-mig-digest-short", "media-mig-job-short", "ab" * 31, "retry"),
)
_DIGEST_ASSET_IDS = [case[1] for case in _DIGEST_CASES]
_DIGEST_JOB_IDS = [case[2] for case in _DIGEST_CASES]
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
        raise AssertionError(f"{_DATABASE_URL_ENV} 必须显式指定实际数据库名")
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
    connect_keyword = {key: value for key, value in keyword.items() if key in accepted}
    return await asyncpg.connect(**connect_keyword)


def _management_database_url() -> str:
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


def _url_with_query_override(url: str, key: str, value: str) -> str:
    return make_url(url).update_query_dict({key: value}).render_as_string(hide_password=False)


async def _public_schema_fingerprint(connection: asyncpg.Connection) -> tuple[tuple[object, ...], ...]:
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


async def _cleanup_temporary_database(management_url: str, database_name: str) -> None:
    if _TEMP_DATABASE_PATTERN.fullmatch(database_name) is None or len(database_name) > 63:
        raise AssertionError(f"拒绝不安全的临时数据库名: {database_name!r}")

    management = await _connect_database(management_url)
    try:
        exists = await management.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
            database_name,
        )
        if exists:
            await management.execute(
                """
                SELECT pg_terminate_backend(activity.pid)
                FROM pg_stat_activity AS activity
                WHERE activity.datname = $1
                  AND activity.pid <> pg_backend_pid()
                """,
                database_name,
            )
            await _database_ddl(
                management,
                operation="drop",
                database_name=database_name,
            )

        still_exists = await management.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
            database_name,
        )
        if still_exists:
            raise AssertionError(f"临时数据库清理失败: {database_name}")
    finally:
        await management.close()


@dataclass(frozen=True)
class _MigrationDatabase:
    url: str = field(repr=False)
    management_database: str
    temporary_database: str


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def migration_database() -> AsyncIterator[_MigrationDatabase]:
    management_url = _management_database_url()
    management_target = _database_target(management_url)
    management_database = management_target.database
    temporary_database = f"{_TEMP_DATABASE_PREFIX}{uuid4().hex}"
    if temporary_database == management_database:
        raise AssertionError("临时数据库不得与管理数据库同名")
    temporary_url = _temporary_database_url(management_url, temporary_database)
    temporary_target = _database_target(temporary_url)

    management = await _connect_database(management_url)
    sentinel_created = False
    management_fingerprint: tuple[tuple[object, ...], ...] | None = None
    sentinel = uuid4().hex
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
                f"{_DATABASE_URL_ENV} 的连接角色必须具有 CREATEDB 或超级用户权限；"
                "拒绝回退到原数据库执行迁移测试"
            )

        await management.execute(
            """
            CREATE TEMP TABLE media_upload_migration_management_sentinel (
                marker varchar(64) PRIMARY KEY
            ) ON COMMIT PRESERVE ROWS
            """
        )
        sentinel_created = True
        await management.execute(
            "INSERT INTO media_upload_migration_management_sentinel (marker) VALUES ($1)",
            sentinel,
        )
        management_fingerprint = await _public_schema_fingerprint(management)

        await _database_ddl(
            management,
            operation="create",
            database_name=temporary_database,
        )
        _ACTIVE_TEMP_TARGETS.add(temporary_target)
        yield _MigrationDatabase(
            url=temporary_url,
            management_database=management_database,
            temporary_database=temporary_database,
        )
    finally:
        cleanup_errors: list[BaseException] = []
        _ACTIVE_TEMP_TARGETS.discard(temporary_target)
        try:
            await _cleanup_temporary_database(management_url, temporary_database)
        except BaseException as error:
            cleanup_errors.append(error)
        if sentinel_created and management_fingerprint is not None:
            try:
                actual_sentinel = await management.fetchval(
                    "SELECT marker FROM media_upload_migration_management_sentinel"
                )
                if actual_sentinel != sentinel:
                    raise AssertionError("管理数据库的会话哨兵数据被修改")
                actual_fingerprint = await _public_schema_fingerprint(management)
                if actual_fingerprint != management_fingerprint:
                    raise AssertionError("管理数据库 public schema 在迁移测试期间发生变化")
            except BaseException as error:
                cleanup_errors.append(error)
        try:
            await management.close()
        except BaseException as error:
            cleanup_errors.append(error)
        if cleanup_errors:
            raise BaseExceptionGroup("媒体上传迁移临时数据库清理或管理库保护失败", cleanup_errors)


def _run_alembic(
    url: str,
    *arguments: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    target = _database_target(url)
    if target not in _ACTIVE_TEMP_TARGETS:
        raise AssertionError(
            f"拒绝在非本模块临时数据库上执行 Alembic: {target.database!r}"
        )
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


async def _database_revision(url: str) -> str:
    connection = await _connect_database(url)
    try:
        revision = await connection.fetchval("SELECT version_num FROM alembic_version")
        assert isinstance(revision, str)
        return revision
    finally:
        await connection.close()


async def _cleanup_media_migration_rows(url: str) -> None:
    connection = await _connect_database(url)
    try:
        async with connection.transaction():
            if await connection.fetchval("SELECT to_regclass('media_storage_jobs')") is not None:
                await connection.execute(
                    """
                    DELETE FROM media_storage_jobs
                    WHERE id = ANY($1::varchar[])
                       OR idempotency_key = ANY($2::varchar[])
                    """,
                    [_LEGACY_JOB_ID, _BACKFILL_JOB_ID, *_DIGEST_JOB_IDS],
                    ["media-upload-migration-legacy", "media-upload-migration-linked"],
                )
                assert (
                    await connection.fetchval(
                        """
                    SELECT count(*) FROM media_storage_jobs
                    WHERE id = ANY($1::varchar[])
                       OR idempotency_key = ANY($2::varchar[])
                    """,
                        [_LEGACY_JOB_ID, _BACKFILL_JOB_ID, *_DIGEST_JOB_IDS],
                        ["media-upload-migration-legacy", "media-upload-migration-linked"],
                    )
                    == 0
                )
            if await connection.fetchval("SELECT to_regclass('media_uploads')") is not None:
                await connection.execute(
                    """
                    DELETE FROM media_uploads
                    WHERE id = $1 OR owner_script_id = $2
                    """,
                    _BACKFILL_UPLOAD_ID,
                    _BACKFILL_SCRIPT_ID,
                )
                assert (
                    await connection.fetchval(
                        """
                    SELECT count(*) FROM media_uploads
                    WHERE id = $1 OR owner_script_id = $2
                    """,
                        _BACKFILL_UPLOAD_ID,
                        _BACKFILL_SCRIPT_ID,
                    )
                    == 0
                )
            for table_name, row_id in (
                *(("asset_entries", asset_id) for asset_id in _DIGEST_ASSET_IDS),
                ("asset_entries", _AMBIGUOUS_ASSET_ID),
                ("asset_entries", _BACKFILL_ASSET_ID),
                ("subjects", _BACKFILL_SUBJECT_ID),
                ("materials", _BACKFILL_MATERIAL_ID),
                ("scripts", _BACKFILL_SCRIPT_ID),
            ):
                if (
                    await connection.fetchval(
                        "SELECT to_regclass($1)",
                        table_name,
                    )
                    is not None
                ):
                    await connection.execute(
                        f"DELETE FROM {table_name} WHERE id = $1",
                        row_id,
                    )
                    assert (
                        await connection.fetchval(
                            f"SELECT count(*) FROM {table_name} WHERE id = $1",
                            row_id,
                        )
                        == 0
                    )
    finally:
        await connection.close()


async def _seed_backfill_graph(connection: asyncpg.Connection) -> None:
    await connection.execute(
        """
        INSERT INTO scripts (id, title, scope, owner_user_id, created_by_user_id)
        VALUES ($1, '媒体上传迁移测试', 'personal', 'media-migration-user', 'media-migration-user')
        """,
        _BACKFILL_SCRIPT_ID,
    )
    await connection.execute(
        """
        INSERT INTO materials (id, name, script_id, version)
        VALUES ($1, '媒体上传迁移画布', $2, 7)
        """,
        _BACKFILL_MATERIAL_ID,
        _BACKFILL_SCRIPT_ID,
    )
    await connection.execute(
        """
        INSERT INTO subjects (
            id, script_id, type, name, pipeline_name, description,
            subject_version, enrich_version, name_locked, description_locked,
            is_child, is_parent, parent_id, episode_nos, is_active,
            review_status, image_entry_id, whitelist_status
        ) VALUES (
            $1, $2, 'character', '迁移测试角色', '迁移测试角色', '验证旧上传资产回填',
            3, 2, false, false, false, false, NULL, '[1]'::jsonb, true,
            'approved', NULL, NULL
        )
        """,
        _BACKFILL_SUBJECT_ID,
        _BACKFILL_SCRIPT_ID,
    )
    await connection.execute(
        """
        INSERT INTO asset_entries (
            id, script_id, subject_id, kind, storage_key, mime_type,
            size_bytes, source, sha256, status, is_approved,
            superseded_by_id, superseded, created_at, updated_at
        ) VALUES (
            $1, $2, $3, 'image', $4, 'image/png',
            $5, 'upload_images', $6, 'candidate', false,
            NULL, false, $7, $8
        )
        """,
        _BACKFILL_ASSET_ID,
        _BACKFILL_SCRIPT_ID,
        _BACKFILL_SUBJECT_ID,
        _BACKFILL_STORAGE_KEY,
        _BACKFILL_SIZE_BYTES,
        _BACKFILL_SHA256,
        _BACKFILL_CREATED_AT,
        _BACKFILL_UPDATED_AT,
    )
    await connection.execute(
        """
        INSERT INTO media_storage_jobs (
            id, idempotency_key, job_type, source_key, final_key,
            asset_id, status, attempt, available_at
        ) VALUES (
            $1, 'media-upload-migration-linked', 'promote',
            $2, $2, $3, 'succeeded', 1, now()
        )
        """,
        _BACKFILL_JOB_ID,
        _BACKFILL_STORAGE_KEY,
        _BACKFILL_ASSET_ID,
    )


async def _seed_ambiguous_backfill_asset(connection: asyncpg.Connection) -> None:
    await connection.execute(
        """
        INSERT INTO asset_entries (
            id, script_id, subject_id, kind, storage_key, mime_type,
            size_bytes, source, sha256, status, is_approved,
            superseded_by_id, superseded, created_at, updated_at
        ) VALUES (
            $1, $2, $3, 'image', $4, 'image/png',
            $5, 'upload_images', $6, 'candidate', false,
            NULL, false, $7, $8
        )
        """,
        _AMBIGUOUS_ASSET_ID,
        _BACKFILL_SCRIPT_ID,
        _BACKFILL_SUBJECT_ID,
        _BACKFILL_STORAGE_KEY,
        _BACKFILL_SIZE_BYTES,
        _BACKFILL_SHA256,
        _BACKFILL_CREATED_AT,
        _BACKFILL_UPDATED_AT,
    )


async def _seed_legacy_storage_job(connection: asyncpg.Connection) -> None:
    await connection.execute(
        """
        INSERT INTO media_storage_jobs (
            id, idempotency_key, job_type, source_key, final_key,
            asset_id, status, attempt, available_at
        ) VALUES (
            $1, 'media-upload-migration-legacy', 'promote',
            'upload_images/_tmp/migration/legacy.png',
            'upload_images/migration/legacy.png',
            NULL, 'pending', 0, now()
        )
        """,
        _LEGACY_JOB_ID,
    )


async def _seed_digest_cases(connection: asyncpg.Connection) -> None:
    for label, asset_id, job_id, digest, status in _DIGEST_CASES:
        storage_key = f"upload_images/{_BACKFILL_MATERIAL_ID}/{label}.png"
        await connection.execute(
            """
            INSERT INTO asset_entries (
                id, script_id, subject_id, kind, storage_key, mime_type,
                size_bytes, source, sha256, status, is_approved,
                superseded_by_id, superseded, created_at, updated_at
            ) VALUES (
                $1, $2, $3, 'image', $4, 'image/png',
                $5, 'upload_images', $6, 'candidate', false,
                NULL, false, $7, $8
            )
            """,
            asset_id,
            _BACKFILL_SCRIPT_ID,
            _BACKFILL_SUBJECT_ID,
            storage_key,
            _BACKFILL_SIZE_BYTES,
            digest,
            _BACKFILL_CREATED_AT,
            _BACKFILL_UPDATED_AT,
        )
        await connection.execute(
            """
            INSERT INTO media_storage_jobs (
                id, idempotency_key, job_type, source_key, final_key,
                asset_id, status, attempt, available_at
            ) VALUES (
                $1, $2, 'promote', $3, $3,
                $4, $5, 1, now()
            )
            """,
            job_id,
            f"media-upload-migration-digest-{label}",
            storage_key,
            asset_id,
            status,
        )


def _raise_isolation_failures(
    primary_error: BaseException | None,
    isolation_errors: list[BaseException],
) -> None:
    failures = ([primary_error] if primary_error is not None else []) + isolation_errors
    if not failures:
        return
    if len(failures) == 1:
        error = failures[0]
        raise error.with_traceback(error.__traceback__)
    raise BaseExceptionGroup("媒体上传迁移测试主体与数据库恢复均失败", failures)


@asynccontextmanager
async def _isolated_media_migration(url: str) -> AsyncIterator[str]:
    _run_alembic(url, "upgrade", "head")
    restore_revision = await _database_revision(url)
    await _cleanup_media_migration_rows(url)
    primary_error: BaseException | None = None
    try:
        yield restore_revision
    except BaseException as error:
        primary_error = error

    isolation_errors: list[BaseException] = []
    try:
        await _cleanup_media_migration_rows(url)
    except BaseException as error:
        isolation_errors.append(error)
    try:
        _run_alembic(url, "upgrade", restore_revision)
    except BaseException as error:
        isolation_errors.append(error)
    try:
        await _cleanup_media_migration_rows(url)
    except BaseException as error:
        isolation_errors.append(error)
    try:
        actual_revision = await _database_revision(url)
        if actual_revision != restore_revision:
            raise AssertionError(f"媒体迁移测试结束版本应为 {restore_revision}，实际为 {actual_revision}")
    except BaseException as error:
        isolation_errors.append(error)
    _raise_isolation_failures(primary_error, isolation_errors)


@pytest.mark.asyncio
async def test_media_upload_migration_uses_only_registered_temporary_database(
    migration_database: _MigrationDatabase,
) -> None:
    assert migration_database.temporary_database != migration_database.management_database
    assert _TEMP_DATABASE_PATTERN.fullmatch(migration_database.temporary_database) is not None
    with pytest.raises(AssertionError, match="拒绝在非本模块临时数据库上执行 Alembic"):
        _run_alembic(_management_database_url(), "current")

    connection = await _connect_database(migration_database.url)
    try:
        assert await connection.fetchval("SELECT current_database()") == (
            migration_database.temporary_database
        )
    finally:
        await connection.close()


@pytest.mark.parametrize(
    ("query_key", "query_value"),
    [
        ("database", "shadow_database"),
        ("dbname", "shadow_database"),
        ("host", "shadow.invalid"),
        ("port", "6543"),
        ("user", "shadow_user"),
        ("username", "shadow_user"),
        ("password", "shadow_password"),
        ("dsn", "postgresql://shadow.invalid/shadow_database"),
        ("passfile", "/tmp/shadow-passfile"),
        ("service", "shadow_service"),
        ("servicefile", "/tmp/shadow-servicefile"),
    ],
)
def test_media_upload_migration_rejects_management_identity_query_overrides(
    monkeypatch: pytest.MonkeyPatch,
    query_key: str,
    query_value: str,
) -> None:
    management_url = _url_with_query_override(
        "postgresql+asyncpg://manager:test-only@127.0.0.1:5432/management",
        query_key,
        query_value,
    )
    monkeypatch.setenv(_DATABASE_URL_ENV, management_url)
    with pytest.raises(AssertionError, match="禁止在 query 中覆盖数据库连接身份"):
        _management_database_url()


def test_media_upload_migration_rejects_database_query_override_before_subprocess(
    migration_database: _MigrationDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malicious_url = _url_with_query_override(
        migration_database.url,
        "database",
        migration_database.management_database,
    )
    subprocess_called = False

    def _forbidden_subprocess(*_args, **_kwargs):
        nonlocal subprocess_called
        subprocess_called = True
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", _forbidden_subprocess)
    with pytest.raises(AssertionError, match="非本模块临时数据库"):
        _run_alembic(malicious_url, "current")
    assert subprocess_called is False


@pytest.mark.asyncio
async def test_media_upload_migration_cleanup_discovers_database_after_create_ack_loss(
    migration_database: _MigrationDatabase,
) -> None:
    management_url = _management_database_url()
    orphan_database = f"{_TEMP_DATABASE_PREFIX}{uuid4().hex}"
    management = await _connect_database(management_url)
    try:
        await _database_ddl(
            management,
            operation="create",
            database_name=orphan_database,
        )
        try:
            cleanup = globals().get("_cleanup_temporary_database")
            assert callable(cleanup), "必须提供与 CREATE 本地确认状态无关的临时库清理函数"
            await cleanup(management_url, orphan_database)
            assert (
                await management.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
                    orphan_database,
                )
                is False
            )
        finally:
            if await management.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
                orphan_database,
            ):
                await management.execute(
                    """
                    SELECT pg_terminate_backend(activity.pid)
                    FROM pg_stat_activity AS activity
                    WHERE activity.datname = $1
                      AND activity.pid <> pg_backend_pid()
                    """,
                    orphan_database,
                )
                await _database_ddl(
                    management,
                    operation="drop",
                    database_name=orphan_database,
                )
    finally:
        await management.close()


@pytest.mark.asyncio
async def test_media_upload_migration_backfills_legacy_asset_and_preserves_owner(
    migration_database: _MigrationDatabase,
) -> None:
    url = migration_database.url
    async with _isolated_media_migration(url):
        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        connection = await _connect_database(url)
        try:
            await _seed_backfill_graph(connection)
        finally:
            await connection.close()

        _run_alembic(url, "upgrade", _MEDIA_REVISION)
        connection = await _connect_database(url)
        try:
            upload = await connection.fetchrow(
                """
                SELECT id, owner_script_id, script_id, material_id, subject_id, asset_id,
                       temp_storage_key, final_storage_key, original_filename, mime_type,
                       size_bytes, sha256, status, last_error_type, completed_at,
                       created_at, updated_at
                FROM media_uploads
                WHERE id = $1
                """,
                _BACKFILL_UPLOAD_ID,
            )
            assert upload is not None
            assert dict(upload) == {
                "id": _BACKFILL_UPLOAD_ID,
                "owner_script_id": _BACKFILL_SCRIPT_ID,
                "script_id": _BACKFILL_SCRIPT_ID,
                "material_id": _BACKFILL_MATERIAL_ID,
                "subject_id": _BACKFILL_SUBJECT_ID,
                "asset_id": _BACKFILL_ASSET_ID,
                "temp_storage_key": _BACKFILL_STORAGE_KEY,
                "final_storage_key": _BACKFILL_STORAGE_KEY,
                "original_filename": f"legacy-{_BACKFILL_ASSET_ID}",
                "mime_type": "image/png",
                "size_bytes": _BACKFILL_SIZE_BYTES,
                "sha256": _BACKFILL_SHA256,
                "status": "ready",
                "last_error_type": None,
                "completed_at": _BACKFILL_UPDATED_AT,
                "created_at": _BACKFILL_CREATED_AT,
                "updated_at": _BACKFILL_UPDATED_AT,
            }

            job = await connection.fetchrow(
                """
                SELECT upload_id, expected_sha256, expected_size_bytes
                FROM media_storage_jobs
                WHERE id = $1
                """,
                _BACKFILL_JOB_ID,
            )
            assert job is not None
            assert tuple(job) == (
                _BACKFILL_UPLOAD_ID,
                _BACKFILL_SHA256,
                _BACKFILL_SIZE_BYTES,
            )

            await connection.execute(
                "DELETE FROM asset_entries WHERE id = $1",
                _BACKFILL_ASSET_ID,
            )
            remaining_links = await connection.fetchrow(
                """
                SELECT asset_id, subject_id, material_id, script_id, owner_script_id
                FROM media_uploads WHERE id = $1
                """,
                _BACKFILL_UPLOAD_ID,
            )
            assert remaining_links is not None
            assert tuple(remaining_links) == (
                None,
                _BACKFILL_SUBJECT_ID,
                _BACKFILL_MATERIAL_ID,
                _BACKFILL_SCRIPT_ID,
                _BACKFILL_SCRIPT_ID,
            )

            await connection.execute(
                "DELETE FROM subjects WHERE id = $1",
                _BACKFILL_SUBJECT_ID,
            )
            assert (
                await connection.fetchval(
                    "SELECT subject_id FROM media_uploads WHERE id = $1",
                    _BACKFILL_UPLOAD_ID,
                )
                is None
            )

            await connection.execute(
                "DELETE FROM materials WHERE id = $1",
                _BACKFILL_MATERIAL_ID,
            )
            assert (
                await connection.fetchval(
                    "SELECT material_id FROM media_uploads WHERE id = $1",
                    _BACKFILL_UPLOAD_ID,
                )
                is None
            )

            await connection.execute(
                "DELETE FROM scripts WHERE id = $1",
                _BACKFILL_SCRIPT_ID,
            )
            final_links = await connection.fetchrow(
                """
                SELECT asset_id, subject_id, material_id, script_id, owner_script_id
                FROM media_uploads WHERE id = $1
                """,
                _BACKFILL_UPLOAD_ID,
            )
            assert final_links is not None
            assert tuple(final_links) == (
                None,
                None,
                None,
                None,
                _BACKFILL_SCRIPT_ID,
            )
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_media_upload_migration_normalizes_only_known_legacy_digests(
    migration_database: _MigrationDatabase,
) -> None:
    url = migration_database.url
    async with _isolated_media_migration(url):
        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        connection = await _connect_database(url)
        try:
            await _seed_backfill_graph(connection)
            await _seed_digest_cases(connection)
        finally:
            await connection.close()

        _run_alembic(url, "upgrade", _MEDIA_REVISION)
        connection = await _connect_database(url)
        try:
            rows = await connection.fetch(
                """
                SELECT upload.asset_id, upload.sha256,
                       job.expected_sha256, job.expected_size_bytes, job.status
                FROM media_uploads AS upload
                JOIN media_storage_jobs AS job ON job.upload_id = upload.id
                WHERE upload.asset_id = ANY($1::varchar[])
                """,
                _DIGEST_ASSET_IDS,
            )
        finally:
            await connection.close()

        actual = {row["asset_id"]: tuple(row)[1:] for row in rows}
        uppercase_id = _DIGEST_CASES[0][1]
        assert actual[uppercase_id] == (
            ("ab" * 32),
            ("ab" * 32),
            _BACKFILL_SIZE_BYTES,
            "pending",
        )
        unknown_digest = "0" * 64
        for _, asset_id, _, _, status in _DIGEST_CASES[1:]:
            assert actual[asset_id] == (unknown_digest, None, None, status)


@pytest.mark.asyncio
async def test_media_upload_migration_rejects_ambiguous_legacy_storage_keys_atomically(
    migration_database: _MigrationDatabase,
) -> None:
    url = migration_database.url
    async with _isolated_media_migration(url):
        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        connection = await _connect_database(url)
        try:
            await _seed_backfill_graph(connection)
            await _seed_ambiguous_backfill_asset(connection)
        finally:
            await connection.close()

        result = _run_alembic(url, "upgrade", _MEDIA_REVISION, check=False)
        assert result.returncode != 0
        assert "存在媒体上传回填唯一键歧义，拒绝迁移" in result.stderr
        assert await _database_revision(url) == _PREVIOUS_REVISION

        connection = await _connect_database(url)
        try:
            assert await connection.fetchval("SELECT to_regclass('media_uploads')") is None
            assert (
                await connection.fetchval(
                    """
                    SELECT count(*)
                    FROM information_schema.columns
                    WHERE table_name = 'media_storage_jobs'
                      AND column_name IN (
                          'upload_id', 'expected_sha256',
                          'expected_size_bytes', 'max_attempts'
                      )
                    """
                )
                == 0
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM asset_entries WHERE id = ANY($1::varchar[])",
                    [_BACKFILL_ASSET_ID, _AMBIGUOUS_ASSET_ID],
                )
                == 2
            )
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_media_upload_migration_constraint_name_matches_orm_metadata(
    migration_database: _MigrationDatabase,
) -> None:
    url = migration_database.url
    async with _isolated_media_migration(url):
        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        _run_alembic(url, "upgrade", _MEDIA_REVISION)

        orm_names = {
            constraint.name
            for constraint in MediaUpload.__table__.constraints
            if isinstance(constraint, CheckConstraint)
        }
        connection = await _connect_database(url)
        try:
            database_names = {
                row["conname"]
                for row in await connection.fetch(
                    """
                    SELECT candidate.conname
                    FROM pg_constraint AS candidate
                    JOIN pg_class AS target ON target.oid = candidate.conrelid
                    JOIN pg_namespace AS namespace ON namespace.oid = target.relnamespace
                    WHERE namespace.nspname = current_schema()
                      AND target.relname = 'media_uploads'
                      AND candidate.contype = 'c'
                    """
                )
            }
            max_attempts_default = await connection.fetchval(
                """
                SELECT pg_get_expr(default_value.adbin, default_value.adrelid)
                FROM pg_attribute AS attribute
                JOIN pg_class AS target ON target.oid = attribute.attrelid
                JOIN pg_namespace AS namespace ON namespace.oid = target.relnamespace
                LEFT JOIN pg_attrdef AS default_value
                  ON default_value.adrelid = attribute.attrelid
                 AND default_value.adnum = attribute.attnum
                WHERE namespace.nspname = current_schema()
                  AND target.relname = 'media_storage_jobs'
                  AND attribute.attname = 'max_attempts'
                """
            )
        finally:
            await connection.close()

        assert orm_names == database_names == {"ck_media_uploads_status"}
        assert max_attempts_default is None


@pytest.mark.asyncio
async def test_media_upload_migration_downgrade_rejects_upload_and_linked_job_data(
    migration_database: _MigrationDatabase,
) -> None:
    url = migration_database.url
    async with _isolated_media_migration(url):
        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        connection = await _connect_database(url)
        try:
            await _seed_backfill_graph(connection)
        finally:
            await connection.close()
        _run_alembic(url, "upgrade", _MEDIA_REVISION)

        result = _run_alembic(url, "downgrade", _PREVIOUS_REVISION, check=False)
        assert result.returncode != 0
        assert "存在无法由旧版本表达的媒体上传数据，拒绝降级" in result.stderr
        assert await _database_revision(url) == _MEDIA_REVISION

        connection = await _connect_database(url)
        try:
            upload = await connection.fetchrow(
                "SELECT id, asset_id, status FROM media_uploads WHERE id = $1",
                _BACKFILL_UPLOAD_ID,
            )
            assert upload is not None
            assert tuple(upload) == (_BACKFILL_UPLOAD_ID, _BACKFILL_ASSET_ID, "ready")
            job = await connection.fetchrow(
                """
                SELECT upload_id, expected_sha256, expected_size_bytes, max_attempts
                FROM media_storage_jobs WHERE id = $1
                """,
                _BACKFILL_JOB_ID,
            )
            assert job is not None
            assert tuple(job) == (
                _BACKFILL_UPLOAD_ID,
                _BACKFILL_SHA256,
                _BACKFILL_SIZE_BYTES,
                8,
            )
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_media_upload_migration_downgrade_rejects_job_only_new_state(
    migration_database: _MigrationDatabase,
) -> None:
    url = migration_database.url
    async with _isolated_media_migration(url):
        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        connection = await _connect_database(url)
        try:
            await _seed_legacy_storage_job(connection)
        finally:
            await connection.close()
        _run_alembic(url, "upgrade", _MEDIA_REVISION)

        connection = await _connect_database(url)
        try:
            await connection.execute(
                """
                UPDATE media_storage_jobs
                SET expected_sha256 = $1,
                    expected_size_bytes = $2,
                    max_attempts = 3
                WHERE id = $3
                """,
                _BACKFILL_SHA256,
                _BACKFILL_SIZE_BYTES,
                _LEGACY_JOB_ID,
            )
        finally:
            await connection.close()

        result = _run_alembic(url, "downgrade", _PREVIOUS_REVISION, check=False)
        assert result.returncode != 0
        assert "存在无法由旧版本表达的媒体上传数据，拒绝降级" in result.stderr
        assert await _database_revision(url) == _MEDIA_REVISION

        connection = await _connect_database(url)
        try:
            row = await connection.fetchrow(
                """
                SELECT upload_id, expected_sha256, expected_size_bytes, max_attempts
                FROM media_storage_jobs WHERE id = $1
                """,
                _LEGACY_JOB_ID,
            )
            assert row is not None
            assert tuple(row) == (None, _BACKFILL_SHA256, _BACKFILL_SIZE_BYTES, 3)
            assert await connection.fetchval("SELECT count(*) FROM media_uploads") == 0
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_media_upload_migration_round_trip_preserves_legacy_null_digest(
    migration_database: _MigrationDatabase,
) -> None:
    url = migration_database.url
    async with _isolated_media_migration(url):
        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        connection = await _connect_database(url)
        try:
            await _seed_legacy_storage_job(connection)
        finally:
            await connection.close()

        _run_alembic(url, "upgrade", _MEDIA_REVISION)
        connection = await _connect_database(url)
        try:
            row = await connection.fetchrow(
                """
                SELECT upload_id, expected_sha256, expected_size_bytes, max_attempts
                FROM media_storage_jobs
                WHERE id = $1
                """,
                _LEGACY_JOB_ID,
            )
            assert row is not None
            assert row["upload_id"] is None
            assert row["expected_sha256"] is None
            assert row["expected_size_bytes"] is None
            assert row["max_attempts"] == 8
            assert await connection.fetchval("SELECT to_regclass('media_uploads')") == "media_uploads"
        finally:
            await connection.close()

        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        connection = await _connect_database(url)
        try:
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM media_storage_jobs WHERE id = $1",
                    _LEGACY_JOB_ID,
                )
                == 1
            )
            assert (
                await connection.fetchval(
                    """
                SELECT count(*)
                FROM information_schema.columns
                WHERE table_name = 'media_storage_jobs'
                  AND column_name IN (
                      'upload_id', 'expected_sha256',
                      'expected_size_bytes', 'max_attempts'
                  )
                """
                )
                == 0
            )
        finally:
            await connection.close()

        _run_alembic(url, "upgrade", "head")
        connection = await _connect_database(url)
        try:
            row = await connection.fetchrow(
                """
                SELECT upload_id, expected_sha256, expected_size_bytes, max_attempts
                FROM media_storage_jobs
                WHERE id = $1
                """,
                _LEGACY_JOB_ID,
            )
            assert row is not None
            assert tuple(row) == (None, None, None, 8)
        finally:
            await connection.close()
        _run_alembic(url, "check")


@pytest.mark.asyncio
async def test_media_upload_migration_empty_database_round_trip(
    migration_database: _MigrationDatabase,
) -> None:
    url = migration_database.url
    _run_alembic(url, "upgrade", "head")
    primary_error: BaseException | None = None
    try:
        _run_alembic(url, "downgrade", "base")
        connection = await _connect_database(url)
        try:
            assert await connection.fetchval("SELECT to_regclass('media_uploads')") is None
            assert await connection.fetchval("SELECT to_regclass('media_storage_jobs')") is None
        finally:
            await connection.close()

        _run_alembic(url, "upgrade", "head")
        assert await _database_revision(url) == _MEDIA_REVISION
        _run_alembic(url, "check")
    except BaseException as error:
        primary_error = error

    isolation_errors: list[BaseException] = []
    try:
        _run_alembic(url, "upgrade", "head")
    except BaseException as error:
        isolation_errors.append(error)
    try:
        actual_revision = await _database_revision(url)
        if actual_revision != _MEDIA_REVISION:
            raise AssertionError(f"空库往返测试结束版本应为 {_MEDIA_REVISION}，实际为 {actual_revision}")
    except BaseException as error:
        isolation_errors.append(error)
    _raise_isolation_failures(primary_error, isolation_errors)
