"""mgrouter 视频供应商约束迁移的安全与 PostgreSQL 往返测试。"""

from __future__ import annotations

import os
import subprocess
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import pytest

_BACKEND_DIR = Path(__file__).resolve().parents[1]
_PYTHON = _BACKEND_DIR / ".venv" / "bin" / "python"
_ALEMBIC_CONFIG = _BACKEND_DIR / "alembic.ini"
_MIGRATION_PATH = (
    _BACKEND_DIR
    / "alembic"
    / "versions"
    / "50028cba1845_启用_mgrouter_视频供应商.py"
)
_DATABASE_URL_ENV = "MTV_TEST_POSTGRES_URL"
_REVISION = "50028cba1845"
_PREVIOUS_REVISION = "3a7d8e5c1f40"
_NO_DOTENV_ALEMBIC_RUNNER = """
import sys
from pydantic_settings.sources.providers.dotenv import DotEnvSettingsSource
DotEnvSettingsSource._read_env_files = lambda self: {}
from alembic.config import CommandLine
CommandLine(prog="alembic").main(argv=["-c", sys.argv[1], *sys.argv[2:]])
"""


def test_migration_source_orders_write_fences_and_downgrade_lock() -> None:
    source = _MIGRATION_PATH.read_text(encoding="utf-8")
    replacement = source[source.index("def _replace_check_constraint") : source.index("def upgrade")]
    upgrade = source[source.index("def upgrade") : source.index("def downgrade")]
    downgrade = source[source.index("def downgrade") :]

    assert source.count('down_revision: str | None = "3a7d8e5c1f40"') == 1
    assert replacement.index("ADD CONSTRAINT") < replacement.index("VALIDATE CONSTRAINT")
    assert replacement.index("VALIDATE CONSTRAINT") < replacement.index("drop_constraint")
    assert replacement.index("drop_constraint") < replacement.index("RENAME CONSTRAINT")
    assert "NOT VALID" in replacement
    assert upgrade.count("condition=_MGROUTER_PROVIDER_CHECK") == 2
    assert downgrade.count("condition=_PREVIOUS_PROVIDER_CHECK") == 2
    assert downgrade.index("LOCK TABLE video_provider_configs, video_generation_jobs") < downgrade.index(
        "SELECT count(*) INTO mgrouter_config_count"
    )
    assert "IN SHARE ROW EXCLUSIVE MODE" in downgrade
    assert "FROM video_provider_configs" in downgrade
    assert "FROM video_generation_jobs" in downgrade
    assert "RAISE EXCEPTION" in downgrade
    assert "DELETE FROM" not in downgrade.upper()
    assert "UPDATE " not in downgrade.upper()


def _database_url() -> str:
    url = os.environ.get(_DATABASE_URL_ENV)
    if not url:
        pytest.skip(f"未配置 {_DATABASE_URL_ENV}，跳过真实 PostgreSQL 迁移测试")
    if not url.startswith("postgresql+asyncpg://"):
        pytest.fail(f"{_DATABASE_URL_ENV} 必须使用 postgresql+asyncpg 驱动")
    return url


def _asyncpg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _run_alembic(
    url: str,
    *arguments: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
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
        diagnostic = f"{result.stdout}\n{result.stderr}".replace(url, "[DATABASE_URL]")
        diagnostic = diagnostic.replace(_asyncpg_dsn(url), "[DATABASE_URL]")
        pytest.fail(f"Alembic {' '.join(arguments)} 失败:\n{diagnostic}")
    return result


async def _database_revision(url: str) -> str:
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        revision = await connection.fetchval("SELECT version_num FROM alembic_version")
        assert isinstance(revision, str)
        return revision
    finally:
        await connection.close()


async def _constraint_definition(
    connection: asyncpg.Connection,
    table_name: str,
    constraint_name: str,
) -> str:
    definition = await connection.fetchval(
        """
        SELECT pg_get_constraintdef(candidate.oid)
        FROM pg_constraint AS candidate
        WHERE candidate.conrelid = $1::regclass
          AND candidate.conname = $2
        """,
        table_name,
        constraint_name,
    )
    assert isinstance(definition, str)
    return definition


async def _seed_job_dependencies(connection: asyncpg.Connection) -> None:
    await connection.execute(
        """
        INSERT INTO scripts (id, title, scope, owner_user_id, created_by_user_id)
        VALUES (
            'mgrouter-migration-script', 'mgrouter 迁移测试', 'personal',
            'mgrouter-migration-user', 'mgrouter-migration-user'
        )
        ON CONFLICT (id) DO NOTHING
        """
    )
    await connection.execute(
        """
        INSERT INTO episodes (
            episode_id, script_id, episode_no, title,
            schema_version, episode_version, client_save_seq
        )
        VALUES (
            'mgrouter-migration-episode', 'mgrouter-migration-script', 1, '第一集',
            1, 1, 0
        )
        ON CONFLICT (episode_id) DO NOTHING
        """
    )
    await connection.execute(
        """
        INSERT INTO shot_groups (
            shot_group_id, script_id, episode_id, scene_no, group_index, group_no,
            prompt, reference_frames, duration_seconds, materials, audio_elements,
            description_text, unit_id, unit_runtime_key, reference_image_audit
        )
        VALUES (
            'mgrouter-migration-group', 'mgrouter-migration-script',
            'mgrouter-migration-episode', 1, 1, 'G1', '迁移测试', '{}'::jsonb, 4,
            '{}'::jsonb, '{}'::jsonb, '', 'group:mgrouter-migration-group',
            'mgrouter-migration-episode:1:group:mgrouter-migration-group', '{}'::jsonb
        )
        ON CONFLICT (shot_group_id) DO NOTHING
        """
    )


async def _insert_provider_config(
    connection: asyncpg.Connection,
    *,
    provider_key: str,
    suffix: str,
) -> None:
    await connection.execute(
        """
        INSERT INTO video_provider_configs (id, provider_key, job_type, model, is_active)
        VALUES ($1, $2, 'all_reference', $3, false)
        """,
        f"mgrouter-migration-config-{suffix}",
        provider_key,
        f"migration-model-{suffix}",
    )


async def _insert_video_job(
    connection: asyncpg.Connection,
    *,
    provider_key: str,
    suffix: str,
    attempt: int,
) -> None:
    await _seed_job_dependencies(connection)
    await connection.execute(
        """
        INSERT INTO video_generation_jobs (
            job_id, script_id, episode_id, shot_group_id, owner_user_id,
            idempotency_key, request_hash, input_version, attempt,
            scene_no, unit_id, unit_runtime_key, provider_key, job_type,
            status, progress, retryable, input_json, metrics, version
        )
        VALUES (
            $1, 'mgrouter-migration-script', 'mgrouter-migration-episode',
            'mgrouter-migration-group', 'mgrouter-migration-user', $2, $3, 1, $4,
            1, 'group:mgrouter-migration-group',
            'mgrouter-migration-episode:1:group:mgrouter-migration-group',
            $5, 'all_reference', 'starting', 0, false, '{}'::jsonb, '{}'::jsonb, 0
        )
        """,
        f"mgrouter-migration-job-{suffix}",
        f"mgrouter-migration-key-{suffix}",
        suffix,
        attempt,
        provider_key,
    )


async def _cleanup_rows(connection: asyncpg.Connection) -> None:
    async with connection.transaction():
        await connection.execute(
            """
            DELETE FROM video_generation_idempotency_requests
            WHERE script_id = 'mgrouter-migration-script'
               OR job_id LIKE 'mgrouter-migration-job-%'
            """
        )
        await connection.execute(
            "DELETE FROM video_generation_jobs WHERE job_id LIKE 'mgrouter-migration-job-%'"
        )
        await connection.execute(
            "DELETE FROM video_provider_configs WHERE id LIKE 'mgrouter-migration-config-%'"
        )
        await connection.execute(
            "DELETE FROM shot_groups WHERE shot_group_id = 'mgrouter-migration-group'"
        )
        await connection.execute(
            "DELETE FROM episodes WHERE episode_id = 'mgrouter-migration-episode'"
        )
        await connection.execute(
            "DELETE FROM scripts WHERE id = 'mgrouter-migration-script'"
        )


async def _cleanup_database(url: str) -> None:
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await _cleanup_rows(connection)
    finally:
        await connection.close()


@asynccontextmanager
async def _isolated_migration(url: str) -> AsyncIterator[str]:
    _run_alembic(url, "upgrade", "head")
    restore_revision = await _database_revision(url)
    await _cleanup_database(url)
    try:
        yield restore_revision
    finally:
        await _cleanup_database(url)
        _run_alembic(url, "upgrade", "head")
        assert await _database_revision(url) == restore_revision


@pytest.mark.asyncio
async def test_mgrouter_constraint_round_trip_preserves_legacy_rows() -> None:
    url = _database_url()
    async with _isolated_migration(url):
        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            await _insert_provider_config(
                connection,
                provider_key="modelsai",
                suffix="legacy",
            )
            await _insert_video_job(
                connection,
                provider_key="modelsai",
                suffix="legacy",
                attempt=1,
            )
        finally:
            await connection.close()

        _run_alembic(url, "upgrade", _REVISION)
        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            for table_name, constraint_name in (
                ("video_provider_configs", "ck_video_provider_configs_provider_key"),
                ("video_generation_jobs", "ck_video_jobs_provider_key"),
            ):
                definition = await _constraint_definition(
                    connection,
                    table_name,
                    constraint_name,
                )
                assert "mgrouter" in definition
            await _insert_provider_config(
                connection,
                provider_key="mgrouter",
                suffix="canonical",
            )
            await _insert_video_job(
                connection,
                provider_key="mgrouter",
                suffix="canonical",
                attempt=2,
            )
            await connection.execute(
                "DELETE FROM video_generation_jobs "
                "WHERE job_id = 'mgrouter-migration-job-canonical'"
            )
            await connection.execute(
                "DELETE FROM video_provider_configs "
                "WHERE id = 'mgrouter-migration-config-canonical'"
            )
        finally:
            await connection.close()

        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            for table_name, constraint_name in (
                ("video_provider_configs", "ck_video_provider_configs_provider_key"),
                ("video_generation_jobs", "ck_video_jobs_provider_key"),
            ):
                definition = await _constraint_definition(
                    connection,
                    table_name,
                    constraint_name,
                )
                assert "mgrouter" not in definition
            assert await connection.fetchval(
                "SELECT count(*) FROM video_provider_configs "
                "WHERE id = 'mgrouter-migration-config-legacy' AND provider_key = 'modelsai'"
            ) == 1
            assert await connection.fetchval(
                "SELECT count(*) FROM video_generation_jobs "
                "WHERE job_id = 'mgrouter-migration-job-legacy' AND provider_key = 'modelsai'"
            ) == 1
        finally:
            await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mgrouter_row", ["provider_config", "video_job"])
async def test_mgrouter_downgrade_fails_closed_without_deleting_data(
    mgrouter_row: str,
) -> None:
    url = _database_url()
    async with _isolated_migration(url) as restore_revision:
        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            if mgrouter_row == "provider_config":
                await _insert_provider_config(
                    connection,
                    provider_key="mgrouter",
                    suffix="fail",
                )
            else:
                await _insert_video_job(
                    connection,
                    provider_key="mgrouter",
                    suffix="fail",
                    attempt=3,
                )
        finally:
            await connection.close()

        result = _run_alembic(
            url,
            "downgrade",
            _PREVIOUS_REVISION,
            check=False,
        )
        assert result.returncode != 0
        assert "存在 mgrouter 视频供应商数据，拒绝降级以避免数据丢失" in (
            result.stdout + result.stderr
        )
        assert await _database_revision(url) == restore_revision

        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            table_name = (
                "video_provider_configs"
                if mgrouter_row == "provider_config"
                else "video_generation_jobs"
            )
            id_column = "id" if mgrouter_row == "provider_config" else "job_id"
            row_id = (
                "mgrouter-migration-config-fail"
                if mgrouter_row == "provider_config"
                else "mgrouter-migration-job-fail"
            )
            assert await connection.fetchval(
                f"SELECT count(*) FROM {table_name} "
                f"WHERE {id_column} = $1 AND provider_key = 'mgrouter'",
                row_id,
            ) == 1
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_downgrade_lock_blocks_concurrent_writes() -> None:
    url = _database_url()
    async with _isolated_migration(url):
        locker = await asyncpg.connect(_asyncpg_dsn(url))
        writer = await asyncpg.connect(_asyncpg_dsn(url))
        transaction = locker.transaction()
        await transaction.start()
        try:
            await locker.execute(
                "LOCK TABLE video_provider_configs, video_generation_jobs "
                "IN SHARE ROW EXCLUSIVE MODE"
            )
            locks = await locker.fetch(
                """
                SELECT relation.relname, held.mode
                FROM pg_locks AS held
                JOIN pg_class AS relation ON relation.oid = held.relation
                WHERE held.pid = pg_backend_pid()
                  AND held.granted
                  AND relation.relname = ANY($1::text[])
                """,
                ["video_provider_configs", "video_generation_jobs"],
            )
            assert {row["relname"]: row["mode"] for row in locks} == {
                "video_provider_configs": "ShareRowExclusiveLock",
                "video_generation_jobs": "ShareRowExclusiveLock",
            }

            with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
                async with writer.transaction():
                    await writer.execute("SET LOCAL lock_timeout = '250ms'")
                    await _insert_provider_config(
                        writer,
                        provider_key="mgrouter",
                        suffix="concurrent",
                    )
        finally:
            await transaction.rollback()
            await writer.close()
            await locker.close()
