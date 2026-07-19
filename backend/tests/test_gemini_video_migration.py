"""Gemini Veo 数据库约束迁移的 PostgreSQL 往返测试。"""

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
_DATABASE_URL_ENV = "MTV_TEST_POSTGRES_URL"
_PREVIOUS_REVISION = "a91f0d7e3b24"
_NO_DOTENV_ALEMBIC_RUNNER = """
import sys
from pydantic_settings.sources.providers.dotenv import DotEnvSettingsSource
DotEnvSettingsSource._read_env_files = lambda self: {}
from alembic.config import CommandLine
CommandLine(prog="alembic").main(argv=["-c", sys.argv[1], *sys.argv[2:]])
"""


def _database_url() -> str:
    url = os.environ.get(_DATABASE_URL_ENV)
    if not url:
        pytest.skip(f"未配置 {_DATABASE_URL_ENV}，跳过真实 PostgreSQL 迁移测试")
    return url


def _asyncpg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _run_alembic(
    url: str, *arguments: str, check: bool = True
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
        pytest.fail(f"Alembic {' '.join(arguments)} 失败:\n{result.stdout}\n{result.stderr}")
    return result


def _reset_to_previous_revision(url: str) -> None:
    _run_alembic(url, "downgrade", "base")
    _run_alembic(url, "upgrade", _PREVIOUS_REVISION)


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
    raise BaseExceptionGroup("Gemini 迁移测试主体与数据库隔离恢复均失败", failures)


async def _database_revision(url: str) -> str:
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        revision = await connection.fetchval("SELECT version_num FROM alembic_version")
        assert isinstance(revision, str)
        return revision
    finally:
        await connection.close()


async def _constraint_definition(
    connection: asyncpg.Connection, table_name: str, constraint_name: str
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
            'gemini-migration-script', 'Gemini 迁移测试', 'personal',
            'gemini-migration-user', 'gemini-migration-user'
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
            'gemini-migration-episode', 'gemini-migration-script', 1, '第一集',
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
            description_text, unit_id, unit_runtime_key
        )
        VALUES (
            'gemini-migration-group', 'gemini-migration-script',
            'gemini-migration-episode', 1, 1, 'G1', '迁移测试', '{}'::jsonb, 3,
            '{}'::jsonb, '{}'::jsonb, '', 'group:gemini-migration-group',
            'gemini-migration-episode:1:group:gemini-migration-group'
        )
        ON CONFLICT (shot_group_id) DO NOTHING
        """
    )


async def _insert_video_job(
    connection: asyncpg.Connection, *, provider_key: str, suffix: str, attempt: int
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
            $1, 'gemini-migration-script', 'gemini-migration-episode',
            'gemini-migration-group', 'gemini-migration-user', $2, $3, 1, $4,
            1, 'group:gemini-migration-group',
            'gemini-migration-episode:1:group:gemini-migration-group',
            $5, 'all_reference', 'starting', 0, false, '{}'::jsonb, '{}'::jsonb, 0
        )
        """,
        f"gemini-migration-job-{suffix}",
        f"gemini-migration-key-{suffix}",
        suffix,
        attempt,
        provider_key,
    )


async def _cleanup_gemini_rows(connection: asyncpg.Connection) -> None:
    async with connection.transaction():
        if await connection.fetchval(
            "SELECT to_regclass('video_generation_idempotency_requests')"
        ) is not None:
            await connection.execute(
                """
                DELETE FROM video_generation_idempotency_requests
                WHERE script_id = 'gemini-migration-script'
                   OR job_id LIKE 'gemini-migration-job-%'
                """
            )
        await connection.execute(
            "DELETE FROM video_generation_jobs WHERE job_id LIKE 'gemini-migration-job-%'"
        )
        await connection.execute(
            "DELETE FROM video_provider_configs WHERE id LIKE 'gemini-migration-config-%'"
        )
        await connection.execute(
            "DELETE FROM shot_groups WHERE shot_group_id = 'gemini-migration-group'"
        )
        await connection.execute(
            "DELETE FROM episodes WHERE episode_id = 'gemini-migration-episode'"
        )
        await connection.execute(
            "DELETE FROM scripts WHERE id = 'gemini-migration-script'"
        )


async def _cleanup_gemini_database(url: str) -> None:
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    cleanup_error: BaseException | None = None
    try:
        await _cleanup_gemini_rows(connection)
    except BaseException as error:
        cleanup_error = error

    close_errors: list[BaseException] = []
    try:
        await connection.close()
    except BaseException as error:
        close_errors.append(error)
    _raise_isolation_failures(cleanup_error, close_errors)


@asynccontextmanager
async def _isolated_gemini_migration(url: str) -> AsyncIterator[str]:
    _run_alembic(url, "upgrade", "head")
    restore_revision = await _database_revision(url)
    primary_error: BaseException | None = None
    try:
        yield restore_revision
    except BaseException as error:
        primary_error = error

    isolation_errors: list[BaseException] = []
    try:
        _run_alembic(url, "upgrade", restore_revision)
    except BaseException as error:
        isolation_errors.append(error)
    try:
        await _cleanup_gemini_database(url)
    except BaseException as error:
        isolation_errors.append(error)
    try:
        actual_revision = await _database_revision(url)
        if actual_revision != restore_revision:
            raise AssertionError(
                f"Gemini 迁移测试结束版本应为 {restore_revision}，实际为 {actual_revision}"
            )
    except BaseException as error:
        isolation_errors.append(error)
    _raise_isolation_failures(primary_error, isolation_errors)


@pytest.mark.asyncio
async def test_gemini_provider_constraint_round_trip_preserves_legacy_rows() -> None:
    url = _database_url()
    async with _isolated_gemini_migration(url):
        _reset_to_previous_revision(url)
        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            legacy_definition = await _constraint_definition(
                connection,
                "video_generation_jobs",
                "ck_video_jobs_provider_key",
            )
            assert "gemini" not in legacy_definition
            await connection.execute(
                """
                INSERT INTO video_provider_configs (
                    id, provider_key, job_type, model, is_active
                ) VALUES (
                    'gemini-migration-config-legacy', 'modelsai', 'all_reference',
                    'seedance-legacy', true
                )
                """
            )
            await _insert_video_job(
                connection, provider_key="modelsai", suffix="legacy", attempt=1
            )
        finally:
            await connection.close()

        _run_alembic(url, "upgrade", "head")
        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            for table_name, constraint_name in (
                ("video_provider_configs", "ck_video_provider_configs_provider_key"),
                ("video_generation_jobs", "ck_video_jobs_provider_key"),
            ):
                assert "gemini" in await _constraint_definition(
                    connection, table_name, constraint_name
                )
            await connection.execute(
                """
                INSERT INTO video_provider_configs (
                    id, provider_key, job_type, model, is_active
                ) VALUES (
                    'gemini-migration-config-canonical', 'gemini', 'all_reference',
                    'veo-test', false
                )
                """
            )
            await _insert_video_job(
                connection, provider_key="gemini", suffix="canonical", attempt=2
            )
            await connection.execute(
                """
                DELETE FROM video_generation_jobs
                WHERE job_id = 'gemini-migration-job-canonical'
                """
            )
            await connection.execute(
                """
                DELETE FROM video_provider_configs
                WHERE id = 'gemini-migration-config-canonical'
                """
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
                assert "gemini" not in await _constraint_definition(
                    connection, table_name, constraint_name
                )
            assert await connection.fetchval(
                """
                SELECT count(*) FROM video_generation_jobs
                WHERE job_id = 'gemini-migration-job-legacy'
                  AND provider_key = 'modelsai'
                """
            ) == 1
            assert await connection.fetchval(
                """
                SELECT count(*) FROM video_provider_configs
                WHERE id = 'gemini-migration-config-legacy'
                  AND provider_key = 'modelsai'
                """
            ) == 1
        finally:
            await connection.close()

        _run_alembic(url, "upgrade", "head")
        _run_alembic(url, "check")


@pytest.mark.asyncio
@pytest.mark.parametrize("gemini_row", ["provider_config", "video_job"])
async def test_gemini_provider_downgrade_fails_closed_without_deleting_data(
    gemini_row: str,
) -> None:
    url = _database_url()
    async with _isolated_gemini_migration(url) as restore_revision:
        _reset_to_previous_revision(url)
        _run_alembic(url, "upgrade", "head")
        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            revision_before_downgrade = await connection.fetchval(
                "SELECT version_num FROM alembic_version"
            )
            assert isinstance(revision_before_downgrade, str)
            assert revision_before_downgrade == restore_revision
            if gemini_row == "provider_config":
                await connection.execute(
                    """
                    INSERT INTO video_provider_configs (
                        id, provider_key, job_type, model, is_active
                    ) VALUES (
                        'gemini-migration-config-fail', 'gemini', 'all_reference',
                        'veo-test', false
                    )
                    """
                )
            else:
                await _insert_video_job(
                    connection, provider_key="gemini", suffix="fail", attempt=1
                )
        finally:
            await connection.close()

        result = _run_alembic(
            url, "downgrade", _PREVIOUS_REVISION, check=False
        )
        assert result.returncode != 0
        assert "存在 Gemini 视频供应商数据，拒绝降级以避免数据丢失" in result.stderr

        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            assert (
                await connection.fetchval("SELECT version_num FROM alembic_version")
                == revision_before_downgrade
            )
            if gemini_row == "provider_config":
                assert await connection.fetchval(
                    """
                    SELECT count(*) FROM video_provider_configs
                    WHERE id = 'gemini-migration-config-fail'
                      AND provider_key = 'gemini'
                    """
                ) == 1
            else:
                assert await connection.fetchval(
                    """
                    SELECT count(*) FROM video_generation_jobs
                    WHERE job_id = 'gemini-migration-job-fail'
                      AND provider_key = 'gemini'
                    """
                ) == 1
        finally:
            await connection.close()

        await _cleanup_gemini_database(url)
        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
