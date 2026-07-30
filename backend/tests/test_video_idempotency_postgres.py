"""视频生成幂等映射、并发与行锁的真实 PostgreSQL 测试。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import wraps
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi import HTTPException
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.errors import ConflictError, ForbiddenError
from app.models.user import User
from app.schemas.video import VideoJobBatchCreateIn

_BACKEND_DIR = Path(__file__).resolve().parents[1]
_PYTHON = _BACKEND_DIR / ".venv" / "bin" / "python"
_ALEMBIC_CONFIG = _BACKEND_DIR / "alembic.ini"
_DATABASE_URL_ENV = "MTV_TEST_POSTGRES_URL"
_PREVIOUS_REVISION = "b31d8f4a6c20"
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
        pytest.skip(f"未配置 {_DATABASE_URL_ENV}，跳过真实 PostgreSQL 测试")
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


def _reset_to_revision(url: str, revision: str) -> None:
    _run_alembic(url, "downgrade", "base")
    _run_alembic(url, "upgrade", revision)


def _head_revision() -> str:
    config = Config(str(_ALEMBIC_CONFIG))
    config.set_main_option("script_location", str(_BACKEND_DIR / "alembic"))
    revision = ScriptDirectory.from_config(config).get_current_head()
    assert isinstance(revision, str)
    return revision


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
    raise BaseExceptionGroup("视频幂等测试主体与数据库隔离恢复均失败", failures)


async def _database_revision(url: str) -> str:
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        revision = await connection.fetchval("SELECT version_num FROM alembic_version")
        assert isinstance(revision, str)
        return revision
    finally:
        await connection.close()


async def _cleanup_runtime_rows(connection: asyncpg.Connection) -> None:
    async with connection.transaction():
        await connection.execute(
            """
            DELETE FROM task_dispatch_outbox AS dispatch
            WHERE dispatch.task_type = 'video.generate'
              AND dispatch.payload ->> 'script_id' IN ('idem-script', 'foreign-script')
              AND dispatch.payload ->> 'job_id' = dispatch.job_id
              AND (
                  NOT EXISTS (
                      SELECT 1 FROM video_generation_jobs AS job
                      WHERE job.job_id = dispatch.job_id
                  )
                  OR EXISTS (
                      SELECT 1 FROM video_generation_jobs AS job
                      WHERE job.job_id = dispatch.job_id
                        AND job.script_id = dispatch.payload ->> 'script_id'
                        AND job.idempotency_key IN (
                            'history-key-a', 'history-key-b', '   ', 'primary-key',
                            'contended-key', 'foreign-lock-key', 'batch-lock-key:0'
                        )
                  )
              )
            """
        )
        if await connection.fetchval(
            "SELECT to_regclass('video_generation_idempotency_requests')"
        ) is not None:
            await connection.execute(
                """
                DELETE FROM video_generation_idempotency_requests
                WHERE script_id IN ('idem-script', 'foreign-script')
                   OR job_id = 'alias-job'
                """
            )
        await connection.execute(
            """
            DELETE FROM video_generation_jobs
            WHERE script_id IN ('idem-script', 'foreign-script')
               OR job_id = 'alias-job'
            """
        )
        await connection.execute(
            "DELETE FROM video_provider_configs WHERE id = 'idem-provider'"
        )
        await connection.execute(
            """
            DELETE FROM shot_groups
            WHERE shot_group_id IN ('idem-group-a', 'idem-group-b', 'foreign-group')
            """
        )
        await connection.execute(
            """
            DELETE FROM episodes
            WHERE episode_id IN ('idem-episode', 'foreign-episode')
            """
        )
        await connection.execute(
            "DELETE FROM scripts WHERE id IN ('idem-script', 'foreign-script')"
        )


async def _cleanup_runtime_database(url: str) -> None:
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    cleanup_error: BaseException | None = None
    try:
        await _cleanup_runtime_rows(connection)
    except BaseException as error:
        cleanup_error = error

    close_errors: list[BaseException] = []
    try:
        await connection.close()
    except BaseException as error:
        close_errors.append(error)
    _raise_isolation_failures(cleanup_error, close_errors)


async def _assert_runtime_rows_removed(url: str) -> None:
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        mapping_count = 0
        if await connection.fetchval(
            "SELECT to_regclass('video_generation_idempotency_requests')"
        ) is not None:
            mapping_count = await connection.fetchval(
                """
                SELECT count(*) FROM video_generation_idempotency_requests
                WHERE script_id IN ('idem-script', 'foreign-script')
                   OR job_id = 'alias-job'
                """
            )
        counts = {
            "outbox": await connection.fetchval(
                """
                SELECT count(*) FROM task_dispatch_outbox
                WHERE task_type = 'video.generate'
                  AND payload ->> 'script_id' IN ('idem-script', 'foreign-script')
                  AND payload ->> 'job_id' = job_id
                """
            ),
            "mappings": mapping_count,
            "jobs": await connection.fetchval(
                """
                SELECT count(*) FROM video_generation_jobs
                WHERE script_id IN ('idem-script', 'foreign-script')
                   OR job_id = 'alias-job'
                """
            ),
            "provider": await connection.fetchval(
                "SELECT count(*) FROM video_provider_configs WHERE id = 'idem-provider'"
            ),
            "groups": await connection.fetchval(
                """
                SELECT count(*) FROM shot_groups
                WHERE shot_group_id IN ('idem-group-a', 'idem-group-b', 'foreign-group')
                """
            ),
            "episodes": await connection.fetchval(
                """
                SELECT count(*) FROM episodes
                WHERE episode_id IN ('idem-episode', 'foreign-episode')
                """
            ),
            "scripts": await connection.fetchval(
                "SELECT count(*) FROM scripts WHERE id IN ('idem-script', 'foreign-script')"
            ),
        }
        remaining = {name: count for name, count in counts.items() if count}
        if remaining:
            raise AssertionError(f"视频幂等测试固定数据未清理: {remaining}")
    finally:
        await connection.close()


@asynccontextmanager
async def _isolated_runtime_rows(url: str) -> AsyncIterator[str]:
    restore_revision = _head_revision()
    primary_error: BaseException | None = None
    try:
        yield restore_revision
    except BaseException as error:
        primary_error = error

    isolation_errors: list[BaseException] = []
    for attempt in (1, 2):
        try:
            await _cleanup_runtime_database(url)
        except BaseException as error:
            error.add_note(f"视频幂等测试第 {attempt} 次固定数据清理失败")
            isolation_errors.append(error)
        try:
            _run_alembic(url, "upgrade", restore_revision)
        except BaseException as error:
            error.add_note(f"视频幂等测试第 {attempt} 次 head 恢复失败")
            isolation_errors.append(error)
    try:
        actual_revision = await _database_revision(url)
        if actual_revision != restore_revision:
            raise AssertionError(
                f"视频幂等测试结束版本应为 {restore_revision}，实际为 {actual_revision}"
            )
    except BaseException as error:
        error.add_note("视频幂等测试最终 revision 校验失败")
        isolation_errors.append(error)
    try:
        await _assert_runtime_rows_removed(url)
    except BaseException as error:
        error.add_note("视频幂等测试最终固定数据校验失败")
        isolation_errors.append(error)
    _raise_isolation_failures(primary_error, isolation_errors)


def _with_isolated_runtime_rows[**P](
    test: Callable[P, Awaitable[None]],
) -> Callable[P, Awaitable[None]]:
    @wraps(test)
    async def isolated(*args: P.args, **kwargs: P.kwargs) -> None:
        async with _isolated_runtime_rows(_database_url()):
            await test(*args, **kwargs)

    return isolated


def _request_hash(script_id: str, episode_id: str, shot_group_id: str) -> str:
    canonical = json.dumps(
        {
            "script_id": script_id,
            "episode_id": episode_id,
            "shot_group_id": shot_group_id,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


async def _seed_runtime_rows(connection: asyncpg.Connection) -> None:
    await connection.execute(
        """
        INSERT INTO scripts (id, title, scope, owner_user_id, created_by_user_id)
        VALUES
            ('idem-script', '幂等并发测试', 'personal', 'idem-user', 'idem-user'),
            ('foreign-script', '跨剧本锁测试', 'personal', 'idem-user', 'idem-user')
        """
    )
    await connection.execute(
        """
        INSERT INTO episodes (
            episode_id, script_id, episode_no, title,
            schema_version, episode_version, client_save_seq
        )
        VALUES
            ('idem-episode', 'idem-script', 1, '第一集', 1, 1, 0),
            ('foreign-episode', 'foreign-script', 1, '其他分集', 1, 1, 0)
        """
    )
    has_reference_image_audit = bool(
        await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'shot_groups'
                  AND column_name = 'reference_image_audit'
            )
            """
        )
    )
    audit_column = ", reference_image_audit" if has_reference_image_audit else ""
    audit_value = ", '{}'::jsonb" if has_reference_image_audit else ""
    await connection.execute(
        f"""
        INSERT INTO shot_groups (
            shot_group_id, script_id, episode_id, scene_no, group_index, group_no,
            prompt, reference_frames, duration_seconds, materials, audio_elements,
            description_text, unit_id, unit_runtime_key{audit_column}
        )
        VALUES
            (
                'idem-group-a', 'idem-script', 'idem-episode', 1, 1, 'G1',
                '镜头 A', '{{}}'::jsonb, 3, '{{}}'::jsonb, '{{}}'::jsonb, '',
                'group:idem-group-a', 'idem-episode:1:group:idem-group-a'{audit_value}
            ),
            (
                'idem-group-b', 'idem-script', 'idem-episode', 1, 2, 'G2',
                '镜头 B', '{{}}'::jsonb, 3, '{{}}'::jsonb, '{{}}'::jsonb, '',
                'group:idem-group-b', 'idem-episode:1:group:idem-group-b'{audit_value}
            ),
            (
                'foreign-group', 'foreign-script', 'foreign-episode', 1, 1, 'G1',
                '其他镜头', '{{}}'::jsonb, 3, '{{}}'::jsonb, '{{}}'::jsonb, '',
                'group:foreign-group', 'foreign-episode:1:group:foreign-group'{audit_value}
            )
        """
    )
    await connection.execute(
        """
        INSERT INTO video_provider_configs (id, provider_key, job_type, model, is_active)
        VALUES (
            'idem-provider', 'modelsai', 'all_reference',
            'doubao-seedance-2-0-260128', true
        )
        """
    )


async def _insert_historical_job(
    connection: asyncpg.Connection,
    *,
    job_id: str,
    key: str,
    request_hash: str | None,
    shot_group_id: str,
    attempt: int,
) -> None:
    await connection.execute(
        """
        INSERT INTO video_generation_jobs (
            job_id, script_id, episode_id, shot_group_id, owner_user_id,
            idempotency_key, request_hash, input_version, attempt,
            scene_no, unit_id, unit_runtime_key, provider_key, job_type,
            status, progress, retryable, input_json, metrics, version
        )
        VALUES (
            $1, 'idem-script', 'idem-episode', $2::text, 'idem-user', $3, $4, 1, $5,
            1, 'group:' || $2::text, 'idem-episode:1:group:' || $2::text,
            'modelsai', 'all_reference', 'succeeded', 100, false,
            '{}'::jsonb, '{}'::jsonb, 0
        )
        """,
        job_id,
        shot_group_id,
        key,
        request_hash,
        attempt,
    )


@pytest.mark.asyncio
@_with_isolated_runtime_rows
async def test_idempotency_mapping_migration_backfills_and_round_trips() -> None:
    url = _database_url()
    _reset_to_revision(url, _PREVIOUS_REVISION)
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await _seed_runtime_rows(connection)
        expected_a = _request_hash("idem-script", "idem-episode", "idem-group-a")
        expected_b = _request_hash("idem-script", "idem-episode", "idem-group-b")
        await _insert_historical_job(
            connection,
            job_id="history-job-a",
            key="history-key-a",
            request_hash=expected_a,
            shot_group_id="idem-group-a",
            attempt=1,
        )
        await _insert_historical_job(
            connection,
            job_id="history-job-b",
            key="history-key-b",
            request_hash=None,
            shot_group_id="idem-group-b",
            attempt=1,
        )
    finally:
        await connection.close()

    _run_alembic(url, "upgrade", "head")
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        rows = await connection.fetch(
            """
            SELECT idempotency_key, request_hash, job_id
            FROM video_generation_idempotency_requests
            ORDER BY idempotency_key
            """
        )
        assert [dict(row) for row in rows] == [
            {
                "idempotency_key": "history-key-a",
                "request_hash": expected_a,
                "job_id": "history-job-a",
            },
            {
                "idempotency_key": "history-key-b",
                "request_hash": expected_b,
                "job_id": "history-job-b",
            },
        ]
        assert await connection.fetchval(
            "SELECT request_hash FROM video_generation_jobs WHERE job_id = 'history-job-b'"
        ) == expected_b
    finally:
        await connection.close()

    _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        assert await connection.fetchval(
            "SELECT to_regclass('video_generation_idempotency_requests')"
        ) is None
        assert await connection.fetchval(
            "SELECT request_hash FROM video_generation_jobs WHERE job_id = 'history-job-b'"
        ) == expected_b
    finally:
        await connection.close()

    _run_alembic(url, "upgrade", "head")
    _run_alembic(url, "check")
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        assert await connection.fetchval(
            "SELECT count(*) FROM video_generation_idempotency_requests"
        ) == 2
        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_historical_job(
                connection,
                job_id="history-job-duplicate",
                key="history-key-a",
                request_hash=expected_a,
                shot_group_id="idem-group-a",
                attempt=2,
            )
    finally:
        await connection.close()


@pytest.mark.asyncio
@_with_isolated_runtime_rows
async def test_idempotency_mapping_migration_rejects_blank_historical_key() -> None:
    url = _database_url()
    _reset_to_revision(url, _PREVIOUS_REVISION)
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await _seed_runtime_rows(connection)
        await _insert_historical_job(
            connection,
            job_id="history-job-blank",
            key="   ",
            request_hash=None,
            shot_group_id="idem-group-a",
            attempt=1,
        )
    finally:
        await connection.close()

    result = _run_alembic(url, "upgrade", "head", check=False)
    assert result.returncode != 0
    assert "存在空白视频生成幂等键，拒绝迁移" in result.stderr
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        assert await connection.fetchval("SELECT version_num FROM alembic_version") == (
            _PREVIOUS_REVISION
        )
        assert await connection.fetchval(
            "SELECT count(*) FROM video_generation_jobs WHERE job_id = 'history-job-blank'"
        ) == 1
        await connection.execute(
            "DELETE FROM video_generation_jobs WHERE job_id = 'history-job-blank'"
        )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_idempotency_mapping_downgrade_rejects_alias_loss() -> None:
    url = _database_url()
    async with _isolated_runtime_rows(url) as restore_revision:
        _reset_to_revision(url, "head")
        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            await _seed_runtime_rows(connection)
            request_hash = _request_hash("idem-script", "idem-episode", "idem-group-a")
            await _insert_historical_job(
                connection,
                job_id="alias-job",
                key="primary-key",
                request_hash=request_hash,
                shot_group_id="idem-group-a",
                attempt=1,
            )
            await connection.execute(
                """
                INSERT INTO video_generation_idempotency_requests (
                    script_id, idempotency_key, request_hash, job_id
                ) VALUES ('idem-script', 'primary-key', $1, 'alias-job')
                """,
                request_hash,
            )
            await connection.execute(
                """
                INSERT INTO video_generation_idempotency_requests (
                    script_id, idempotency_key, request_hash, job_id
                ) VALUES ('idem-script', 'alias-key', $1, 'alias-job')
                """,
                request_hash,
            )
            revision_before_downgrade = await connection.fetchval(
                "SELECT version_num FROM alembic_version"
            )
            assert isinstance(revision_before_downgrade, str)
            assert revision_before_downgrade == restore_revision
        finally:
            await connection.close()

        result = _run_alembic(url, "downgrade", _PREVIOUS_REVISION, check=False)
        assert result.returncode != 0
        assert "存在视频生成幂等别名或映射漂移" in result.stderr
        connection = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            assert (
                await connection.fetchval("SELECT version_num FROM alembic_version")
                == revision_before_downgrade
            )
            assert await connection.fetchval(
                "SELECT count(*) FROM video_generation_idempotency_requests"
            ) == 2
            await connection.execute(
                """
                DELETE FROM video_generation_idempotency_requests
                WHERE script_id = 'idem-script'
                  AND idempotency_key = 'alias-key'
                  AND job_id = 'alias-job'
                """
            )
        finally:
            await connection.close()

        _run_alembic(url, "downgrade", _PREVIOUS_REVISION)
        _run_alembic(url, "upgrade", "head")


@pytest.mark.asyncio
@_with_isolated_runtime_rows
async def test_two_connections_contending_for_same_key_choose_one_authoritative_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = _database_url()
    _reset_to_revision(url, "head")
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await _seed_runtime_rows(connection)
    finally:
        await connection.close()

    from app.services import video_generation

    arrived = 0
    arrived_lock = asyncio.Lock()
    both_arrived = asyncio.Event()

    async def aggregate_references(_db, _shot_group_id):
        nonlocal arrived
        async with arrived_lock:
            arrived += 1
            if arrived == 2:
                both_arrived.set()
        await asyncio.wait_for(both_arrived.wait(), timeout=5)
        return SimpleNamespace(
            reference_images=[],
            reference_image_names=[],
            reference_audios=[],
            reference_audio_names=[],
            missing_subjects=[],
            missing_audio=[],
        )

    monkeypatch.setattr(video_generation, "aggregate_references", aggregate_references)
    monkeypatch.setattr(video_generation, "_credits_reserve", None)
    engine = create_async_engine(url, pool_size=2, max_overflow=0)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async def create(shot_group_id: str):
        async with session_factory() as db:
            return await video_generation.create_video_job(
                db,
                script_id="idem-script",
                episode_id="idem-episode",
                shot_group_id=shot_group_id,
                user_id="idem-user",
                idempotency_key="contended-key",
            )

    try:
        results = await asyncio.gather(
            create("idem-group-a"),
            create("idem-group-b"),
            return_exceptions=True,
        )
    finally:
        await engine.dispose()

    jobs = [result for result in results if not isinstance(result, BaseException)]
    conflicts = [result for result in results if isinstance(result, ConflictError)]
    assert len(jobs) == 1
    assert len(conflicts) == 1
    assert conflicts[0].detail == "幂等键已用于不同请求载荷"
    winning_job = jobs[0]
    assert winning_job.shot_group_id in {"idem-group-a", "idem-group-b"}

    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        binding = await connection.fetchrow(
            """
            SELECT request_hash, job_id
            FROM video_generation_idempotency_requests
            WHERE script_id = 'idem-script' AND idempotency_key = 'contended-key'
            """
        )
        assert binding is not None
        assert binding["job_id"] == winning_job.job_id
        assert binding["request_hash"] == _request_hash(
            "idem-script", "idem-episode", winning_job.shot_group_id
        )
        assert await connection.fetchval(
            """
            SELECT count(*) FROM video_generation_jobs
            WHERE script_id = 'idem-script' AND idempotency_key = 'contended-key'
            """
        ) == 1
    finally:
        await connection.close()


@pytest.mark.asyncio
@_with_isolated_runtime_rows
async def test_two_connections_verify_cross_project_and_batch_failure_release_locks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = _database_url()
    _reset_to_revision(url, "head")
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await _seed_runtime_rows(connection)
    finally:
        await connection.close()

    from app.api.v1 import video_generation as video_api
    from app.services import video_generation

    monkeypatch.setattr(video_generation, "_credits_reserve", None)
    engine = create_async_engine(url, pool_size=2, max_overflow=0)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    lock_connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        async with session_factory() as db:
            with pytest.raises(ForbiddenError):
                await video_generation.create_video_job(
                    db,
                    script_id="idem-script",
                    episode_id="idem-episode",
                    shot_group_id="foreign-group",
                    user_id="idem-user",
                    idempotency_key="foreign-lock-key",
                )
            with pytest.raises(HTTPException) as missing_error:
                await video_generation.create_video_job(
                    db,
                    script_id="idem-script",
                    episode_id="idem-episode",
                    shot_group_id="missing-group",
                    user_id="idem-user",
                    idempotency_key="missing-lock-key",
                )
            assert missing_error.value.status_code == 404
            assert missing_error.value.detail == "镜头组不存在"
            transaction = lock_connection.transaction()
            await transaction.start()
            try:
                assert await lock_connection.fetchval(
                    """
                    SELECT shot_group_id FROM shot_groups
                    WHERE shot_group_id = 'foreign-group'
                    FOR UPDATE NOWAIT
                    """
                ) == "foreign-group"
            finally:
                await transaction.rollback()
            await db.rollback()

        async def fail_after_lock(_db, _shot_group_id):
            raise HTTPException(status_code=400, detail="聚合失败")

        monkeypatch.setattr(video_generation, "aggregate_references", fail_after_lock)
        monkeypatch.setattr(
            video_api,
            "_load_and_check_script",
            AsyncMock(return_value=SimpleNamespace(id="idem-script")),
        )
        test_user = User(
            user_id="idem-user",
            username="idem-user",
            password_hash="test-only",
            enabled=True,
            account_type="personal",
        )
        async with session_factory() as db:
            result = await video_api.batch_create_video_generation_jobs(
                VideoJobBatchCreateIn(
                    script_id="idem-script",
                    episode_id="idem-episode",
                    shot_group_ids=["idem-group-a"],
                ),
                db=db,
                user=test_user,
                idempotency_key="batch-lock-key",
            )
            assert result.success == []
            assert [item.error_message for item in result.failed] == ["聚合失败"]
            assert not db.in_transaction()

            transaction = lock_connection.transaction()
            await transaction.start()
            try:
                assert await lock_connection.fetchval(
                    """
                    SELECT shot_group_id FROM shot_groups
                    WHERE shot_group_id = 'idem-group-a'
                    FOR UPDATE NOWAIT
                    """
                ) == "idem-group-a"
            finally:
                await transaction.rollback()
    finally:
        await lock_connection.close()
        await engine.dispose()


@pytest.mark.asyncio
@_with_isolated_runtime_rows
@pytest.mark.parametrize("resource", ["script", "episode"])
async def test_video_create_and_lifecycle_delete_follow_parent_first_lock_order(
    monkeypatch: pytest.MonkeyPatch,
    resource: str,
) -> None:
    """创建必须先锁父行，使删除等待在 Script，而不是与 ShotGroup 构成死锁。"""
    url = _database_url()
    _reset_to_revision(url, "head")
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await _seed_runtime_rows(connection)
    finally:
        await connection.close()

    from app.crud import script as script_crud
    from app.models.script import Script
    from app.services import video_generation

    aggregate_entered = asyncio.Event()
    release_create = asyncio.Event()

    async def aggregate_references(_db, shot_group_id: str):
        assert shot_group_id == "idem-group-a"
        aggregate_entered.set()
        await asyncio.wait_for(release_create.wait(), timeout=8)
        return SimpleNamespace(
            reference_images=[],
            reference_image_names=[],
            reference_audios=[],
            reference_audio_names=[],
            missing_subjects=[],
            missing_audio=[],
        )

    monkeypatch.setattr(video_generation, "aggregate_references", aggregate_references)
    monkeypatch.setattr(video_generation, "_credits_reserve", None)

    engine = create_async_engine(url, pool_size=2, max_overflow=0)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    delete_pid: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    executed_sql: list[str] = []

    def capture_sql(
        _connection,
        _cursor,
        statement: str,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        executed_sql.append(" ".join(statement.split()))

    event.listen(engine.sync_engine, "before_cursor_execute", capture_sql)

    async def create_video():
        async with session_factory() as db:
            await db.execute(text("SET LOCAL lock_timeout = '5s'"))
            await db.execute(text("SET LOCAL statement_timeout = '10s'"))
            return await video_generation.create_video_job(
                db,
                script_id="idem-script",
                episode_id="idem-episode",
                shot_group_id="idem-group-a",
                user_id="idem-user",
                idempotency_key="batch-lock-key:0",
            )

    async def delete_lifecycle_resource():
        async with session_factory() as db:
            await db.execute(text("SET LOCAL lock_timeout = '5s'"))
            await db.execute(text("SET LOCAL statement_timeout = '10s'"))
            delete_pid.set_result(
                int((await db.execute(text("SELECT pg_backend_pid()"))).scalar_one())
            )
            if resource == "script":
                script = (
                    await db.execute(select(Script).where(Script.id == "idem-script"))
                ).scalar_one()
                return await script_crud.delete_script(db, script)
            return await script_crud.delete_episode(
                db,
                "idem-script",
                "idem-episode",
            )

    async def wait_until_delete_waits_for_lock(pid: int) -> str:
        monitor = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            async with asyncio.timeout(8):
                while True:
                    row = await monitor.fetchrow(
                        """
                        SELECT wait_event_type, query
                        FROM pg_stat_activity
                        WHERE pid = $1
                        """,
                        pid,
                    )
                    if row is not None and row["wait_event_type"] == "Lock":
                        return str(row["query"])
                    await asyncio.sleep(0.02)
        finally:
            await monitor.close()

    create_task = asyncio.create_task(create_video())
    delete_task: asyncio.Task | None = None
    try:
        await asyncio.wait_for(aggregate_entered.wait(), timeout=8)
        delete_task = asyncio.create_task(delete_lifecycle_resource())
        pid = await asyncio.wait_for(delete_pid, timeout=8)
        blocked_query = await wait_until_delete_waits_for_lock(pid)
        release_create.set()
        results = await asyncio.wait_for(
            asyncio.gather(create_task, delete_task, return_exceptions=True),
            timeout=12,
        )
    finally:
        release_create.set()
        pending = [task for task in (create_task, delete_task) if task is not None]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        event.remove(engine.sync_engine, "before_cursor_execute", capture_sql)
        await engine.dispose()

    failures = [result for result in results if isinstance(result, BaseException)]
    failure_details = [
        {
            "type": type(error).__name__,
            "sqlstate": getattr(getattr(error, "orig", None), "sqlstate", None),
            "detail": str(error),
        }
        for error in failures
    ]
    assert failures == [], failure_details
    normalized_query = " ".join(blocked_query.split())
    assert "FROM scripts" in normalized_query
    assert "FOR UPDATE" in normalized_query
    script_key_share = next(
        index
        for index, sql in enumerate(executed_sql)
        if "FROM scripts" in sql and "FOR KEY SHARE" in sql
    )
    episode_key_share = next(
        index
        for index, sql in enumerate(executed_sql)
        if "FROM episodes" in sql and "FOR KEY SHARE" in sql
    )
    shot_group_update = next(
        index
        for index, sql in enumerate(executed_sql)
        if "FROM shot_groups" in sql and "FOR UPDATE" in sql
    )
    assert script_key_share < episode_key_share < shot_group_update


@pytest.mark.asyncio
@_with_isolated_runtime_rows
async def test_cancel_refreshes_locked_video_before_merging_cleanup_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """独立事务新写入的清理债务不得被 identity map 旧快照覆盖。"""
    url = _database_url()
    _reset_to_revision(url, "head")
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await _seed_runtime_rows(connection)
        await connection.execute(
            """
            INSERT INTO video_generation_jobs (
                job_id, script_id, episode_id, shot_group_id, owner_user_id,
                idempotency_key, request_hash, input_version, attempt,
                scene_no, unit_id, unit_runtime_key, provider_key, job_type,
                status, progress, retryable, input_json, output_json, metrics, version
            ) VALUES (
                'cancel-stale-job', 'idem-script', 'idem-episode', 'idem-group-a',
                'idem-user', 'cancel-stale-key', $1, 1, 1, 1,
                'group:idem-group-a', 'idem-episode:1:group:idem-group-a',
                'modelsai', 'all_reference', 'starting', 0, false,
                '{}'::jsonb, '{"existing":"kept"}'::jsonb, '{}'::jsonb, 0
            )
            """,
            _request_hash("idem-script", "idem-episode", "idem-group-a"),
        )
    finally:
        await connection.close()

    from app.models.video_generation_job import VideoGenerationJob
    from app.services import video_generation

    monkeypatch.setattr(video_generation, "_credits_reserve", None)
    engine = create_async_engine(url, pool_size=2, max_overflow=0)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as db:
            await db.execute(text("SET LOCAL lock_timeout = '5s'"))
            await db.execute(text("SET LOCAL statement_timeout = '10s'"))
            stale = (
                await db.execute(
                    select(VideoGenerationJob).where(
                        VideoGenerationJob.job_id == "cancel-stale-job"
                    )
                )
            ).scalar_one()
            assert stale.output_json == {"existing": "kept"}

            writer = await asyncpg.connect(_asyncpg_dsn(url))
            try:
                await writer.execute(
                    """
                    UPDATE video_generation_jobs
                    SET output_json = '{
                        "existing":"kept",
                        "attempt_output_cleanup": {
                            "attempt-stale": {
                                "storage_attempt_id":"attempt-stale",
                                "video_storage_key":"generate_images/video/stale.mp4",
                                "retryable":true
                            }
                        }
                    }'::jsonb
                    WHERE job_id = 'cancel-stale-job'
                    """
                )
            finally:
                await writer.close()

            canceled = await video_generation.cancel_video_job(
                db,
                script_id="idem-script",
                job_id="cancel-stale-job",
                actor_user_id="idem-user",
            )
            assert canceled.status == "canceled"
    finally:
        await engine.dispose()

    check = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        assert await check.fetchval(
            """
            SELECT output_json #>> '{attempt_output_cleanup,attempt-stale,storage_attempt_id}'
            FROM video_generation_jobs
            WHERE job_id = 'cancel-stale-job'
            """
        ) == "attempt-stale"
        assert await check.fetchval(
            """
            SELECT output_json ->> 'existing'
            FROM video_generation_jobs
            WHERE job_id = 'cancel-stale-job'
            """
        ) == "kept"
    finally:
        await check.close()


@pytest.mark.asyncio
@_with_isolated_runtime_rows
async def test_idempotent_replay_waits_for_delete_and_rechecks_authoritative_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """幂等 fast path 不得绕过父锁返回即将被删除的任务。"""
    url = _database_url()
    _reset_to_revision(url, "head")
    request_hash = _request_hash("idem-script", "idem-episode", "idem-group-a")
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await _seed_runtime_rows(connection)
        await _insert_historical_job(
            connection,
            job_id="history-job-a",
            key="history-key-a",
            request_hash=request_hash,
            shot_group_id="idem-group-a",
            attempt=1,
        )
        await connection.execute(
            """
            INSERT INTO video_generation_idempotency_requests (
                script_id, idempotency_key, request_hash, job_id
            ) VALUES ('idem-script', 'history-key-a', $1, 'history-job-a')
            """,
            request_hash,
        )
    finally:
        await connection.close()

    from app.crud import script as script_crud
    from app.services import video_generation

    monkeypatch.setattr(video_generation, "_credits_reserve", None)
    engine = create_async_engine(url, pool_size=2, max_overflow=0)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    delete_holds_script = asyncio.Event()
    release_delete = asyncio.Event()
    replay_pid: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    class PauseAfterFirstExecute:
        def __init__(self, session) -> None:
            self.session = session
            self.paused = False

        async def execute(self, statement):
            result = await self.session.execute(statement)
            if not self.paused:
                self.paused = True
                delete_holds_script.set()
                await asyncio.wait_for(release_delete.wait(), timeout=8)
            return result

        def __getattr__(self, name: str):
            return getattr(self.session, name)

    async def delete_episode():
        async with session_factory() as db:
            await db.execute(text("SET LOCAL lock_timeout = '5s'"))
            await db.execute(text("SET LOCAL statement_timeout = '10s'"))
            return await script_crud.delete_episode(
                PauseAfterFirstExecute(db),
                "idem-script",
                "idem-episode",
            )

    async def replay_video():
        async with session_factory() as db:
            await db.execute(text("SET LOCAL lock_timeout = '5s'"))
            await db.execute(text("SET LOCAL statement_timeout = '10s'"))
            replay_pid.set_result(
                int((await db.execute(text("SELECT pg_backend_pid()"))).scalar_one())
            )
            return await video_generation.create_video_job(
                db,
                script_id="idem-script",
                episode_id="idem-episode",
                shot_group_id="idem-group-a",
                user_id="idem-user",
                idempotency_key="history-key-a",
            )

    async def wait_until_replay_blocks_or_returns(
        pid: int,
        replay_task: asyncio.Task,
    ) -> str | None:
        monitor = await asyncpg.connect(_asyncpg_dsn(url))
        try:
            async with asyncio.timeout(8):
                while True:
                    if replay_task.done():
                        return None
                    row = await monitor.fetchrow(
                        """
                        SELECT wait_event_type, query
                        FROM pg_stat_activity
                        WHERE pid = $1
                        """,
                        pid,
                    )
                    if row is not None and row["wait_event_type"] == "Lock":
                        return str(row["query"])
                    await asyncio.sleep(0.02)
        finally:
            await monitor.close()

    delete_task = asyncio.create_task(delete_episode())
    replay_task: asyncio.Task | None = None
    try:
        await asyncio.wait_for(delete_holds_script.wait(), timeout=8)
        replay_task = asyncio.create_task(replay_video())
        pid = await asyncio.wait_for(replay_pid, timeout=8)
        blocked_query = await wait_until_replay_blocks_or_returns(pid, replay_task)
        release_delete.set()
        results = await asyncio.wait_for(
            asyncio.gather(delete_task, replay_task, return_exceptions=True),
            timeout=12,
        )
    finally:
        release_delete.set()
        pending = [task for task in (delete_task, replay_task) if task is not None]
        await asyncio.gather(*pending, return_exceptions=True)
        await engine.dispose()

    assert blocked_query is not None, "幂等重放在删除持锁时提前返回了旧任务"
    normalized_query = " ".join(blocked_query.split())
    assert "FROM scripts" in normalized_query
    assert "FOR KEY SHARE" in normalized_query
    assert not isinstance(results[0], BaseException)
    replay_error = results[1]
    assert isinstance(replay_error, HTTPException)
    assert replay_error.status_code == 404
    assert replay_error.detail == "镜头组不存在"
