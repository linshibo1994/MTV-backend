"""视频 worker 提交不确定性与财务补偿隔离测试。"""

from __future__ import annotations

import hashlib
import os
import secrets
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import create_engine, delete, func, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.script import Script
from app.models.shot_group import ShotGroup
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _ScalarResult:
    def __init__(self, value) -> None:
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _AsyncSessionAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.commit_count = 0
        self.rollback_count = 0
        self.executed_statements: list[object] = []
        self.poisoned = False
        self.rollback_fails = False

    async def execute(self, statement):
        if self.poisoned:
            raise RuntimeError("事务未回滚")
        self.executed_statements.append(statement)
        return self.session.execute(statement)

    async def commit(self) -> None:
        if self.poisoned:
            raise RuntimeError("污染事务不得提交")
        self.commit_count += 1
        self.session.commit()

    async def flush(self) -> None:
        if self.poisoned:
            raise RuntimeError("污染事务不得 flush")
        self.session.flush()

    async def rollback(self) -> None:
        self.rollback_count += 1
        if self.rollback_fails:
            raise RuntimeError("事务回滚失败")
        self.session.rollback()
        self.poisoned = False


class _AdapterContext:
    def __init__(self, adapter: _AsyncSessionAdapter) -> None:
        self.adapter = adapter

    async def __aenter__(self) -> _AsyncSessionAdapter:
        return self.adapter

    async def __aexit__(self, *_args) -> None:
        self.adapter.session.close()


def _fresh_financial_session_factory(
    engine,
    adapters: list[_AsyncSessionAdapter],
):
    def factory() -> _AdapterContext:
        adapter = _AsyncSessionAdapter(Session(engine, expire_on_commit=False))
        adapters.append(adapter)
        return _AdapterContext(adapter)

    return factory


def _make_financial_db() -> tuple[object, Session, SimpleNamespace]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table in (
        User.__table__,
        Script.__table__,
        Episode.__table__,
        ShotGroup.__table__,
        CreditAccount.__table__,
        CreditReservation.__table__,
        VideoGenerationJob.__table__,
    ):
        table.create(engine)
    raw = Session(engine, expire_on_commit=False)
    user = User(
        user_id="commit-user",
        username="commit-user",
        password_hash="hash",
        enabled=True,
        account_type="personal",
    )
    script = Script(
        id="commit-script",
        title="提交不确定性",
        scope="personal",
        owner_user_id=user.user_id,
        created_by_user_id=user.user_id,
    )
    episode = Episode(
        episode_id="commit-episode",
        script_id=script.id,
        episode_no=1,
        title="第一集",
        episode_version=1,
    )
    shot_group = ShotGroup(
        shot_group_id="commit-group",
        script_id=script.id,
        episode_id=episode.episode_id,
        scene_no=1,
        group_index=1,
        group_no="G1",
        prompt="镜头",
        duration_seconds=6,
        unit_runtime_key="commit-episode:1:G1",
    )
    account = CreditAccount(
        id="commit-account",
        owner_type="user",
        owner_id=user.user_id,
        balance_points=5000,
        frozen_points=1000,
        status="active",
    )
    raw.add_all([user, script, episode, shot_group, account])
    raw.commit()
    return engine, raw, SimpleNamespace(
        user=user,
        script=script,
        episode=episode,
        shot_group=shot_group,
        account=account,
    )


def _add_financial_job(
    raw: Session,
    seed: SimpleNamespace,
    *,
    suffix: str,
    reservation_status: str | None = "reserved",
    include_payload_reservation: bool = False,
) -> tuple[VideoGenerationJob, CreditReservation | None]:
    job_id = f"financial-job-{suffix}"
    reservation_id = f"financial-reservation-{suffix}"
    reservation = None
    if reservation_status is not None:
        reservation = CreditReservation(
            id=reservation_id,
            account_id=seed.account.id,
            source_type="generation_job",
            source_id=job_id,
            reserved_points=210,
            status=reservation_status,
            actual_points=900 if reservation_status in {"captured", "refunded"} else None,
        )
    job = VideoGenerationJob(
        job_id=job_id,
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        shot_group_id=seed.shot_group.shot_group_id,
        owner_user_id=seed.user.user_id,
        scene_no=1,
        unit_id=f"group:{seed.shot_group.shot_group_id}:{suffix}",
        unit_runtime_key=f"{seed.episode.episode_id}:1:{suffix}",
        provider_key="modelsai",
        job_type="all_reference",
        idempotency_key=f"financial-{suffix}",
        request_hash="a" * 64,
        input_version=1,
        attempt=1,
        reservation_id=reservation_id if include_payload_reservation else None,
        status="failed",
        error_type="financial_cleanup",
        retryable=True,
        input_json=(
            {"reservation_id": reservation_id}
            if include_payload_reservation
            else {}
        ),
    )
    raw.add(job)
    if reservation is not None:
        raw.add(reservation)
    raw.commit()
    return job, reservation


async def test_financial_cleanup_keeps_debt_when_release_does_not_change_authoritative_source(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    engine, raw, seed = _make_financial_db()
    job, reservation = _add_financial_job(
        raw,
        seed,
        suffix="source-lookup",
        include_payload_reservation=False,
    )
    assert reservation is not None
    db = _AsyncSessionAdapter(raw)
    release = AsyncMock(return_value=None)
    monkeypatch.setattr(worker, "_credits_release", release)

    try:
        assert await worker.reconcile_video_financial_cleanup(db) == 0
        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved is not None and saved.retryable is True
        release.assert_awaited_once_with(
            db,
            reservation_id=reservation.id,
            reason="video financial cleanup",
            actor_user_id=job.owner_user_id,
            commit=False,
        )
    finally:
        raw.close()
        engine.dispose()


async def test_financial_cleanup_clears_debt_after_authoritative_source_transition(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    engine, raw, seed = _make_financial_db()
    job, reservation = _add_financial_job(
        raw,
        seed,
        suffix="source-transition",
        include_payload_reservation=False,
    )
    assert reservation is not None
    db = _AsyncSessionAdapter(raw)

    async def release(release_db, *, reservation_id: str, **_kwargs) -> None:
        authoritative = release_db.session.get(CreditReservation, reservation_id)
        assert authoritative is not None
        authoritative.status = "released"

    monkeypatch.setattr(worker, "_credits_release", release)

    try:
        assert await worker.reconcile_video_financial_cleanup(db) == 1
        raw.expire_all()
        saved_job = raw.get(VideoGenerationJob, job.job_id)
        saved_reservation = raw.get(CreditReservation, reservation.id)
        assert saved_job is not None and saved_job.retryable is False
        assert saved_reservation is not None and saved_reservation.status == "released"
    finally:
        raw.close()
        engine.dispose()


@pytest.mark.parametrize(
    ("reservation_status", "expected_recovered", "expected_retryable"),
    [
        ("reserved", 0, True),
        ("released", 1, False),
        ("captured", 1, False),
        (None, 1, False),
    ],
)
async def test_financial_cleanup_only_clears_authoritatively_resolved_debt(
    monkeypatch,
    reservation_status: str | None,
    expected_recovered: int,
    expected_retryable: bool,
) -> None:
    from app.workers import video_generation as worker

    engine, raw, seed = _make_financial_db()
    job, _ = _add_financial_job(
        raw,
        seed,
        suffix=f"unavailable-{reservation_status}",
        reservation_status=reservation_status,
    )
    db = _AsyncSessionAdapter(raw)
    monkeypatch.setattr(worker, "_credits_release", None)

    try:
        assert (
            await worker.reconcile_video_financial_cleanup(db)
            == expected_recovered
        )
        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved is not None and saved.retryable is expected_retryable
    finally:
        raw.close()
        engine.dispose()


async def test_financial_cleanup_uses_skip_locked_and_isolates_each_job(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    engine, raw, seed = _make_financial_db()
    failed_job, failed_reservation = _add_financial_job(
        raw,
        seed,
        suffix="failure-first",
    )
    recovered_job, recovered_reservation = _add_financial_job(
        raw,
        seed,
        suffix="success-second",
    )
    assert failed_reservation is not None
    assert recovered_reservation is not None
    scan_db = _AsyncSessionAdapter(raw)
    task_dbs: list[_AsyncSessionAdapter] = []
    session_factory = _fresh_financial_session_factory(engine, task_dbs)

    async def release(release_db, *, reservation_id: str, **_kwargs) -> None:
        if reservation_id == failed_reservation.id:
            release_db.poisoned = True
            release_db.rollback_fails = True
            raise RuntimeError("release backend failed")
        authoritative = release_db.session.get(CreditReservation, reservation_id)
        assert authoritative is not None
        authoritative.status = "released"

    monkeypatch.setattr(worker, "_credits_release", release)

    try:
        assert (
            await worker.reconcile_video_financial_cleanup(
                scan_db,
                session_factory=session_factory,
            )
            == 1
        )
        raw.expire_all()
        failed = raw.get(VideoGenerationJob, failed_job.job_id)
        recovered = raw.get(VideoGenerationJob, recovered_job.job_id)
        assert failed is not None and failed.retryable is True
        assert recovered is not None and recovered.retryable is False
        assert len(task_dbs) == 2
        assert task_dbs[0].rollback_count == 1
        assert task_dbs[0].rollback_fails is True
        assert task_dbs[1].commit_count == 1
        postgres_sql = [
            " ".join(str(statement.compile(dialect=postgresql.dialect())).split())
            for task_db in [scan_db, *task_dbs]
            for statement in task_db.executed_statements
            if getattr(statement, "is_select", False)
        ]
        assert any("FOR UPDATE SKIP LOCKED" in sql for sql in postgres_sql)
    finally:
        raw.close()
        engine.dispose()


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 财务补偿测试",
)
async def test_postgres_financial_cleanup_skips_locked_and_recovers_after_aborted_transaction(
    monkeypatch,
) -> None:
    """真实 PG 锁竞争和 aborted 事务不能造成重复释放或阻断后续任务。"""

    from app.workers import video_generation as worker

    database_url = os.environ["MTV_TEST_POSTGRES_URL"]
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace(
            "postgresql://",
            "postgresql+asyncpg://",
            1,
        )
    engine = create_async_engine(database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    suffix = secrets.token_hex(6)
    user_id = f"pg-fin-user-{suffix}"
    script_id = f"pg-fin-script-{suffix}"
    episode_id = f"pg-fin-episode-{suffix}"
    shot_group_id = f"pg-fin-group-{suffix}"
    account_id = f"pg-fin-account-{suffix}"
    job_ids = {
        "locked": f"pg-fin-a-{suffix}",
        "failed": f"pg-fin-b-{suffix}",
        "success": f"pg-fin-c-{suffix}",
    }
    reservation_ids = {
        key: f"pg-fin-r-{key[0]}-{suffix}" for key in job_ids
    }
    created_at = datetime.now(UTC)

    real_release = worker._credits_release
    assert real_release is not None

    async def release_with_aborted_first_transaction(
        db,
        *,
        reservation_id: str,
        **kwargs,
    ) -> None:
        if reservation_id == reservation_ids["failed"]:
            # PostgreSQL 执行错误会令当前事务进入 aborted，只有 rollback 后才能继续。
            await db.execute(text("SELECT 1 / 0"))
            raise AssertionError("除零查询应当失败")
        await real_release(db, reservation_id=reservation_id, **kwargs)

    monkeypatch.setattr(
        worker,
        "_credits_release",
        release_with_aborted_first_transaction,
    )

    try:
        async with session_factory() as seed_db:
            seed_db.add_all(
                [
                    User(
                        user_id=user_id,
                        username=user_id,
                        password_hash="hash",
                        enabled=True,
                        account_type="personal",
                    ),
                    Script(
                        id=script_id,
                        title="真实 PostgreSQL 财务补偿",
                        scope="personal",
                        owner_user_id=user_id,
                        created_by_user_id=user_id,
                    ),
                    CreditAccount(
                        id=account_id,
                        owner_type="user",
                        owner_id=user_id,
                        balance_points=1000,
                        frozen_points=630,
                        status="active",
                    ),
                ]
            )
            await seed_db.flush()
            seed_db.add(
                Episode(
                    episode_id=episode_id,
                    script_id=script_id,
                    episode_no=1,
                    title="第一集",
                )
            )
            await seed_db.flush()
            seed_db.add(
                ShotGroup(
                    shot_group_id=shot_group_id,
                    script_id=script_id,
                    episode_id=episode_id,
                    scene_no=1,
                    group_no="G1",
                )
            )
            await seed_db.flush()
            for index, key in enumerate(("locked", "failed", "success"), start=1):
                job_id = job_ids[key]
                seed_db.add_all(
                    [
                        CreditReservation(
                            id=reservation_ids[key],
                            account_id=account_id,
                            source_type="generation_job",
                            source_id=job_id,
                            reserved_points=210,
                            status="reserved",
                        ),
                        VideoGenerationJob(
                            job_id=job_id,
                            script_id=script_id,
                            episode_id=episode_id,
                            shot_group_id=shot_group_id,
                            owner_user_id=user_id,
                            scene_no=1,
                            unit_id=f"group:{shot_group_id}:{index}",
                            unit_runtime_key=f"{episode_id}:1:G{index}",
                            provider_key="modelsai",
                            job_type="all_reference",
                            idempotency_key=f"pg-fin-{key}-{suffix}",
                            request_hash="a" * 64,
                            status="failed",
                            error_type="financial_cleanup",
                            retryable=True,
                            input_json={},
                            created_at=created_at,
                        ),
                    ]
                )
            await seed_db.commit()

        async with session_factory() as lock_db:
            await lock_db.execute(
                select(VideoGenerationJob.job_id)
                .where(VideoGenerationJob.job_id == job_ids["locked"])
                .with_for_update()
            )
            async with session_factory() as reconcile_db:
                assert (
                    await worker.reconcile_video_financial_cleanup(
                        reconcile_db,
                        session_factory=session_factory,
                    )
                    == 1
                )
            await lock_db.rollback()

        # 解锁后再处理一次：锁住的任务成功，持续失败任务仍保持可重试。
        async with session_factory() as reconcile_db:
            assert (
                await worker.reconcile_video_financial_cleanup(
                    reconcile_db,
                    session_factory=session_factory,
                )
                == 1
            )
            assert (
                await worker.reconcile_video_financial_cleanup(
                    reconcile_db,
                    session_factory=session_factory,
                )
                == 0
            )

        async with session_factory() as verify_db:
            jobs = {
                job.job_id: job
                for job in (
                    await verify_db.execute(
                        select(VideoGenerationJob).where(
                            VideoGenerationJob.job_id.in_(job_ids.values())
                        )
                    )
                )
                .scalars()
                .all()
            }
            reservations = {
                reservation.id: reservation
                for reservation in (
                    await verify_db.execute(
                        select(CreditReservation).where(
                            CreditReservation.id.in_(reservation_ids.values())
                        )
                    )
                )
                .scalars()
                .all()
            }
            account = await verify_db.get(CreditAccount, account_id)
            release_log_count = await verify_db.scalar(
                select(func.count())
                .select_from(CreditLog)
                .where(
                    CreditLog.account_id == account_id,
                    CreditLog.log_type == "release",
                )
            )

            assert jobs[job_ids["locked"]].retryable is False
            assert jobs[job_ids["success"]].retryable is False
            assert jobs[job_ids["failed"]].retryable is True
            assert reservations[reservation_ids["locked"]].status == "released"
            assert reservations[reservation_ids["success"]].status == "released"
            assert reservations[reservation_ids["failed"]].status == "reserved"
            assert account is not None and account.frozen_points == 210
            assert release_log_count == 2
    finally:
        try:
            async with session_factory() as cleanup_db:
                await cleanup_db.execute(
                    delete(CreditLog).where(CreditLog.account_id == account_id)
                )
                await cleanup_db.execute(
                    delete(VideoGenerationJob).where(
                        VideoGenerationJob.job_id.in_(job_ids.values())
                    )
                )
                await cleanup_db.execute(
                    delete(CreditReservation).where(
                        CreditReservation.id.in_(reservation_ids.values())
                    )
                )
                await cleanup_db.execute(
                    delete(CreditAccount).where(CreditAccount.id == account_id)
                )
                await cleanup_db.execute(
                    delete(ShotGroup).where(ShotGroup.shot_group_id == shot_group_id)
                )
                await cleanup_db.execute(
                    delete(Episode).where(Episode.episode_id == episode_id)
                )
                await cleanup_db.execute(delete(Script).where(Script.id == script_id))
                await cleanup_db.execute(delete(User).where(User.user_id == user_id))
                await cleanup_db.commit()
        finally:
            await engine.dispose()


class _ProgressCommitDB:
    def __init__(self, *, apply_before_raise: bool, rollback_fails: bool) -> None:
        self.apply_before_raise = apply_before_raise
        self.rollback_fails = rollback_fails
        self.commit_count = 0
        self.rollback_count = 0
        self.progress_commit_applied = False

    async def commit(self) -> None:
        self.commit_count += 1
        if self.commit_count != 2:
            return
        if self.apply_before_raise:
            self.progress_commit_applied = True
        raise ConnectionError("progress commit response unavailable")

    async def rollback(self) -> None:
        self.rollback_count += 1
        if self.rollback_fails:
            raise ConnectionError("rollback response unavailable")


@pytest.mark.parametrize(
    (
        "apply_before_raise",
        "rollback_fails",
        "authority",
        "cleanup_scheduled",
    ),
    [
        (False, False, False, True),
        (True, False, False, True),
        (True, True, False, True),
        (True, True, None, True),
        (False, False, None, False),
        (True, True, None, False),
    ],
)
async def test_progress_commit_uncertainty_keeps_attempt_key_and_finishes_failure(
    monkeypatch,
    apply_before_raise: bool,
    rollback_fails: bool,
    authority: bool | None,
    cleanup_scheduled: bool,
) -> None:
    from app.crud import video as video_crud
    from app.workers import video_generation as worker

    lease_token = "progress-commit-uncertainty"
    attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    video_key = f"generate_images/video/progress-job-{attempt_id}.mp4"
    job = SimpleNamespace(
        job_id="progress-job",
        script_id="progress-script",
        episode_id="progress-episode",
        shot_group_id="progress-group",
        input_version=1,
        input_json={"prompt": "镜头", "model": "test-model"},
        output_json={},
        metrics={},
        reservation_id="progress-reservation",
        owner_user_id="progress-user",
        provider_key="fake",
        unit_id="group:progress-group",
    )
    state = {"status": "processing", "release_count": 0}
    deleted: list[str] = []

    async def claim(*_args, **_kwargs):
        return SimpleNamespace(lease_token=lease_token)

    async def mark_failed_once(*_args, **_kwargs) -> None:
        state["status"] = "failed"
        state["release_count"] += 1

    async def mark_failed_independently(*_args, **_kwargs) -> None:
        state["status"] = "failed"
        state["release_count"] += 1

    class Provider:
        async def generate(self, request, on_progress):
            on_progress(
                SimpleNamespace(
                    progress=100,
                    stage="succeeded",
                    message="完成",
                    provider_task_id="provider-progress-job",
                )
            )
            return SimpleNamespace(
                video_storage_key=video_key,
                provider_job_id="provider-progress-job",
                metrics={},
            )

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)

    monkeypatch.setattr(video_crud, "claim_video_job", claim)
    monkeypatch.setattr(
        video_crud,
        "update_video_job_progress",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        video_crud,
        "heartbeat_video_job",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(worker, "_get_video_job", AsyncMock(return_value=job))
    monkeypatch.setattr(worker, "_input_version_is_current", AsyncMock(return_value=True))
    monkeypatch.setattr(
        worker,
        "_resolve_execution_first_frame",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(worker, "_credits_settle", None)
    monkeypatch.setattr(
        worker,
        "_authoritative_video_attempt_succeeded",
        AsyncMock(return_value=authority),
    )
    schedule_cleanup = AsyncMock(return_value=cleanup_scheduled)
    monkeypatch.setattr(worker, "_schedule_video_attempt_cleanup", schedule_cleanup)
    monkeypatch.setattr(worker, "_mark_failed_and_release_once", mark_failed_once)
    monkeypatch.setattr(worker, "_mark_failed_and_release", mark_failed_independently)
    db = _ProgressCommitDB(
        apply_before_raise=apply_before_raise,
        rollback_fails=rollback_fails,
    )

    if cleanup_scheduled:
        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
            storage=Storage(),
        )
    else:
        with pytest.raises(RuntimeError, match="video_attempt_cleanup_unavailable"):
            await worker.run_video_generation_once(
                db,
                job.job_id,
                owner="worker",
                provider=Provider(),
                extract_last_frame=AsyncMock(),
                storage=Storage(),
            )

    assert db.progress_commit_applied is apply_before_raise
    expected_state = (
        {"status": "failed", "release_count": 1}
        if cleanup_scheduled
        else {"status": "processing", "release_count": 0}
    )
    assert state == expected_state
    if authority is False:
        assert deleted == [video_key]
        schedule_cleanup.assert_not_awaited()
    else:
        assert deleted == []
        schedule_cleanup.assert_awaited_once_with(
            job_id=job.job_id,
            script_id=job.script_id,
            shot_group_id=job.shot_group_id,
            storage_attempt_id=attempt_id,
            video_storage_key=video_key,
            last_frame_storage_key=None,
            provider_task_id="provider-progress-job",
        )


async def test_cleanup_schedule_persists_and_publishes_provider_task_id(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    persist = AsyncMock(return_value=True)
    publish = Mock()
    monkeypatch.setattr(worker, "_persist_video_attempt_cleanup_debt", persist)
    monkeypatch.setattr(
        worker.reconcile_video_attempt_outputs_task,
        "apply_async",
        publish,
    )

    assert await worker._schedule_video_attempt_cleanup(
        job_id="provider-id-job",
        script_id="provider-id-script",
        shot_group_id="provider-id-group",
        storage_attempt_id="aabbccddeeff0011",
        video_storage_key="generate_images/video/provider-id-job-aabbccddeeff0011.mp4",
        last_frame_storage_key=None,
        provider_task_id="provider-task-123",
    )
    persist.assert_awaited_once_with(
        job_id="provider-id-job",
        script_id="provider-id-script",
        shot_group_id="provider-id-group",
        storage_attempt_id="aabbccddeeff0011",
        video_storage_key="generate_images/video/provider-id-job-aabbccddeeff0011.mp4",
        last_frame_storage_key=None,
        provider_task_id="provider-task-123",
    )
    publish.assert_called_once_with(
        kwargs={
            "job_id": "provider-id-job",
            "shot_group_id": "provider-id-group",
            "storage_attempt_id": "aabbccddeeff0011",
            "video_storage_key": (
                "generate_images/video/provider-id-job-aabbccddeeff0011.mp4"
            ),
            "last_frame_storage_key": None,
            "provider_task_id": "provider-task-123",
        },
        countdown=worker._ATTEMPT_CLEANUP_RETRY_SECONDS,
    )


async def test_cleanup_schedule_reports_database_and_publish_double_failure(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    persist = AsyncMock(return_value=False)
    publish = Mock(side_effect=ConnectionError("broker unavailable"))
    monkeypatch.setattr(worker, "_persist_video_attempt_cleanup_debt", persist)
    monkeypatch.setattr(
        worker.reconcile_video_attempt_outputs_task,
        "apply_async",
        publish,
    )

    assert not await worker._schedule_video_attempt_cleanup(
        job_id="double-failure-job",
        script_id="double-failure-script",
        shot_group_id="double-failure-group",
        storage_attempt_id="1122334455667788",
        video_storage_key=(
            "generate_images/video/double-failure-job-1122334455667788.mp4"
        ),
        last_frame_storage_key=None,
        provider_task_id="provider-double-failure",
    )
    persist.assert_awaited_once()
    assert persist.await_args.kwargs["provider_task_id"] == "provider-double-failure"
    publish.assert_called_once()
    assert (
        publish.call_args.kwargs["kwargs"]["provider_task_id"]
        == "provider-double-failure"
    )


@pytest.mark.parametrize("provider_task_id", [None, "provider-task-stale"])
def test_stale_lease_records_deterministic_attempt_cleanup_debt(
    provider_task_id: str | None,
) -> None:
    from app.workers import video_generation as worker

    lease_token = "stale-lease-token"
    attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    output_json = (
        {"provider_task_id": provider_task_id} if provider_task_id is not None else {}
    )
    job = SimpleNamespace(
        job_id="stale-video-job",
        shot_group_id="stale-shot-group",
        lease_token=lease_token,
        output_json=output_json,
        metrics={},
    )

    assert worker._record_stale_video_attempt_cleanup_debt(job) is True
    record = job.output_json[worker._ATTEMPT_CLEANUP_DEBT_KEY][attempt_id]
    expected_record = {
        "storage_attempt_id": attempt_id,
        "shot_group_id": job.shot_group_id,
        "video_storage_key": f"generate_images/video/{job.job_id}-{attempt_id}.mp4",
        "last_frame_storage_key": (
            "/generate_images/video/frames/"
            f"{job.shot_group_id}_{attempt_id}_last.png"
        ),
        "retryable": True,
    }
    if provider_task_id is not None:
        expected_record["provider_task_id"] = provider_task_id
    assert record == expected_record


async def test_stale_recovery_persists_attempt_debt_before_clearing_lease(
    monkeypatch,
) -> None:
    from app.crud import video as video_crud
    from app.services import task_dispatch
    from app.workers import video_generation as worker

    lease_token = "stale-recovery-lease"
    attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    job = SimpleNamespace(
        job_id="stale-recovery-job",
        script_id="stale-recovery-script",
        episode_id="stale-recovery-episode",
        shot_group_id="stale-recovery-group",
        input_version=3,
        attempt=2,
        status="processing",
        lease_owner="old-worker",
        lease_token=lease_token,
        lease_until=datetime.now(UTC),
        output_json={},
        metrics={"provider_task_id": "provider-stale-recovery"},
        dispatch_id=None,
    )

    class DB:
        def __init__(self) -> None:
            self.commit_count = 0

        async def execute(self, _statement):
            return _ScalarResult(None)

        async def commit(self) -> None:
            self.commit_count += 1

    db = DB()
    monkeypatch.setattr(
        worker,
        "reconcile_video_attempt_cleanup_debts",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        video_crud,
        "list_stale_processing_jobs",
        AsyncMock(return_value=[job]),
    )
    enqueue = AsyncMock(return_value=SimpleNamespace(id="stale-recovery-outbox"))
    monkeypatch.setattr(task_dispatch, "enqueue_task", enqueue)

    assert await worker.reconcile_stale_video_jobs(db) == [job.job_id]
    assert job.status == "starting"
    assert job.lease_owner is None
    assert job.lease_token is None
    assert job.lease_until is None
    assert job.dispatch_id == "stale-recovery-outbox"
    assert db.commit_count == 1
    assert job.output_json[worker._ATTEMPT_CLEANUP_DEBT_KEY][attempt_id] == {
        "storage_attempt_id": attempt_id,
        "shot_group_id": job.shot_group_id,
        "video_storage_key": f"generate_images/video/{job.job_id}-{attempt_id}.mp4",
        "last_frame_storage_key": (
            "/generate_images/video/frames/"
            f"{job.shot_group_id}_{attempt_id}_last.png"
        ),
        "provider_task_id": "provider-stale-recovery",
        "retryable": True,
    }
    assert job.output_json["provider_task_id"] == "provider-stale-recovery"
