"""图片与分镜财务补偿器的权威状态和事务隔离测试。"""

from __future__ import annotations

import asyncio
import os
import secrets
from collections import Counter
from dataclasses import dataclass
from typing import Literal
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema, DropSchema

from app.models.base import Base
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.script import Script
from app.models.storyboard_job import StoryboardJob
from app.models.subject import Subject
from app.models.user import User
from app.services.credits import release as authoritative_release
from app.workers import generation as generation_worker
from app.workers import storyboard as storyboard_worker

JobKind = Literal["generation", "storyboard"]


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncSessionAdapter:
    def __init__(self, session: Session, *, rollback_fails: bool = False) -> None:
        self.session = session
        self.rollback_fails = rollback_fails
        self.poisoned = False
        self.commit_count = 0
        self.rollback_count = 0
        self.executed_statements: list[object] = []

    async def execute(self, statement):
        if self.poisoned:
            raise RuntimeError("事务未回滚")
        self.executed_statements.append(statement)
        return self.session.execute(statement)

    async def get(self, *args, **kwargs):
        if self.poisoned:
            raise RuntimeError("事务未回滚")
        return self.session.get(*args, **kwargs)

    async def flush(self) -> None:
        if self.poisoned:
            raise RuntimeError("污染事务不得 flush")
        self.session.flush()

    async def commit(self) -> None:
        if self.poisoned:
            raise RuntimeError("污染事务不得提交")
        self.commit_count += 1
        self.session.commit()

    async def rollback(self) -> None:
        self.rollback_count += 1
        if self.rollback_fails:
            raise RuntimeError("事务回滚失败")
        self.session.rollback()
        self.poisoned = False

    def add(self, row) -> None:
        self.session.add(row)


class _SessionContext:
    def __init__(self, adapter: _AsyncSessionAdapter) -> None:
        self.adapter = adapter

    async def __aenter__(self) -> _AsyncSessionAdapter:
        return self.adapter

    async def __aexit__(self, *_args) -> None:
        self.adapter.session.close()


class _FreshSessionFactory:
    def __init__(self, engine, *, first_rollback_fails: bool = False) -> None:
        self.engine = engine
        self.first_rollback_fails = first_rollback_fails
        self.adapters: list[_AsyncSessionAdapter] = []

    def __call__(self) -> _SessionContext:
        adapter = _AsyncSessionAdapter(
            Session(self.engine, expire_on_commit=False),
            rollback_fails=self.first_rollback_fails and not self.adapters,
        )
        self.adapters.append(adapter)
        return _SessionContext(adapter)


class _PostScanBarrierSession:
    """在候选扫描回滚后暂停，使两个 reconciler 竞争同一候选。"""

    def __init__(
        self,
        session,
        *,
        before_scan: asyncio.Event | None,
        scan_released: asyncio.Event,
        proceed: asyncio.Event,
    ) -> None:
        self.session = session
        self.before_scan = before_scan
        self.scan_released = scan_released
        self.proceed = proceed

    async def execute(self, statement):
        if self.before_scan is not None:
            await self.before_scan.wait()
        return await self.session.execute(statement)

    async def rollback(self) -> None:
        await self.session.rollback()
        self.scan_released.set()
        await self.proceed.wait()


@dataclass(frozen=True)
class _Seed:
    user_id: str
    script_id: str
    episode_id: str
    material_id: str
    subject_id: str
    account_id: str


def _make_db(path) -> tuple[object, Session, _Seed]:
    engine = create_engine(f"sqlite+pysqlite:///{path}")
    Base.metadata.create_all(engine)

    raw = Session(engine, expire_on_commit=False)
    seed = _Seed(
        user_id="cleanup-user",
        script_id="cleanup-script",
        episode_id="cleanup-episode",
        material_id="cleanup-material",
        subject_id="cleanup-subject",
        account_id="cleanup-account",
    )
    raw.add_all(
        [
            User(
                user_id=seed.user_id,
                username="cleanup-user",
                password_hash="hash",
                enabled=True,
                account_type="personal",
            ),
            Script(
                id=seed.script_id,
                title="财务补偿",
                scope="personal",
                owner_user_id=seed.user_id,
                created_by_user_id=seed.user_id,
            ),
            Episode(
                episode_id=seed.episode_id,
                script_id=seed.script_id,
                episode_no=1,
                title="第一集",
            ),
            Materials(
                id=seed.material_id,
                script_id=seed.script_id,
                name="画布",
            ),
            Subject(
                id=seed.subject_id,
                script_id=seed.script_id,
                type="character",
                name="主角",
                episode_nos=[1],
            ),
            CreditAccount(
                id=seed.account_id,
                owner_type="user",
                owner_id=seed.user_id,
                balance_points=1000,
                frozen_points=0,
                status="active",
            ),
        ]
    )
    raw.commit()
    return engine, raw, seed


def _add_debt(
    raw: Session,
    seed: _Seed,
    *,
    kind: JobKind,
    suffix: str,
    attempt: int = 1,
    reservation_status: str | None = "reserved",
    include_payload_reservation: bool = False,
    points: int = 10,
) -> tuple[GenerationJob | StoryboardJob, CreditReservation | None]:
    job_id = f"{kind}-{suffix}"
    reservation_id = f"reservation-{kind}-{suffix}"
    reservation = None
    if reservation_status is not None:
        reservation = CreditReservation(
            id=reservation_id,
            account_id=seed.account_id,
            source_type="generation_job" if kind == "generation" else "storyboard",
            source_id=job_id,
            reserved_points=points,
            status=reservation_status,
            actual_points=(points if reservation_status in {"captured", "refunded"} else None),
        )
        raw.add(reservation)
        if reservation_status == "reserved":
            account = raw.get(CreditAccount, seed.account_id)
            assert account is not None
            account.frozen_points += points

    if kind == "generation":
        job: GenerationJob | StoryboardJob = GenerationJob(
            job_id=job_id,
            material_id=seed.material_id,
            script_id=seed.script_id,
            subject_id=seed.subject_id,
            owner_user_id=seed.user_id,
            idempotency_key=f"idem-{job_id}",
            request_hash=f"hash-{job_id}",
            input_version=1,
            attempt=attempt,
            episode_id=seed.episode_id,
            job_type="generate_images",
            status="failed",
            error_type="financial_cleanup",
            retryable=True,
            input_json=(
                {"reservation_id": reservation_id}
                if include_payload_reservation
                else {}
            ),
            metrics=(
                {
                    "financial_cleanup": {
                        "action": "release",
                        "reservation_id": reservation_id,
                        "actual_points": None,
                    }
                }
                if include_payload_reservation
                else {}
            ),
        )
    else:
        job = StoryboardJob(
            job_id=job_id,
            script_id=seed.script_id,
            episode_id=seed.episode_id,
            triggered_by_user_id=seed.user_id,
            owner_user_id=seed.user_id,
            episode_version=1,
            input_version=1,
            attempt=attempt,
            idempotency_key=f"idem-{job_id}",
            request_hash=f"hash-{job_id}",
            reservation_id=(reservation_id if include_payload_reservation else None),
            detail=(
                {"reservation_id": reservation_id}
                if include_payload_reservation
                else {}
            ),
            status="failed",
            error_type="internal_error",
            retryable=True,
            financial_cleanup_status="pending",
            financial_cleanup_error="release_failed",
        )
    raw.add(job)
    raw.commit()
    return job, reservation


async def _reconcile(kind: JobKind, db, **kwargs) -> int:
    if kind == "generation":
        return await generation_worker.reconcile_generation_jobs(db, **kwargs)
    return await storyboard_worker.reconcile_storyboard_financial_cleanup(db, **kwargs)


def _worker_release(kind: JobKind) -> tuple[object, str]:
    if kind == "generation":
        return generation_worker, "credits_release"
    return storyboard_worker, "release"


def _assert_pending(kind: JobKind, job: GenerationJob | StoryboardJob) -> None:
    if kind == "generation":
        assert isinstance(job, GenerationJob)
        assert job.retryable is True
    else:
        assert isinstance(job, StoryboardJob)
        assert job.financial_cleanup_status == "pending"


def _assert_cleared(kind: JobKind, job: GenerationJob | StoryboardJob) -> None:
    if kind == "generation":
        assert isinstance(job, GenerationJob)
        assert job.retryable is False
    else:
        assert isinstance(job, StoryboardJob)
        assert job.financial_cleanup_status == "done"
        assert job.financial_cleanup_error is None


@pytest.mark.parametrize("kind", ["generation", "storyboard"])
async def test_cleanup_recovers_missing_payload_from_authoritative_source_once(
    monkeypatch,
    tmp_path,
    kind: JobKind,
) -> None:
    engine, raw, seed = _make_db(tmp_path / f"{kind}-source.db")
    job, reservation = _add_debt(
        raw,
        seed,
        kind=kind,
        suffix="source",
        include_payload_reservation=False,
    )
    assert reservation is not None
    db = _AsyncSessionAdapter(raw)
    worker, release_name = _worker_release(kind)
    monkeypatch.setattr(worker, release_name, authoritative_release)

    try:
        assert await _reconcile(kind, db) == 1
        raw.expire_all()
        saved_job = raw.get(type(job), job.job_id)
        saved_reservation = raw.get(CreditReservation, reservation.id)
        account = raw.get(CreditAccount, seed.account_id)
        assert saved_job is not None and saved_reservation is not None and account is not None
        _assert_cleared(kind, saved_job)
        assert saved_reservation.status == "released"
        assert account.frozen_points == 0

        logs = list(
            raw.scalars(
                select(CreditLog).where(
                    CreditLog.reservation_id == reservation.id,
                    CreditLog.log_type == "release",
                )
            )
        )
        assert len(logs) == 1
        assert logs[0].actor_user_id == seed.user_id
        assert logs[0].frozen_delta == -reservation.reserved_points

        assert await _reconcile(kind, db) == 0
        assert len(
            list(
                raw.scalars(
                    select(CreditLog).where(
                        CreditLog.reservation_id == reservation.id,
                        CreditLog.log_type == "release",
                    )
                )
            )
        ) == 1

        compiled = [
            " ".join(str(statement.compile(dialect=postgresql.dialect())).split())
            for statement in db.executed_statements
            if getattr(statement, "is_select", False)
        ]
        assert sum("FOR UPDATE SKIP LOCKED" in sql for sql in compiled) >= 2
        assert any(
            "FROM credit_reservations" in sql and "FOR UPDATE" in sql
            for sql in compiled
        )
    finally:
        raw.close()
        engine.dispose()


@pytest.mark.parametrize("kind", ["generation", "storyboard"])
async def test_cleanup_keeps_debt_when_release_is_a_noop(
    monkeypatch,
    tmp_path,
    kind: JobKind,
) -> None:
    engine, raw, seed = _make_db(tmp_path / f"{kind}-noop.db")
    job, reservation = _add_debt(
        raw,
        seed,
        kind=kind,
        suffix="noop",
        include_payload_reservation=True,
    )
    assert reservation is not None
    db = _AsyncSessionAdapter(raw)
    worker, release_name = _worker_release(kind)
    release_noop = AsyncMock(return_value=None)
    monkeypatch.setattr(worker, release_name, release_noop)

    try:
        assert await _reconcile(kind, db) == 0
        raw.expire_all()
        saved_job = raw.get(type(job), job.job_id)
        saved_reservation = raw.get(CreditReservation, reservation.id)
        account = raw.get(CreditAccount, seed.account_id)
        assert saved_job is not None and saved_reservation is not None and account is not None
        _assert_pending(kind, saved_job)
        assert saved_reservation.status == "reserved"
        assert account.frozen_points == reservation.reserved_points
        release_noop.assert_awaited_once()
        assert release_noop.await_args.kwargs["actor_user_id"] == seed.user_id
        assert release_noop.await_args.kwargs["commit"] is False
    finally:
        raw.close()
        engine.dispose()


@pytest.mark.parametrize("kind", ["generation", "storyboard"])
async def test_cleanup_keeps_debt_when_release_is_unavailable(
    monkeypatch,
    tmp_path,
    kind: JobKind,
) -> None:
    engine, raw, seed = _make_db(tmp_path / f"{kind}-unavailable.db")
    job, reservation = _add_debt(
        raw,
        seed,
        kind=kind,
        suffix="unavailable",
        include_payload_reservation=False,
    )
    assert reservation is not None
    db = _AsyncSessionAdapter(raw)
    worker, release_name = _worker_release(kind)
    monkeypatch.setattr(worker, release_name, None)

    try:
        assert await _reconcile(kind, db) == 0
        raw.expire_all()
        saved_job = raw.get(type(job), job.job_id)
        assert saved_job is not None
        _assert_pending(kind, saved_job)
        assert raw.get(CreditReservation, reservation.id).status == "reserved"
    finally:
        raw.close()
        engine.dispose()


@pytest.mark.parametrize("kind", ["generation", "storyboard"])
@pytest.mark.parametrize("reservation_status", [None, "released", "captured"])
async def test_cleanup_clears_only_missing_or_nonreserved_authority(
    monkeypatch,
    tmp_path,
    kind: JobKind,
    reservation_status: str | None,
) -> None:
    engine, raw, seed = _make_db(
        tmp_path / f"{kind}-terminal-{reservation_status}.db"
    )
    job, _ = _add_debt(
        raw,
        seed,
        kind=kind,
        suffix=f"terminal-{reservation_status}",
        reservation_status=reservation_status,
        include_payload_reservation=False,
    )
    db = _AsyncSessionAdapter(raw)
    worker, release_name = _worker_release(kind)
    release = AsyncMock(return_value=None)
    monkeypatch.setattr(worker, release_name, release)

    try:
        assert await _reconcile(kind, db) == 1
        raw.expire_all()
        saved_job = raw.get(type(job), job.job_id)
        assert saved_job is not None
        _assert_cleared(kind, saved_job)
        release.assert_not_awaited()
    finally:
        raw.close()
        engine.dispose()


@pytest.mark.parametrize("kind", ["generation", "storyboard"])
async def test_cleanup_rollback_failure_does_not_poison_next_job(
    monkeypatch,
    tmp_path,
    kind: JobKind,
) -> None:
    engine, raw, seed = _make_db(tmp_path / f"{kind}-isolation.db")
    failed_job, failed_reservation = _add_debt(
        raw,
        seed,
        kind=kind,
        suffix="a-failed",
        attempt=1,
        include_payload_reservation=False,
    )
    recovered_job, recovered_reservation = _add_debt(
        raw,
        seed,
        kind=kind,
        suffix="b-recovered",
        attempt=2,
        include_payload_reservation=False,
    )
    assert failed_reservation is not None and recovered_reservation is not None
    db = _AsyncSessionAdapter(raw)
    sessions = _FreshSessionFactory(engine, first_rollback_fails=True)
    worker, release_name = _worker_release(kind)

    async def release_with_first_failure(
        task_db,
        reservation_id: str,
        *args,
        **kwargs,
    ) -> None:
        if reservation_id == failed_reservation.id:
            task_db.poisoned = True
            raise RuntimeError("release backend failed")
        await authoritative_release(
            task_db,
            reservation_id,
            *args,
            **kwargs,
        )

    monkeypatch.setattr(worker, release_name, release_with_first_failure)

    try:
        assert await _reconcile(kind, db, session_factory=sessions) == 1
        raw.expire_all()
        failed = raw.get(type(failed_job), failed_job.job_id)
        recovered = raw.get(type(recovered_job), recovered_job.job_id)
        first_reservation = raw.get(CreditReservation, failed_reservation.id)
        second_reservation = raw.get(CreditReservation, recovered_reservation.id)
        account = raw.get(CreditAccount, seed.account_id)
        assert all(
            item is not None
            for item in (failed, recovered, first_reservation, second_reservation, account)
        )
        _assert_pending(kind, failed)
        _assert_cleared(kind, recovered)
        assert first_reservation.status == "reserved"
        assert second_reservation.status == "released"
        assert account.frozen_points == failed_reservation.reserved_points
        assert len(sessions.adapters) == 2
        assert sessions.adapters[0].rollback_count == 1
        assert sessions.adapters[1].commit_count == 1
    finally:
        raw.close()
        engine.dispose()


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 财务补偿锁测试",
)
@pytest.mark.parametrize("kind", ["generation", "storyboard"])
async def test_postgres_cleanup_skips_locked_job_and_recovers_exactly_once(
    monkeypatch,
    kind: JobKind,
) -> None:
    database_url = os.environ["MTV_TEST_POSTGRES_URL"]
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace(
            "postgresql://", "postgresql+asyncpg://", 1
        )
    suffix = secrets.token_hex(6)
    schema_name = f"mtv_financial_cleanup_{kind}_{suffix}"
    admin_engine = create_async_engine(database_url)
    engine = None
    seed = _Seed(
        user_id=f"cleanup-user-{suffix}",
        script_id=f"cleanup-script-{suffix}",
        episode_id=f"cleanup-episode-{suffix}",
        material_id=f"cleanup-material-{suffix}",
        subject_id=f"cleanup-subject-{suffix}",
        account_id=f"cleanup-account-{suffix}",
    )
    job_ids = [f"{kind}-a-{suffix}", f"{kind}-b-{suffix}"]
    reservation_ids = [f"cleanup-r-a-{suffix}", f"cleanup-r-b-{suffix}"]
    job_model = GenerationJob if kind == "generation" else StoryboardJob
    worker, release_name = _worker_release(kind)
    release_calls: list[str] = []

    async def tracked_release(db, reservation_id: str, *args, **kwargs) -> None:
        release_calls.append(reservation_id)
        await authoritative_release(db, reservation_id, *args, **kwargs)

    monkeypatch.setattr(worker, release_name, tracked_release)

    try:
        async with admin_engine.begin() as schema_db:
            await schema_db.execute(CreateSchema(schema_name))
        engine = create_async_engine(
            database_url,
            connect_args={"server_settings": {"search_path": schema_name}},
        )
        async with engine.begin() as schema_db:
            assert (
                await schema_db.scalar(select(func.current_schema()))
            ) == schema_name
            await schema_db.run_sync(Base.metadata.create_all)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)

        async with session_factory() as db:
            db.add_all(
                [
                    User(
                        user_id=seed.user_id,
                        username=seed.user_id,
                        password_hash="hash",
                        enabled=True,
                        account_type="personal",
                    ),
                    Script(
                        id=seed.script_id,
                        title="PostgreSQL 财务补偿",
                        scope="personal",
                        owner_user_id=seed.user_id,
                        created_by_user_id=seed.user_id,
                    ),
                    CreditAccount(
                        id=seed.account_id,
                        owner_type="user",
                        owner_id=seed.user_id,
                        balance_points=1000,
                        frozen_points=20,
                        status="active",
                    ),
                ]
            )
            await db.flush()
            db.add_all(
                [
                    Episode(
                        episode_id=seed.episode_id,
                        script_id=seed.script_id,
                        episode_no=1,
                        title="第一集",
                    ),
                    Materials(
                        id=seed.material_id,
                        script_id=seed.script_id,
                        name="画布",
                    ),
                    Subject(
                        id=seed.subject_id,
                        script_id=seed.script_id,
                        type="character",
                        name="主角",
                        episode_nos=[1],
                    ),
                ]
            )
            await db.flush()
            for index, (job_id, reservation_id) in enumerate(
                zip(job_ids, reservation_ids, strict=True),
                1,
            ):
                db.add(
                    CreditReservation(
                        id=reservation_id,
                        account_id=seed.account_id,
                        source_type=(
                            "generation_job" if kind == "generation" else "storyboard"
                        ),
                        source_id=job_id,
                        reserved_points=10,
                        status="reserved",
                    )
                )
                if kind == "generation":
                    db.add(
                        GenerationJob(
                            job_id=job_id,
                            material_id=seed.material_id,
                            script_id=seed.script_id,
                            subject_id=seed.subject_id,
                            owner_user_id=seed.user_id,
                            idempotency_key=f"idem-{job_id}",
                            request_hash=f"hash-{job_id}",
                            input_version=1,
                            attempt=index,
                            episode_id=seed.episode_id,
                            job_type="generate_images",
                            status="failed",
                            error_type="financial_cleanup",
                            retryable=True,
                            input_json={},
                            metrics={},
                        )
                    )
                else:
                    db.add(
                        StoryboardJob(
                            job_id=job_id,
                            script_id=seed.script_id,
                            episode_id=seed.episode_id,
                            triggered_by_user_id=seed.user_id,
                            owner_user_id=seed.user_id,
                            episode_version=1,
                            input_version=1,
                            attempt=index,
                            idempotency_key=f"idem-{job_id}",
                            request_hash=f"hash-{job_id}",
                            status="failed",
                            error_type="internal_error",
                            retryable=True,
                            financial_cleanup_status="pending",
                            financial_cleanup_error="release_failed",
                            detail={},
                        )
                    )
            await db.commit()

        async with session_factory() as blocker:
            await blocker.execute(
                select(job_model)
                .where(job_model.job_id == job_ids[0])
                .with_for_update()
            )
            async with session_factory() as scan_db:
                assert (
                    await _reconcile(
                        kind,
                        scan_db,
                        session_factory=session_factory,
                    )
                    == 1
                )

            async with session_factory() as verify:
                first = await verify.get(job_model, job_ids[0])
                second = await verify.get(job_model, job_ids[1])
                assert first is not None and second is not None
                _assert_pending(kind, first)
                _assert_cleared(kind, second)
            await blocker.rollback()

        first_scan_released = asyncio.Event()
        second_scan_released = asyncio.Event()

        async def run_contender(*, first: bool) -> int:
            async with session_factory() as scan_db:
                barrier_db = _PostScanBarrierSession(
                    scan_db,
                    before_scan=None if first else first_scan_released,
                    scan_released=(first_scan_released if first else second_scan_released),
                    proceed=second_scan_released,
                )
                return await _reconcile(
                    kind,
                    barrier_db,
                    session_factory=session_factory,
                )

        async with asyncio.timeout(10):
            results = await asyncio.gather(
                run_contender(first=True),
                run_contender(first=False),
            )
        assert sorted(results) == [0, 1]
        assert Counter(release_calls) == Counter(reservation_ids)

        async with session_factory() as verify:
            jobs = list(
                (
                    await verify.scalars(
                        select(job_model).where(job_model.job_id.in_(job_ids))
                    )
                ).all()
            )
            reservations = list(
                (
                    await verify.scalars(
                        select(CreditReservation).where(
                            CreditReservation.id.in_(reservation_ids)
                        )
                    )
                ).all()
            )
            logs = list(
                (
                    await verify.scalars(
                        select(CreditLog).where(
                            CreditLog.reservation_id.in_(reservation_ids),
                            CreditLog.log_type == "release",
                        )
                    )
                ).all()
            )
            account = await verify.get(CreditAccount, seed.account_id)
            assert account is not None and account.frozen_points == 0
            assert len(jobs) == 2
            for job in jobs:
                _assert_cleared(kind, job)
            assert {item.status for item in reservations} == {"released"}
            assert len(logs) == 2
            assert {item.reservation_id for item in logs} == set(reservation_ids)
            assert {item.actor_user_id for item in logs} == {seed.user_id}
    finally:
        try:
            if engine is not None:
                await engine.dispose()
        finally:
            try:
                async with admin_engine.begin() as schema_db:
                    await schema_db.execute(
                        DropSchema(schema_name, cascade=True, if_exists=True)
                    )
            finally:
                await admin_engine.dispose()
