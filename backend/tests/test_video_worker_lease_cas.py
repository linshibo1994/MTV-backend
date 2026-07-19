"""视频 worker 租约 CAS 回归测试。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

from sqlalchemy.orm import Session

from app.models.credit_reservation import CreditReservation
from app.models.video_generation_job import VideoGenerationJob
from tests.test_video_generation import (
    _add_worker_job,
    _AsyncSessionAdapter,
    _make_db,
    _seed,
)


class _ReclaimOnRollbackSession(_AsyncSessionAdapter):
    """首次 rollback 后模拟另一个 worker 立即重领过期租约。"""

    def __init__(
        self,
        session: Session,
        reclaim: Callable[[], Awaitable[None]],
    ) -> None:
        super().__init__(session)
        self._reclaim = reclaim
        self._reclaimed = False

    async def rollback(self) -> None:
        await super().rollback()
        if self._reclaimed:
            return
        self._reclaimed = True
        await self._reclaim()


def test_claimed_setup_failure_after_reclaim_cannot_touch_new_lease(monkeypatch) -> None:
    async def scenario() -> None:
        from app.crud.video import claim_video_job
        from app.workers import video_generation as worker

        engine, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-setup-race",
            reservation_id="reservation-setup-race",
        )
        db_a = _AsyncSessionAdapter(Session(engine, expire_on_commit=False))
        db_b = _AsyncSessionAdapter(Session(engine, expire_on_commit=False))
        release = AsyncMock()
        claim_b = None

        async def reclaim_then_fail(_db, job_id: str):
            nonlocal claim_b
            claim_b = await claim_video_job(
                db_b,
                job_id,
                owner="worker-b",
                now=datetime.now(UTC) + timedelta(seconds=worker._LEASE_SECONDS + 5),
                lease_seconds=worker._LEASE_SECONDS,
            )
            assert claim_b is not None
            await db_b.commit()
            raise RuntimeError("初始化阶段读取任务失败")

        monkeypatch.setattr(worker, "_get_video_job", reclaim_then_fail)
        monkeypatch.setattr(worker, "_credits_release", release)

        await worker.run_video_generation_once(db_a, job.job_id, owner="worker-a")

        raw.expire_all()
        refreshed = raw.get(VideoGenerationJob, job.job_id)
        reservation = raw.get(CreditReservation, "reservation-setup-race")
        assert refreshed is not None
        assert refreshed.status == "processing"
        assert refreshed.lease_owner == "worker-b"
        assert refreshed.lease_token == claim_b.lease_token
        assert reservation is not None
        assert reservation.status == "reserved"
        release.assert_not_awaited()

    asyncio.run(scenario())


def test_claimed_setup_failure_releases_reservation_with_current_token(monkeypatch) -> None:
    async def scenario() -> None:
        from app.workers import video_generation as worker

        engine, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-setup-current",
            reservation_id="reservation-setup-current",
        )
        db = _AsyncSessionAdapter(Session(engine, expire_on_commit=False))

        async def fail_after_claim(_db, _job_id: str):
            raise RuntimeError("领取后读取任务失败")

        monkeypatch.setattr(worker, "_get_video_job", fail_after_claim)

        await worker.run_video_generation_once(db, job.job_id, owner="worker-a")

        raw.expire_all()
        refreshed = raw.get(VideoGenerationJob, job.job_id)
        reservation = raw.get(CreditReservation, "reservation-setup-current")
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.lease_token is None
        assert reservation is not None
        assert reservation.status == "released"

    asyncio.run(scenario())


def test_release_failure_rechecks_token_after_rollback_before_marking_failed(monkeypatch) -> None:
    async def scenario() -> None:
        from app.crud.video import claim_video_job
        from app.workers import video_generation as worker

        engine, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-release-race",
            reservation_id="reservation-release-race",
        )
        db_b = _AsyncSessionAdapter(Session(engine, expire_on_commit=False))
        claim_b = None

        async def reclaim() -> None:
            nonlocal claim_b
            claim_b = await claim_video_job(
                db_b,
                job.job_id,
                owner="worker-b",
                now=datetime.now(UTC) + timedelta(seconds=worker._LEASE_SECONDS + 5),
                lease_seconds=worker._LEASE_SECONDS,
            )
            assert claim_b is not None
            await db_b.commit()

        db_a = _ReclaimOnRollbackSession(
            Session(engine, expire_on_commit=False),
            reclaim,
        )
        claim_a = await claim_video_job(
            db_a,
            job.job_id,
            owner="worker-a",
            lease_seconds=worker._LEASE_SECONDS,
        )
        assert claim_a is not None
        await db_a.commit()
        release = AsyncMock(side_effect=RuntimeError("积分服务写入失败"))
        monkeypatch.setattr(worker, "_credits_release", release)

        await worker._fail_video_with_release(
            db_a,
            job.job_id,
            claim_a.lease_token,
            "reservation-release-race",
            actor_user_id="user-1",
            error_type="worker_internal_error",
            retryable=False,
        )

        raw.expire_all()
        refreshed = raw.get(VideoGenerationJob, job.job_id)
        reservation = raw.get(CreditReservation, "reservation-release-race")
        assert claim_b is not None
        assert refreshed is not None
        assert refreshed.status == "processing"
        assert refreshed.lease_owner == "worker-b"
        assert refreshed.lease_token == claim_b.lease_token
        assert reservation is not None
        assert reservation.status == "reserved"
        release.assert_awaited_once()

    asyncio.run(scenario())


def test_task_wrapper_without_claim_token_does_not_run_financial_cleanup(monkeypatch) -> None:
    from app.workers import video_generation as worker

    async def failing_run(_job_id: str):
        raise RuntimeError("数据库连接在领取任务前失败")

    cleanup = AsyncMock()
    monkeypatch.setattr(worker, "_run_video_generation_async", failing_run)
    monkeypatch.setattr(worker, "_mark_failed_and_release", cleanup, raising=False)

    result = worker.run_video_generation.run("job-before-claim-failure")

    assert result == {
        "job_id": "job-before-claim-failure",
        "status": "failed",
        "error": "worker_internal_error",
    }
    cleanup.assert_not_awaited()
