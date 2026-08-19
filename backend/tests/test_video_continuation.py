"""F14 续帧依赖状态机专项测试。"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from app.crud import video as video_crud
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.shot_group import ShotGroup
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.video_generation_job import VideoGenerationJob
from app.services.reference_aggregator import AggregatedReferences
from app.services.runtime_models import RuntimeVideoModel
from tests.test_video_generation import _AsyncSessionAdapter, _make_db, _seed


def _add_group(
    raw,
    seeded,
    *,
    shot_group_id: str,
    group_index: int,
    is_continuation: bool,
) -> ShotGroup:
    group = ShotGroup(
        shot_group_id=shot_group_id,
        script_id=seeded.script.id,
        episode_id=seeded.episode.episode_id,
        scene_no=1,
        group_index=group_index,
        group_no=f"G{group_index}",
        prompt=f"镜头组 {group_index}",
        duration_seconds=6,
        materials={},
        audio_elements={},
        is_continuation=is_continuation,
    )
    raw.add(group)
    raw.commit()
    return group


def _add_waiting_job(
    raw,
    seeded,
    group: ShotGroup,
    *,
    job_id: str,
    reservation_id: str,
    predecessor_job_id: str | None,
    predecessor_shot_group_id: str,
) -> VideoGenerationJob:
    reservation = CreditReservation(
        id=reservation_id,
        account_id="account-1",
        source_type="generation_job",
        source_id=job_id,
        reserved_points=210,
        status="reserved",
    )
    account = raw.get(CreditAccount, "account-1")
    account.frozen_points += reservation.reserved_points
    job = VideoGenerationJob(
        job_id=job_id,
        script_id=seeded.script.id,
        episode_id=seeded.episode.episode_id,
        shot_group_id=group.shot_group_id,
        owner_user_id=seeded.user.user_id,
        scene_no=group.scene_no,
        unit_id=group.unit_id,
        unit_runtime_key=group.unit_runtime_key,
        provider_key="modelsai",
        job_type="all_reference",
        idempotency_key=f"{job_id}-key",
        request_hash="h",
        input_version=seeded.episode.episode_version,
        attempt=1,
        reservation_id=reservation_id,
        predecessor_job_id=predecessor_job_id,
        status="waiting_dependency",
        input_json={
            "is_continuation": True,
            "reservation_id": reservation_id,
            "continuation_dependency": {
                "state": "waiting",
                "predecessor_shot_group_id": predecessor_shot_group_id,
                "predecessor_job_id": predecessor_job_id,
            },
        },
    )
    raw.add_all([reservation, job])
    raw.commit()
    return job


def test_continuation_uses_only_same_scene_direct_predecessor() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        direct = _add_group(
            raw,
            seeded,
            shot_group_id="sg-9",
            group_index=9,
            is_continuation=False,
        )
        direct.reference_frames = {}
        seeded.shot_group.is_continuation = True
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        from app.services.video_generation import (
            _resolve_direct_predecessor,
            _resolve_first_frame,
        )

        assert (await _resolve_direct_predecessor(db, seeded.shot_group)).shot_group_id == "sg-9"
        # G2 有尾帧，但续帧不得跳过无尾帧的直接前驱 G9。
        assert await _resolve_first_frame(db, seeded.shot_group) is None

    asyncio.run(scenario())


def test_waiting_job_cannot_claim_lease_and_has_no_outbox() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        predecessor = _add_group(
            raw,
            seeded,
            shot_group_id="sg-9",
            group_index=9,
            is_continuation=False,
        )
        seeded.shot_group.is_continuation = True
        raw.commit()
        job = _add_waiting_job(
            raw,
            seeded,
            seeded.shot_group,
            job_id="job-waiting",
            reservation_id="reservation-waiting",
            predecessor_job_id=None,
            predecessor_shot_group_id=predecessor.shot_group_id,
        )
        db = _AsyncSessionAdapter(raw)

        claim = await video_crud.claim_video_job(db, job.job_id, owner="worker")

        assert claim is None
        assert job.lease_token is None
        assert raw.scalar(
            select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == job.job_id)
        ) is None

    asyncio.run(scenario())


def test_out_of_order_child_binds_when_predecessor_job_is_created() -> None:
    async def scenario() -> None:
        _, raw = _make_db()
        seeded = _seed(raw)
        predecessor_group = _add_group(
            raw,
            seeded,
            shot_group_id="sg-9",
            group_index=9,
            is_continuation=False,
        )
        child = _add_waiting_job(
            raw,
            seeded,
            seeded.shot_group,
            job_id="job-child-first",
            reservation_id="reservation-child-first",
            predecessor_job_id=None,
            predecessor_shot_group_id=predecessor_group.shot_group_id,
        )
        db = _AsyncSessionAdapter(raw)
        predecessor = await video_crud.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=predecessor_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id=predecessor_group.unit_id,
            unit_runtime_key=predecessor_group.unit_runtime_key,
            provider_key="modelsai",
            job_type="all_reference",
            model="model",
            idempotency_key="predecessor-key",
            request_hash="h",
            input_version=seeded.episode.episode_version,
            attempt=1,
            reservation_id=None,
            input_json={},
        )

        bound = await video_crud.bind_waiting_dependents_to_predecessor_job(db, predecessor)
        await db.commit()

        assert [row.job_id for row in bound] == [child.job_id]
        assert child.predecessor_job_id == predecessor.job_id
        assert child.input_json["continuation_dependency"]["predecessor_job_id"] == predecessor.job_id

    asyncio.run(scenario())


def test_predecessor_success_wakes_child_once_with_frozen_real_tail(monkeypatch) -> None:
    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        predecessor_group = _add_group(
            raw,
            seeded,
            shot_group_id="sg-9",
            group_index=9,
            is_continuation=False,
        )
        predecessor = await video_crud.create_video_job(
            _AsyncSessionAdapter(raw),
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=predecessor_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id=predecessor_group.unit_id,
            unit_runtime_key=predecessor_group.unit_runtime_key,
            provider_key="modelsai",
            job_type="all_reference",
            model="model",
            idempotency_key="parent-key",
            request_hash="h",
            input_version=seeded.episode.episode_version,
            attempt=1,
            reservation_id=None,
            input_json={},
        )
        raw.commit()
        child = _add_waiting_job(
            raw,
            seeded,
            seeded.shot_group,
            job_id="job-child-wake",
            reservation_id="reservation-child-wake",
            predecessor_job_id=predecessor.job_id,
            predecessor_shot_group_id=predecessor_group.shot_group_id,
        )
        db = _AsyncSessionAdapter(raw)

        assert await worker._resolve_continuation_dependents(
            db,
            predecessor.job_id,
            last_frame_storage_key="generate_images/video/frames/real-tail.png",
            succeeded=True,
        ) == 1
        assert await worker._resolve_continuation_dependents(
            db,
            predecessor.job_id,
            last_frame_storage_key="generate_images/video/frames/other-tail.png",
            succeeded=True,
        ) == 0
        await db.commit()

        assert child.status == "starting"
        assert child.lease_token is None
        assert child.input_json["first_frame"] == "generate_images/video/frames/real-tail.png"
        assert raw.get(ShotGroup, child.shot_group_id).reference_frames["first_frame"] == (
            "generate_images/video/frames/real-tail.png"
        )
        outboxes = list(
            raw.scalars(
                select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == child.job_id)
            ).all()
        )
        assert len(outboxes) == 1
        assert child.dispatch_id == outboxes[0].id

    asyncio.run(scenario())


def test_reconciler_recovers_child_after_terminal_followup_transaction_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """前驱已成功，但后置 child 事务失败时，下一轮扫描恢复且双轮不重复。"""

    async def scenario() -> None:
        from app.services import task_dispatch
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        predecessor_group = _add_group(
            raw,
            seeded,
            shot_group_id="sg-reconcile-parent",
            group_index=9,
            is_continuation=False,
        )
        predecessor = await video_crud.create_video_job(
            _AsyncSessionAdapter(raw),
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=predecessor_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id=predecessor_group.unit_id,
            unit_runtime_key=predecessor_group.unit_runtime_key,
            provider_key="modelsai",
            job_type="all_reference",
            model="model",
            idempotency_key="reconcile-parent-key",
            request_hash="h",
            input_version=seeded.episode.episode_version,
            attempt=1,
            reservation_id=None,
            input_json={},
        )
        tail = "generate_images/video/frames/reconciled-real-tail.png"
        predecessor.status = "succeeded"
        predecessor.progress = 100
        predecessor.output_json = {"last_frame_storage_key": tail}
        raw.commit()
        child = _add_waiting_job(
            raw,
            seeded,
            seeded.shot_group,
            job_id="job-reconcile-child",
            reservation_id="reservation-reconcile-child",
            predecessor_job_id=predecessor.job_id,
            predecessor_shot_group_id=predecessor_group.shot_group_id,
        )
        db = _AsyncSessionAdapter(raw)
        real_enqueue = task_dispatch.enqueue_task
        enqueue_attempts = 0

        async def fail_followup_once(*args, **kwargs):
            nonlocal enqueue_attempts
            enqueue_attempts += 1
            if enqueue_attempts == 1:
                raise RuntimeError("injected_followup_transaction_failure")
            return await real_enqueue(*args, **kwargs)

        monkeypatch.setattr(task_dispatch, "enqueue_task", fail_followup_once)

        assert await worker._resolve_continuation_dependents_best_effort(
            db,
            predecessor.job_id,
            last_frame_storage_key=tail,
            succeeded=True,
        ) == 0
        raw.expire_all()
        rolled_back_child = raw.get(VideoGenerationJob, child.job_id)
        assert rolled_back_child is not None
        assert rolled_back_child.status == "waiting_dependency"
        assert rolled_back_child.lease_token is None
        assert raw.scalar(
            select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == child.job_id)
        ) is None

        # 真实周期入口先执行 waiting 补偿，再处理 starting/processing 租约恢复。
        assert await worker.reconcile_stale_video_jobs(db) == [child.job_id]
        # 第二轮不再扫描 starting，也不能新增同 attempt Outbox。
        assert await worker.reconcile_stale_video_jobs(db) == []

        raw.expire_all()
        recovered = raw.get(VideoGenerationJob, child.job_id)
        assert recovered is not None
        assert recovered.status == "starting"
        assert recovered.lease_owner is None
        assert recovered.lease_token is None
        assert recovered.lease_until is None
        assert recovered.input_json["first_frame"] == tail
        assert recovered.input_json["continuation_dependency"]["frame_source"] == "real_tail"
        outboxes = list(
            raw.scalars(
                select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == child.job_id)
            ).all()
        )
        assert len(outboxes) == 1
        assert recovered.dispatch_id == outboxes[0].id
        assert enqueue_attempts == 2

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("predecessor_status", "with_fallback", "expected_status"),
    [
        ("succeeded", True, "starting"),
        ("failed", False, "failed"),
        ("canceled", False, "failed"),
    ],
)
def test_reconciler_handles_terminal_predecessor_fallback_or_release_once(
    predecessor_status: str,
    with_fallback: bool,
    expected_status: str,
) -> None:
    """周期补偿覆盖成功无尾帧静态回退，以及失败/取消无回退的幂等释放。"""

    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        predecessor_group = _add_group(
            raw,
            seeded,
            shot_group_id=f"sg-reconcile-{predecessor_status}",
            group_index=9,
            is_continuation=False,
        )
        predecessor_group.reference_frames = {}
        seeded.shot_group.reference_frames = (
            {"reference_frame": "/frames/reconciler-static.png"}
            if with_fallback
            else {}
        )
        raw.commit()
        predecessor = await video_crud.create_video_job(
            _AsyncSessionAdapter(raw),
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=predecessor_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id=predecessor_group.unit_id,
            unit_runtime_key=predecessor_group.unit_runtime_key,
            provider_key="modelsai",
            job_type="all_reference",
            model="model",
            idempotency_key=f"reconcile-{predecessor_status}-parent",
            request_hash="h",
            input_version=seeded.episode.episode_version,
            attempt=1,
            reservation_id=None,
            input_json={},
        )
        predecessor.status = predecessor_status
        raw.commit()
        child = _add_waiting_job(
            raw,
            seeded,
            seeded.shot_group,
            job_id=f"job-reconcile-{predecessor_status}",
            reservation_id=f"reservation-reconcile-{predecessor_status}",
            predecessor_job_id=predecessor.job_id,
            predecessor_shot_group_id=predecessor_group.shot_group_id,
        )
        db = _AsyncSessionAdapter(raw)

        assert await worker.reconcile_waiting_video_dependencies(db) == [child.job_id]
        assert await worker.reconcile_waiting_video_dependencies(db) == []

        raw.expire_all()
        saved = raw.get(VideoGenerationJob, child.job_id)
        assert saved is not None
        assert saved.status == expected_status
        assert saved.lease_token is None
        if expected_status == "starting":
            assert saved.input_json["first_frame"] == "/frames/reconciler-static.png"
            assert saved.input_json["continuation_dependency"]["frame_source"] == (
                "static_fallback"
            )
            assert raw.get(CreditReservation, child.reservation_id).status == "reserved"
            assert raw.scalar(
                select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == child.job_id)
            ) is not None
        else:
            assert saved.error_type == "dependency_unavailable"
            assert raw.get(CreditReservation, child.reservation_id).status == "released"
            release_logs = list(
                raw.scalars(
                    select(CreditLog).where(
                        CreditLog.log_type == "release",
                        CreditLog.reservation_id == child.reservation_id,
                    )
                ).all()
            )
            assert len(release_logs) == 1
            assert raw.scalar(
                select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == child.job_id)
            ) is None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("terminal_name", "succeeded"),
    [("failed", False), ("succeeded_without_tail", True)],
)
def test_predecessor_terminal_without_real_tail_activates_static_fallback(
    monkeypatch: pytest.MonkeyPatch,
    terminal_name: str,
    succeeded: bool,
) -> None:
    """失败或成功但无真实尾帧时，冻结静态帧仍可安全激活 child。"""

    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        predecessor_group = _add_group(
            raw,
            seeded,
            shot_group_id=f"sg-{terminal_name}-parent",
            group_index=9,
            is_continuation=False,
        )
        seeded.shot_group.reference_frames = {
            "first_frame": None,
            "reference_frame": f"/frames/{terminal_name}-fallback.png",
        }
        raw.commit()
        predecessor = await video_crud.create_video_job(
            _AsyncSessionAdapter(raw),
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=predecessor_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id=predecessor_group.unit_id,
            unit_runtime_key=predecessor_group.unit_runtime_key,
            provider_key="modelsai",
            job_type="all_reference",
            model="model",
            idempotency_key=f"{terminal_name}-parent-key",
            request_hash="h",
            input_version=seeded.episode.episode_version,
            attempt=1,
            reservation_id=None,
            input_json={},
        )
        raw.commit()
        child = _add_waiting_job(
            raw,
            seeded,
            seeded.shot_group,
            job_id=f"job-{terminal_name}-fallback",
            reservation_id=f"reservation-{terminal_name}-fallback",
            predecessor_job_id=predecessor.job_id,
            predecessor_shot_group_id=predecessor_group.shot_group_id,
        )
        db = _AsyncSessionAdapter(raw)

        assert await worker._resolve_continuation_dependents(
            db,
            predecessor.job_id,
            last_frame_storage_key=None,
            succeeded=succeeded,
        ) == 1
        await db.commit()

        expected = f"/frames/{terminal_name}-fallback.png"
        assert child.status == "starting"
        assert child.lease_token is None
        assert child.input_json["first_frame"] == expected
        dependency = child.input_json["continuation_dependency"]
        assert dependency["frame_source"] == "static_fallback"
        assert dependency["predecessor_terminal_succeeded"] is succeeded
        assert raw.get(CreditReservation, child.reservation_id).status == "reserved"
        assert raw.scalar(
            select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == child.job_id)
        ) is not None

    asyncio.run(scenario())


def test_predecessor_failure_without_any_static_fallback_is_dependency_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """真实尾帧与三种静态来源都为空时，才失败并释放冻结积分。"""

    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        predecessor_group = _add_group(
            raw,
            seeded,
            shot_group_id="sg-no-fallback-parent",
            group_index=9,
            is_continuation=False,
        )
        predecessor_group.reference_frames = {}
        seeded.shot_group.reference_frames = {}
        raw.commit()
        predecessor = await video_crud.create_video_job(
            _AsyncSessionAdapter(raw),
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=predecessor_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id=predecessor_group.unit_id,
            unit_runtime_key=predecessor_group.unit_runtime_key,
            provider_key="modelsai",
            job_type="all_reference",
            model="model",
            idempotency_key="no-fallback-parent-key",
            request_hash="h",
            input_version=seeded.episode.episode_version,
            attempt=1,
            reservation_id=None,
            input_json={},
        )
        raw.commit()
        child = _add_waiting_job(
            raw,
            seeded,
            seeded.shot_group,
            job_id="job-no-fallback",
            reservation_id="reservation-no-fallback",
            predecessor_job_id=predecessor.job_id,
            predecessor_shot_group_id=predecessor_group.shot_group_id,
        )
        db = _AsyncSessionAdapter(raw)

        assert await worker._resolve_continuation_dependents(
            db,
            predecessor.job_id,
            last_frame_storage_key=None,
            succeeded=False,
        ) == 1
        await db.commit()

        assert child.status == "failed"
        assert child.error_type == "dependency_unavailable"
        assert raw.get(CreditReservation, child.reservation_id).status == "released"
        assert raw.scalar(
            select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == child.job_id)
        ) is None

    asyncio.run(scenario())


def test_predecessor_failure_cascades_and_releases_each_reservation_once(monkeypatch) -> None:
    async def scenario() -> None:
        from app.workers import video_generation as worker

        _, raw = _make_db()
        seeded = _seed(raw)
        group_9 = _add_group(
            raw,
            seeded,
            shot_group_id="sg-9",
            group_index=9,
            is_continuation=False,
        )
        group_11 = raw.get(ShotGroup, "sg-11")
        group_11.is_continuation = True
        raw.commit()
        parent = await video_crud.create_video_job(
            _AsyncSessionAdapter(raw),
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=group_9.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id=group_9.unit_id,
            unit_runtime_key=group_9.unit_runtime_key,
            provider_key="modelsai",
            job_type="all_reference",
            model="model",
            idempotency_key="cascade-parent",
            request_hash="h",
            input_version=seeded.episode.episode_version,
            attempt=1,
            reservation_id=None,
            input_json={},
        )
        raw.commit()
        child = _add_waiting_job(
            raw,
            seeded,
            seeded.shot_group,
            job_id="cascade-child",
            reservation_id="cascade-child-reservation",
            predecessor_job_id=parent.job_id,
            predecessor_shot_group_id=group_9.shot_group_id,
        )
        grandchild = _add_waiting_job(
            raw,
            seeded,
            group_11,
            job_id="cascade-grandchild",
            reservation_id="cascade-grandchild-reservation",
            predecessor_job_id=child.job_id,
            predecessor_shot_group_id=seeded.shot_group.shot_group_id,
        )
        db = _AsyncSessionAdapter(raw)

        assert await worker._resolve_continuation_dependents(
            db,
            parent.job_id,
            last_frame_storage_key=None,
            succeeded=False,
        ) == 1
        await db.commit()
        # 重复终态通知不得再释放或新增日志。
        assert await worker._resolve_continuation_dependents(
            db,
            parent.job_id,
            last_frame_storage_key=None,
            succeeded=False,
        ) == 0
        await db.commit()

        assert child.status == grandchild.status == "failed"
        assert child.error_type == grandchild.error_type == "dependency_unavailable"
        assert raw.get(CreditReservation, child.reservation_id).status == "released"
        assert raw.get(CreditReservation, grandchild.reservation_id).status == "released"
        release_logs = list(
            raw.scalars(
                select(CreditLog).where(
                    CreditLog.log_type == "release",
                    CreditLog.reservation_id.in_([child.reservation_id, grandchild.reservation_id]),
                )
            ).all()
        )
        assert len(release_logs) == 2

    asyncio.run(scenario())


def test_create_continuation_does_not_wait_or_pass_predecessor_tail(monkeypatch) -> None:
    async def scenario() -> None:
        from app.services import video_generation as service

        _, raw = _make_db()
        seeded = _seed(raw)
        predecessor = _add_group(
            raw,
            seeded,
            shot_group_id="sg-9",
            group_index=9,
            is_continuation=False,
        )
        predecessor.reference_frames = {"last_frame": "frames/predecessor-tail.png"}
        seeded.shot_group.is_continuation = True
        seeded.shot_group.reference_image_audit = {}
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(service, "_credits_reserve", None)
        monkeypatch.setattr(
            service,
            "aggregate_references",
            AsyncMock(return_value=AggregatedReferences(reference_images=[{"storage_key": "input.png"}])),
        )
        monkeypatch.setattr(
            service,
            "resolve_video_model",
            AsyncMock(
                return_value=RuntimeVideoModel(
                    provider="bailian",
                    model="wan2.6-i2v",
                    reference_image_audit_required=False,
                    task_provider_key="bailian",
                )
            ),
        )
        enqueue = AsyncMock(return_value=SimpleNamespace(id="dispatch-without-tail"))
        monkeypatch.setattr(service, "enqueue_task", enqueue)

        job = await service.create_video_job(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=seeded.shot_group.shot_group_id,
            user_id=seeded.user.user_id,
            idempotency_key="continuation-waiting-create",
        )

        assert job.status == "starting"
        assert job.predecessor_job_id is None
        assert job.lease_token is None
        assert job.input_json["first_frame"] is None
        assert job.input_json["awaits_predecessor_frame"] is False
        assert job.input_json["continuation_dependency"] is None
        enqueue.assert_awaited_once()

    asyncio.run(scenario())


def test_worker_does_not_extract_tail_frame_without_test_hook(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import VideoGenerationResult
        from app.workers import video_generation as worker
        from tests.test_video_generation import _add_worker_job

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-no-auto-tail-frame",
            reservation_id="reservation-no-auto-tail-frame",
        )
        db = _AsyncSessionAdapter(raw)
        extract_tail = AsyncMock(side_effect=AssertionError("不得自动抽取尾帧"))
        monkeypatch.setattr(worker, "_update_last_frame", extract_tail)
        monkeypatch.setattr(worker, "_credits_settle", AsyncMock())
        provider = SimpleNamespace(
            generate=AsyncMock(
                return_value=VideoGenerationResult(
                    video_storage_key="generate_images/video/no-auto-tail.mp4",
                    provider_job_id="provider-no-auto-tail",
                )
            )
        )

        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=provider,
        )

        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved.status == "succeeded"
        assert "last_frame_storage_key" not in (saved.output_json or {})
        extract_tail.assert_not_awaited()

    asyncio.run(scenario())


def test_batch_orders_direct_predecessor_first_and_preserves_original_idempotency_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api
        from app.schemas.video import VideoJobBatchCreateIn

        _, raw = _make_db()
        seeded = _seed(raw)
        predecessor = _add_group(
            raw,
            seeded,
            shot_group_id="sg-9",
            group_index=9,
            is_continuation=False,
        )
        seeded.shot_group.is_continuation = True
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(api, "_load_and_check_script", AsyncMock(return_value=seeded.script))

        created: list[tuple[str, str]] = []

        async def fake_create(_db, **kwargs):
            created.append((kwargs["shot_group_id"], kwargs["idempotency_key"]))
            group = raw.get(ShotGroup, kwargs["shot_group_id"])
            return SimpleNamespace(
                job_id=f"job-{group.shot_group_id}",
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                shot_group_id=group.shot_group_id,
                owner_user_id=seeded.user.user_id,
                idempotency_key=kwargs["idempotency_key"],
                request_hash="hash",
                input_version=1,
                attempt=1,
                reservation_id=None,
                dispatch_id=None,
                predecessor_job_id=None,
                scene_no=1,
                unit_id=group.unit_id,
                unit_runtime_key=group.unit_runtime_key,
                provider_key="bailian",
                job_type="all_reference",
                status="starting",
                progress=0,
                progress_stage=None,
                progress_message=None,
                error_type=None,
                error_message=None,
                retryable=False,
                input_json={},
                output_json=None,
                metrics=None,
                created_at=None,
                updated_at=None,
            )

        monkeypatch.setattr("app.services.video_generation.create_video_job", fake_create)

        result = await api.batch_create_video_generation_jobs(
            VideoJobBatchCreateIn(
                script_id=seeded.script.id,
                episode_id=seeded.episode.episode_id,
                # 故意把续帧 child 放在直接前驱前面。
                shot_group_ids=[seeded.shot_group.shot_group_id, predecessor.shot_group_id],
            ),
            db=db,
            user=seeded.user,
            idempotency_key="batch-continuation",
        )

        assert result.failed == []
        assert created == [
            # G9 被提前，但仍携带原请求下标 1 派生的 key。
            (predecessor.shot_group_id, "batch-continuation:1"),
            (seeded.shot_group.shot_group_id, "batch-continuation:0"),
        ]

    asyncio.run(scenario())


def test_batch_keeps_non_continuation_request_order() -> None:
    async def scenario() -> None:
        from app.api.v1 import video_generation as api

        _, raw = _make_db()
        seeded = _seed(raw)
        db = _AsyncSessionAdapter(raw)

        ordered = await api._order_batch_items_by_continuation_dependency(
            db,
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_ids=["sg-10", "sg-2"],
            idempotency_keys=["key:0", "key:1"],
        )

        assert ordered == [("sg-10", "key:0"), ("sg-2", "key:1")]

    asyncio.run(scenario())


def test_waiting_dependency_is_counted_as_active_for_lifecycle_delete() -> None:
    from app.crud.script import _VIDEO_ACTIVE_STATUSES
    from app.services.shot_group_delete import _ACTIVE_VIDEO_STATUSES

    assert "waiting_dependency" in _VIDEO_ACTIVE_STATUSES
    assert "waiting_dependency" in _ACTIVE_VIDEO_STATUSES


def test_video_job_output_exposes_predecessor_job_id() -> None:
    from app.schemas.video import VideoJobOut

    payload = VideoJobOut.model_validate(
        SimpleNamespace(
            job_id="child",
            script_id="script",
            episode_id="episode",
            shot_group_id="group",
            owner_user_id="user",
            idempotency_key="key",
            request_hash="hash",
            input_version=1,
            attempt=1,
            reservation_id="reservation",
            dispatch_id=None,
            predecessor_job_id="parent",
            scene_no=1,
            unit_id="group:group",
            unit_runtime_key="runtime",
            provider_key="bailian",
            job_type="all_reference",
            status="waiting_dependency",
            progress=0,
            progress_stage="waiting_dependency",
            progress_message=None,
            error_type=None,
            error_message=None,
            retryable=False,
            input_json={},
            output_json=None,
            metrics=None,
            created_at=None,
            updated_at=None,
        )
    )

    assert payload.predecessor_job_id == "parent"


def test_successful_worker_freezes_real_tail_on_same_attempt_output(monkeypatch) -> None:
    async def scenario() -> None:
        from app.providers.video_gen import VideoGenerationResult
        from app.workers import video_generation as worker
        from tests.test_video_generation import _add_worker_job

        _, raw = _make_db()
        seeded = _seed(raw)
        job = _add_worker_job(
            raw,
            seeded,
            job_id="job-tail-provenance",
            reservation_id="reservation-tail-provenance",
        )
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(worker, "_credits_settle", AsyncMock())
        tail = "generate_images/video/frames/attempt-tail.png"
        provider = SimpleNamespace(
            generate=AsyncMock(
                return_value=VideoGenerationResult(
                    video_storage_key="generate_images/video/attempt.mp4",
                    provider_job_id="provider-tail",
                )
            )
        )

        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="worker",
            provider=provider,
            extract_last_frame=AsyncMock(return_value=tail),
        )

        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved.status == "succeeded"
        assert saved.output_json["last_frame_storage_key"] == tail

    asyncio.run(scenario())


def test_cancel_resolves_waiting_child_in_independent_followup(monkeypatch) -> None:
    """本地取消提交后独立清理 child，不增加原会话提交次数。"""

    async def scenario() -> None:
        from app.services import video_generation as service

        engine, raw = _make_db()
        seeded = _seed(raw)
        predecessor_group = _add_group(
            raw,
            seeded,
            shot_group_id="sg-cancel-parent",
            group_index=9,
            is_continuation=False,
        )
        predecessor_group.reference_frames = {
            "last_frame": "/frames/canceled-parent-static-tail.png"
        }
        predecessor = VideoGenerationJob(
            job_id="job-cancel-parent",
            script_id=seeded.script.id,
            episode_id=seeded.episode.episode_id,
            shot_group_id=predecessor_group.shot_group_id,
            owner_user_id=seeded.user.user_id,
            scene_no=1,
            unit_id=predecessor_group.unit_id,
            unit_runtime_key=predecessor_group.unit_runtime_key,
            provider_key="bailian",
            job_type="all_reference",
            idempotency_key="cancel-parent",
            request_hash="hash",
            input_version=seeded.episode.episode_version,
            attempt=1,
            status="starting",
            input_json={},
        )
        raw.add(predecessor)
        raw.commit()
        child = _add_waiting_job(
            raw,
            seeded,
            seeded.shot_group,
            job_id="job-cancel-child",
            reservation_id="reservation-cancel-child",
            predecessor_job_id=predecessor.job_id,
            predecessor_shot_group_id=predecessor_group.shot_group_id,
        )

        @asynccontextmanager
        async def independent_session():
            from sqlalchemy.orm import Session

            independent_raw = Session(engine, expire_on_commit=False)
            try:
                yield _AsyncSessionAdapter(independent_raw)
            finally:
                independent_raw.close()

        monkeypatch.setattr(
            "app.core.database.async_session_factory",
            independent_session,
        )
        monkeypatch.setattr(service, "_credits_reserve", None)
        db = _AsyncSessionAdapter(raw)

        result = await service.cancel_video_job(
            db,
            script_id=seeded.script.id,
            job_id=predecessor.job_id,
            actor_user_id=seeded.user.user_id,
        )

        assert result.status == "canceled"
        # 取消主事务仅提交一次；子任务由独立事务清理。
        assert db.commit_count == 1
        raw.expire_all()
        saved_child = raw.get(VideoGenerationJob, child.job_id)
        assert saved_child is not None
        assert saved_child.status == "starting"
        assert saved_child.error_type is None
        assert saved_child.input_json["first_frame"] == (
            "/frames/canceled-parent-static-tail.png"
        )
        assert saved_child.input_json["continuation_dependency"]["frame_source"] == (
            "static_fallback"
        )
        assert raw.get(CreditReservation, child.reservation_id).status == "reserved"
        assert raw.scalar(
            select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == child.job_id)
        ) is not None

    asyncio.run(scenario())
