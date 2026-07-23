"""事务 outbox 的入队和可靠发布。"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, exists, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.ids import new_id
from app.core.job_state import LeaseState, PipelineFailure, claim_lease
from app.models.credit_reservation import CreditReservation
from app.models.pipeline_job import PipelineJob
from app.models.pipeline_stage import PipelineStage
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.services.credits import release as credits_release

Publisher = Callable[[str, dict], object | Awaitable[object]]
_ACTIVE_DISPATCH_STATUSES = ("pending", "retry", "running")


def backoff_seconds(publish_attempts: int) -> int:
    """投递失败退避，封顶避免无限等待。"""
    return min(2 ** max(publish_attempts - 1, 0), 300)


def _is_expired(value: datetime | None, now: datetime) -> bool:
    """兼容 SQLite 丢失时区信息的测试路径。"""
    if value is None:
        return False
    comparable_now = now if value.tzinfo is not None else now.replace(tzinfo=None)
    return value <= comparable_now


async def enqueue_task(
    db: AsyncSession,
    *,
    task_type: str,
    job_id: str,
    attempt: int,
    payload: dict,
    available_at: datetime | None = None,
) -> TaskDispatchOutbox:
    """和业务写入处于同一事务；可指定最早发布时间，唯一键使重复触发幂等。"""
    existing = await db.execute(
        select(TaskDispatchOutbox).where(
            TaskDispatchOutbox.task_type == task_type,
            TaskDispatchOutbox.job_id == job_id,
            TaskDispatchOutbox.attempt == attempt,
        )
    )
    row = existing.scalar_one_or_none()
    if row is not None:
        return row
    dispatch_id = new_id()
    stable_payload = {
        **payload,
        "dispatch_id": dispatch_id,
        "run_id": job_id,
    }
    row = TaskDispatchOutbox(
        id=dispatch_id,
        task_type=task_type,
        job_id=job_id,
        attempt=attempt,
        payload=stable_payload,
        status="pending",
        **({"available_at": available_at} if available_at is not None else {}),
    )
    try:
        # savepoint 使并发唯一键冲突不会回滚调用方的业务事务。
        async with db.begin_nested():
            db.add(row)
            await db.flush()
    except IntegrityError:
        result = await db.execute(
            select(TaskDispatchOutbox).where(
                TaskDispatchOutbox.task_type == task_type,
                TaskDispatchOutbox.job_id == job_id,
                TaskDispatchOutbox.attempt == attempt,
            )
        )
        row = result.scalar_one()
    return row


async def cancel_outbox_task(
    db: AsyncSession,
    *,
    task_type: str,
    job_id: str,
    attempt: int | None = None,
) -> int:
    """在调用方事务内取消指定任务的活动投递，不自行提交。"""

    criteria = [
        TaskDispatchOutbox.task_type == task_type,
        TaskDispatchOutbox.job_id == job_id,
        TaskDispatchOutbox.status.in_(_ACTIVE_DISPATCH_STATUSES),
    ]
    if attempt is not None:
        criteria.append(TaskDispatchOutbox.attempt == attempt)
    result = await db.execute(
        select(TaskDispatchOutbox).where(*criteria).with_for_update()
    )
    rows = list(result.scalars().all())
    for row in rows:
        row.status = "canceled"
        row.lease_owner = None
        row.lease_token = None
        row.lease_until = None
    await db.flush()
    return len(rows)


async def _claim_next(
    db: AsyncSession, owner: str, lease_seconds: int
) -> tuple[TaskDispatchOutbox, str] | None:
    now = datetime.now(UTC)
    query = (
        select(TaskDispatchOutbox)
        .where(
            TaskDispatchOutbox.available_at <= now,
            or_(
                TaskDispatchOutbox.status.in_(["pending", "retry"]),
                (TaskDispatchOutbox.status == "running")
                & (TaskDispatchOutbox.lease_until.is_not(None))
                & (TaskDispatchOutbox.lease_until <= now),
            ),
        )
        .order_by(TaskDispatchOutbox.available_at.asc(), TaskDispatchOutbox.created_at.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    row = (await db.execute(query)).scalar_one_or_none()
    if row is None:
        return None
    state = LeaseState(
        status=row.status,
        lease_owner=row.lease_owner,
        lease_token=row.lease_token,
        lease_until=row.lease_until,
        heartbeat_at=row.heartbeat_at,
    )
    token = claim_lease(state, owner, now, timedelta(seconds=lease_seconds))
    if token is None:
        return None
    row.status = state.status
    row.lease_owner = state.lease_owner
    row.lease_token = state.lease_token
    row.lease_until = state.lease_until
    row.heartbeat_at = state.heartbeat_at
    row.publish_attempts += 1
    # 先提交租约，再访问 broker。进程在发布前崩溃时，租约到期后可重复发布。
    await db.commit()
    return row, token


async def _mark_published(db: AsyncSession, row_id: str, token: str) -> bool:
    row = await db.get(TaskDispatchOutbox, row_id, with_for_update=True)
    if row is None or row.status != "running" or row.lease_token != token:
        return False
    row.status = "published"
    row.published_at = datetime.now(UTC)
    row.lease_owner = None
    row.lease_token = None
    row.lease_until = None
    await db.commit()
    return True


async def _mark_retry(db: AsyncSession, row_id: str, token: str, error: Exception) -> bool:
    row = await db.get(TaskDispatchOutbox, row_id, with_for_update=True)
    if row is None or row.status != "running" or row.lease_token != token:
        return False
    row.status = "retry"
    row.last_error = "broker_unavailable: 消息队列暂不可用"
    row.available_at = datetime.now(UTC) + timedelta(seconds=backoff_seconds(row.publish_attempts))
    row.lease_owner = None
    row.lease_token = None
    row.lease_until = None
    await db.commit()
    return True


async def dispatch_outbox_batch(
    db: AsyncSession,
    publisher: Publisher,
    *,
    owner: str,
    lease_seconds: int = 30,
    limit: int = 50,
) -> int:
    """领取、提交、发布、确认；发布失败保留可恢复 retry 行。"""
    published = 0
    for _ in range(limit):
        claimed = await _claim_next(db, owner, lease_seconds)
        if claimed is None:
            break
        row, token = claimed
        try:
            result = publisher(row.task_type, row.payload)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:  # noqa: BLE001 - 外部 broker 必须可恢复
            await _mark_retry(db, row.id, token, exc)
            continue
        if await _mark_published(db, row.id, token):
            published += 1
    return published


async def reconcile_pipeline_dispatches(
    db: AsyncSession,
    *,
    limit: int = 100,
) -> int:
    """恢复过期/半截 DAG，以及 commit 后缺失 outbox 的 pipeline run。"""
    from sqlalchemy.orm import aliased, noload

    class _RecordedPipelineFailure(RuntimeError):
        """把持久化错误类型送入统一安全映射，禁止原始错误消息出站。"""

        def __init__(self, error_type: str | None, retryable: bool | None) -> None:
            super().__init__("pipeline failed")
            self.error_type = error_type
            self.retryable = retryable

    now = datetime.now(UTC)
    failed_leaf = aliased(PipelineJob)
    failed_leaf_exists = exists().where(
        failed_leaf.run_id == PipelineJob.run_id,
        failed_leaf.stage.in_(["enrich", "parents"]),
        failed_leaf.status == "failed",
    )
    reserved_credit_exists = exists().where(
        CreditReservation.source_type == "pipeline",
        CreditReservation.source_id == PipelineJob.run_id,
        CreditReservation.status == "reserved",
    )
    result = await db.execute(
        select(PipelineJob)
        .where(
            or_(
                PipelineJob.status.in_(["pending", "retry", "ready"]),
                (PipelineJob.status == "running")
                & (PipelineJob.lease_until.is_not(None))
                & (PipelineJob.lease_until <= now),
                and_(
                    PipelineJob.stage.in_(["enrich", "parents"]),
                    PipelineJob.status == "running",
                    failed_leaf_exists,
                ),
                (PipelineJob.status == "failed") & reserved_credit_exists,
            )
        )
        .options(noload(PipelineJob.script))
        .order_by(PipelineJob.created_at.asc(), PipelineJob.attempt.desc())
        .limit(limit)
    )
    jobs = list(result.scalars().all())
    recovered = 0
    seen_runs: set[str] = set()
    for job in jobs:
        if job.run_id in seen_runs:
            continue
        seen_runs.add(job.run_id)
        outbox = await db.get(TaskDispatchOutbox, job.dispatch_id)
        state_result = await db.execute(
            select(
                PipelineJob.stage,
                PipelineJob.status,
                PipelineJob.attempt,
                PipelineJob.lease_until,
                PipelineJob.id,
                PipelineJob.error_type,
                PipelineJob.retryable,
            )
            .where(PipelineJob.run_id == job.run_id)
            .order_by(PipelineJob.attempt.asc())
        )
        latest = {row.stage: row for row in state_result.all()}
        terminal_failure = any(row.status == "failed" for row in latest.values())
        reservation = None
        if terminal_failure or outbox is None:
            reservation_result = await db.execute(
                select(CreditReservation).where(
                    CreditReservation.source_type == "pipeline",
                    CreditReservation.source_id == job.run_id,
                )
            )
            reservation = reservation_result.scalar_one_or_none()

        if terminal_failure:
            changed = False
            failed_row = next(row for row in latest.values() if row.status == "failed")
            failure = PipelineFailure.from_exception(
                _RecordedPipelineFailure(
                    failed_row.error_type,
                    failed_row.retryable,
                )
            )
            failure_payload = failure.to_dict()
            finished_at = datetime.now(UTC)
            for row in latest.values():
                if row.stage not in {"enrich", "parents"}:
                    continue
                pipeline_job = await db.get(PipelineJob, row.id)
                if pipeline_job is None:
                    continue
                if row.status == "failed":
                    has_existing_failure = (
                        pipeline_job.error_type is not None
                        or pipeline_job.error_message is not None
                        or pipeline_job.retryable is not None
                    )
                    if not has_existing_failure:
                        pipeline_job.error_type = failure.error_type
                        pipeline_job.error_message = failure.error_message
                        pipeline_job.retryable = failure.retryable
                    pipeline_job.finished_at = pipeline_job.finished_at or finished_at
                    changed = True
                elif row.status == "ready":
                    # ready 表示叶子计算与私有输出已成功，只是在等待统一发布。
                    # 兄弟叶失败不能把这一成功事实伪装成 skipped/internal_error。
                    pipeline_job.error_type = None
                    pipeline_job.error_message = None
                    pipeline_job.retryable = None
                    pipeline_job.finished_at = pipeline_job.finished_at or finished_at
                    changed = True
                elif row.status not in {"done", "skipped"}:
                    pipeline_job.status = "skipped"
                    pipeline_job.error_type = failure.error_type
                    pipeline_job.error_message = failure.error_message
                    pipeline_job.retryable = failure.retryable
                    pipeline_job.finished_at = finished_at
                    changed = True
                pipeline_job.lease_owner = None
                pipeline_job.lease_token = None
                pipeline_job.lease_until = None

                stage_result = await db.execute(
                    select(PipelineStage).where(
                        PipelineStage.script_id == pipeline_job.script_id,
                        PipelineStage.stage == pipeline_job.stage,
                    ).options(noload(PipelineStage.script))
                )
                pipeline_stage = stage_result.scalar_one_or_none()
                stage_detail = (pipeline_stage.detail or {}) if pipeline_stage is not None else {}
                stage_input_version = stage_detail.get("input_version")
                stage_input_client_save_seq = stage_detail.get("input_client_save_seq")
                if (
                    pipeline_stage is not None
                    and pipeline_stage.status in {"running", "ready", "failed", "done"}
                    and stage_input_version in {None, pipeline_job.input_version}
                    and stage_input_client_save_seq
                    in {None, pipeline_job.input_client_save_seq}
                ):
                    if pipeline_job.status in {"done", "ready"}:
                        pipeline_stage.status = pipeline_job.status
                        pipeline_stage.error = None
                    else:
                        pipeline_stage.status = "failed"
                        pipeline_stage.error = failure_payload
            if reservation is not None and reservation.status == "reserved":
                await credits_release(
                    db,
                    reservation.id,
                    "流水线执行失败",
                    commit=False,
                )
                changed = True
            if changed:
                recovered += 1
            continue

        if outbox is not None:
            expired_job = any(
                row.status == "running"
                and _is_expired(row.lease_until, now)
                for row in latest.values()
            )
            predecessors = {
                "extract": "split",
                "merge": "extract",
                "enrich": "merge",
                "parents": "merge",
            }
            half_dag = any(
                row.status == "pending"
                and stage in predecessors
                and latest.get(predecessors[stage]) is not None
                and latest[predecessors[stage]].status == "done"
                for stage, row in latest.items()
            )
            leaves_ready = all(
                latest.get(stage) is not None and latest[stage].status == "ready"
                for stage in ("enrich", "parents")
            )
            stale_root = (
                outbox.status == "published"
                and outbox.published_at is not None
                and _is_expired(outbox.published_at, now - timedelta(minutes=10))
                and latest.get("split") is not None
                and all(row.status == "pending" for row in latest.values())
            )
            expired_outbox = (
                outbox.status == "running"
                and _is_expired(outbox.lease_until, now)
            )
            if expired_outbox or (
                outbox.status == "published"
                and (expired_job or half_dag or leaves_ready or stale_root)
            ):
                outbox.status = "pending"
                outbox.available_at = now
                outbox.lease_owner = None
                outbox.lease_token = None
                outbox.lease_until = None
                recovered += 1
            continue

        if reservation is None:
            continue
        outbox = TaskDispatchOutbox(
            id=job.dispatch_id,
            task_type="pipeline.dag",
            job_id=job.run_id,
            attempt=job.input_version,
            payload={
                "script_id": job.script_id,
                "episode_id": job.episode_id,
                "input_version": job.input_version,
                "input_client_save_seq": job.input_client_save_seq,
                "reservation_id": reservation.id,
                "estimated": reservation.reserved_points,
                "dispatch_id": job.dispatch_id,
                "run_id": job.run_id,
            },
            status="pending",
            available_at=now,
        )
        db.add(outbox)
        await db.flush()
        recovered += 1
    return recovered
