"""媒体存储恢复任务服务。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select, update

from app.core.ids import new_id
from app.models.media_storage_job import MediaStorageJob
from app.providers.storage import validate_key_prefix

_SAFE_ERROR_MESSAGE = "媒体存储任务执行失败"


@dataclass(frozen=True)
class MediaStorageJobClaim:
    id: str
    idempotency_key: str
    job_type: str
    source_key: str
    final_key: str | None
    asset_id: str | None
    status: str
    attempt: int
    lease_token: str


def _backoff_seconds(attempt: int) -> int:
    return min(2 ** max(attempt - 1, 0), 300)


def _sanitize_error(exc: Exception) -> tuple[str, str]:
    return (type(exc).__name__, _SAFE_ERROR_MESSAGE)


def _now_like(reference: datetime | None = None) -> datetime:
    now = datetime.now(UTC)
    if reference is not None and reference.tzinfo is None:
        return now.replace(tzinfo=None)
    return now


async def enqueue_job(
    db,
    *,
    idempotency_key: str,
    job_type: str,
    source_key: str,
    final_key: str | None = None,
    asset_id: str | None = None,
) -> MediaStorageJob:
    """幂等写入媒体存储任务。"""
    validate_key_prefix(source_key)
    if job_type not in {"delete", "promote"}:
        raise ValueError("job_type 必须是 delete 或 promote")
    if job_type == "promote":
        if not final_key:
            raise ValueError("promote 任务必须提供 final_key")
        validate_key_prefix(final_key)

    existing = (
        await db.execute(
            select(MediaStorageJob).where(
                MediaStorageJob.idempotency_key == idempotency_key
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    row = MediaStorageJob(
        idempotency_key=idempotency_key,
        job_type=job_type,
        source_key=validate_key_prefix(source_key),
        final_key=validate_key_prefix(final_key) if final_key else None,
        asset_id=asset_id,
        status="pending",
        available_at=_now_like(),
    )
    db.add(row)
    await db.flush()
    return row


async def enqueue_promote(
    db,
    *,
    idempotency_key: str,
    source_key: str,
    final_key: str,
    asset_id: str | None = None,
) -> MediaStorageJob:
    return await enqueue_job(
        db,
        idempotency_key=idempotency_key,
        job_type="promote",
        source_key=source_key,
        final_key=final_key,
        asset_id=asset_id,
    )


async def enqueue_delete(
    db,
    *,
    idempotency_key: str,
    source_key: str,
    asset_id: str | None = None,
) -> MediaStorageJob:
    return await enqueue_job(
        db,
        idempotency_key=idempotency_key,
        job_type="delete",
        source_key=source_key,
        asset_id=asset_id,
    )


async def cancel_promotes_for_delete(
    db,
    *,
    asset_ids: set[str],
    final_keys: set[str],
) -> set[str]:
    """阻塞等待相关晋升任务结束，并取消仍未执行的任务。

    worker 从领取任务到存储 I/O、状态落库始终持有任务行锁。因此这里使用不带
    ``SKIP LOCKED`` 的行锁：若晋升已经运行，则等待它完成；若尚未运行，则先取消
    再安排源对象和目标对象删除，避免删除后又被晋升任务重新写回。
    """
    relations = []
    if asset_ids:
        relations.append(MediaStorageJob.asset_id.in_(sorted(asset_ids)))
    if final_keys:
        relations.append(MediaStorageJob.final_key.in_(sorted(final_keys)))
    if not relations:
        return set()

    rows = list(
        (
            await db.execute(
                select(MediaStorageJob)
                .where(
                    MediaStorageJob.job_type == "promote",
                    or_(*relations),
                )
                # 故意不按状态过滤：等待运行中行锁后，仍须读取其最新完成态并清理两端键。
                # 所有删除事务按稳定顺序取锁，降低多个资源批量删除时的死锁风险。
                .order_by(MediaStorageJob.id.asc())
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    cleanup_keys: set[str] = set()
    now = _now_like()
    for row in rows:
        cleanup_keys.add(row.source_key)
        if row.final_key:
            cleanup_keys.add(row.final_key)
        if row.status not in {"pending", "retry", "running"}:
            continue
        row.status = "canceled"
        row.lease_owner = None
        row.lease_token = None
        row.lease_until = None
        row.completed_at = _now_like(row.created_at) if row.created_at else now
    await db.flush()
    return cleanup_keys


async def claim_next_job(
    db, *, owner: str, lease_seconds: int = 300
) -> MediaStorageJobClaim | None:
    """原子领取下一条可执行任务。"""
    now = _now_like()
    candidate = (
        await db.execute(
            select(MediaStorageJob)
            .where(
                MediaStorageJob.available_at <= now,
                or_(
                    MediaStorageJob.status.in_(["pending", "retry"]),
                    (MediaStorageJob.status == "running")
                    & (MediaStorageJob.lease_until.is_not(None))
                    & (MediaStorageJob.lease_until <= now),
                ),
            )
            .order_by(MediaStorageJob.available_at.asc(), MediaStorageJob.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
    ).scalar_one_or_none()
    if candidate is None:
        return None
    token = new_id()
    next_attempt = candidate.attempt + 1
    result = await db.execute(
        update(MediaStorageJob)
        .where(
            MediaStorageJob.id == candidate.id,
            MediaStorageJob.status == candidate.status,
            MediaStorageJob.lease_token == candidate.lease_token,
        )
        .values(
            status="running",
            attempt=MediaStorageJob.attempt + 1,
            lease_owner=owner,
            lease_token=token,
            lease_until=now + timedelta(seconds=lease_seconds),
        )
    )
    await db.flush()
    if result.rowcount != 1:
        return None
    return MediaStorageJobClaim(
        id=candidate.id,
        idempotency_key=candidate.idempotency_key,
        job_type=candidate.job_type,
        source_key=candidate.source_key,
        final_key=candidate.final_key,
        asset_id=candidate.asset_id,
        status="running",
        attempt=next_attempt,
        lease_token=token,
    )


async def claim_job(
    db, job_id: str, *, owner: str, lease_seconds: int = 300
) -> MediaStorageJobClaim | None:
    """按 ID 原子领取指定任务，用于上传请求提交后的即时晋升。"""
    now = _now_like()
    candidate = (
        await db.execute(
            select(MediaStorageJob)
            .where(
                MediaStorageJob.id == job_id,
                or_(
                    MediaStorageJob.status.in_(["pending", "retry"]),
                    (MediaStorageJob.status == "running")
                    & (MediaStorageJob.lease_until.is_not(None))
                    & (MediaStorageJob.lease_until <= now),
                ),
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )
    ).scalar_one_or_none()
    if candidate is None:
        return None
    token = new_id()
    next_attempt = candidate.attempt + 1
    result = await db.execute(
        update(MediaStorageJob)
        .where(
            MediaStorageJob.id == candidate.id,
            MediaStorageJob.status == candidate.status,
            MediaStorageJob.lease_token == candidate.lease_token,
        )
        .values(
            status="running",
            attempt=MediaStorageJob.attempt + 1,
            lease_owner=owner,
            lease_token=token,
            lease_until=now + timedelta(seconds=lease_seconds),
        )
    )
    await db.flush()
    if result.rowcount != 1:
        return None
    return MediaStorageJobClaim(
        id=candidate.id,
        idempotency_key=candidate.idempotency_key,
        job_type=candidate.job_type,
        source_key=candidate.source_key,
        final_key=candidate.final_key,
        asset_id=candidate.asset_id,
        status="running",
        attempt=next_attempt,
        lease_token=token,
    )


async def mark_succeeded(db, job_id: str, lease_token: str) -> bool:
    now = _now_like()
    result = await db.execute(
        update(MediaStorageJob)
        .where(
            MediaStorageJob.id == job_id,
            MediaStorageJob.status == "running",
            MediaStorageJob.lease_token == lease_token,
        )
        .values(
            status="succeeded",
            lease_owner=None,
            lease_token=None,
            lease_until=None,
            completed_at=now,
        )
    )
    await db.flush()
    return result.rowcount == 1


async def mark_retry(db, job_id: str, lease_token: str, exc: Exception) -> bool:
    row = await db.get(MediaStorageJob, job_id, with_for_update=True)
    if row is None or row.status != "running" or row.lease_token != lease_token:
        return False
    error_type, error_message = _sanitize_error(exc)
    row.status = "retry"
    row.available_at = _now_like(row.created_at) + timedelta(seconds=_backoff_seconds(row.attempt))
    row.lease_owner = None
    row.lease_token = None
    row.lease_until = None
    row.last_error_type = error_type
    row.last_error_message = error_message
    await db.flush()
    return True


async def mark_failed(db, job_id: str, lease_token: str, exc: Exception) -> bool:
    error_type, error_message = _sanitize_error(exc)
    result = await db.execute(
        update(MediaStorageJob)
        .where(
            MediaStorageJob.id == job_id,
            MediaStorageJob.status == "running",
            MediaStorageJob.lease_token == lease_token,
        )
        .values(
            status="failed",
            lease_owner=None,
            lease_token=None,
            lease_until=None,
            last_error_type=error_type,
            last_error_message=error_message,
        )
    )
    await db.flush()
    return result.rowcount == 1
