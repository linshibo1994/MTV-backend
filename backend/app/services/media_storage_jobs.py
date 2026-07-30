"""媒体存储恢复任务服务。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError

from app.core.errors import ConflictError
from app.core.ids import new_id
from app.core.job_state import MEDIA_STORAGE_JOB_LEASE_SECONDS
from app.models.media_storage_job import MediaStorageJob
from app.models.media_upload import MediaUpload
from app.models.script import Script
from app.providers.storage import (
    StorageObjectConflictError,
    StorageObjectIntegrityError,
    StorageObjectNotFoundError,
    validate_key_prefix,
)

_SAFE_ERROR_MESSAGE = "媒体存储任务执行失败"
_DEFAULT_MAX_ATTEMPTS = 8
_DELETE_REFERENCE_RECHECK_SECONDS = 300
_UNCHANGED_ASSET_ID = object()


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
    upload_id: str | None = None
    expected_sha256: str | None = None
    expected_size_bytes: int | None = None
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS


@dataclass(frozen=True)
class MediaUploadPromoteState:
    job_status: str
    upload_status: str | None
    asset_id: str | None


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
    upload_id: str | None = None,
    expected_sha256: str | None = None,
    expected_size_bytes: int | None = None,
    available_at: datetime | None = None,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    reactivate_canceled: bool = False,
) -> MediaStorageJob:
    """幂等写入媒体存储任务。

    ``canceled`` 可能表示永久撤销，也可能表示引用暂时阻止删除。只有已经重新完成
    引用判定的调用方才可显式请求复活，避免通用补偿误改业务终态。
    """
    validate_key_prefix(source_key)
    if job_type not in {"delete", "promote"}:
        raise ValueError("job_type 必须是 delete 或 promote")
    if job_type == "promote":
        if not final_key:
            raise ValueError("promote 任务必须提供 final_key")
        validate_key_prefix(final_key)
        if (expected_sha256 is None) != (expected_size_bytes is None):
            raise ValueError("promote 任务的摘要与大小必须同时提供")
    if expected_sha256 is not None and len(expected_sha256) != 64:
        raise ValueError("expected_sha256 必须是 64 位十六进制摘要")
    if expected_size_bytes is not None and expected_size_bytes < 0:
        raise ValueError("expected_size_bytes 不能为负数")
    if max_attempts <= 0:
        raise ValueError("max_attempts 必须大于 0")

    normalized_source = validate_key_prefix(source_key)
    normalized_final = validate_key_prefix(final_key) if final_key else None

    def _assert_same_payload(existing: MediaStorageJob) -> None:
        payload = (
            existing.job_type,
            existing.source_key,
            existing.final_key,
            existing.asset_id,
            existing.upload_id,
            existing.expected_sha256,
            existing.expected_size_bytes,
            existing.max_attempts,
        )
        requested = (
            job_type,
            normalized_source,
            normalized_final,
            asset_id,
            upload_id,
            expected_sha256,
            expected_size_bytes,
            max_attempts,
        )
        if payload != requested:
            raise ConflictError("媒体存储幂等键对应的任务载荷不一致")

    def _reactivate_canceled_delete(existing: MediaStorageJob) -> bool:
        if (
            not reactivate_canceled
            or job_type != "delete"
            or existing.status != "canceled"
        ):
            return False
        existing.status = "pending"
        existing.attempt = 0
        existing.available_at = available_at or _now_like(existing.created_at)
        existing.lease_owner = None
        existing.lease_token = None
        existing.lease_until = None
        existing.last_error_type = None
        existing.last_error_message = None
        existing.completed_at = None
        return True

    existing = (
        await db.execute(
            select(MediaStorageJob).where(
                MediaStorageJob.idempotency_key == idempotency_key
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        _assert_same_payload(existing)
        if _reactivate_canceled_delete(existing):
            await db.flush()
        return existing

    row = MediaStorageJob(
        idempotency_key=idempotency_key,
        job_type=job_type,
        source_key=normalized_source,
        final_key=normalized_final,
        asset_id=asset_id,
        upload_id=upload_id,
        expected_sha256=expected_sha256,
        expected_size_bytes=expected_size_bytes,
        status="pending",
        available_at=available_at or _now_like(),
        max_attempts=max_attempts,
    )
    try:
        begin_nested = getattr(db, "begin_nested", None)
        if begin_nested is None:
            db.add(row)
            await db.flush()
        else:
            async with begin_nested():
                db.add(row)
                await db.flush()
        return row
    except IntegrityError:
        existing = (
            await db.execute(
                select(MediaStorageJob).where(
                    MediaStorageJob.idempotency_key == idempotency_key
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            raise
        _assert_same_payload(existing)
        if _reactivate_canceled_delete(existing):
            await db.flush()
        return existing


async def enqueue_promote(
    db,
    *,
    idempotency_key: str,
    source_key: str,
    final_key: str,
    asset_id: str | None = None,
    upload_id: str | None = None,
    expected_sha256: str | None = None,
    expected_size_bytes: int | None = None,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
) -> MediaStorageJob:
    return await enqueue_job(
        db,
        idempotency_key=idempotency_key,
        job_type="promote",
        source_key=source_key,
        final_key=final_key,
        asset_id=asset_id,
        upload_id=upload_id,
        expected_sha256=expected_sha256,
        expected_size_bytes=expected_size_bytes,
        max_attempts=max_attempts,
    )


async def enqueue_delete(
    db,
    *,
    idempotency_key: str,
    source_key: str,
    asset_id: str | None = None,
    upload_id: str | None = None,
    available_at: datetime | None = None,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    reactivate_canceled: bool = False,
) -> MediaStorageJob:
    return await enqueue_job(
        db,
        idempotency_key=idempotency_key,
        job_type="delete",
        source_key=source_key,
        asset_id=asset_id,
        upload_id=upload_id,
        available_at=available_at,
        max_attempts=max_attempts,
        reactivate_canceled=reactivate_canceled,
    )


async def create_upload_staging(
    db,
    *,
    upload_id: str,
    script_id: str,
    material_id: str,
    subject_id: str | None,
    temp_storage_key: str,
    final_storage_key: str,
    original_filename: str,
    mime_type: str,
    size_bytes: int,
    sha256: str,
    cleanup_delay_seconds: int = 3600,
) -> tuple[MediaUpload, MediaStorageJob]:
    """I/O 前持久化上传归属和延迟 temp 清理任务。"""
    script = await db.get(Script, script_id, with_for_update=True)
    if script is None:
        raise ValueError("媒体上传所属剧本不存在")
    upload = MediaUpload(
        id=upload_id,
        owner_script_id=script.id,
        script_id=script.id,
        material_id=material_id,
        subject_id=subject_id,
        temp_storage_key=validate_key_prefix(temp_storage_key),
        final_storage_key=validate_key_prefix(final_storage_key),
        original_filename=original_filename,
        mime_type=mime_type,
        size_bytes=size_bytes,
        sha256=sha256,
        status="staging",
    )
    db.add(upload)
    await db.flush()
    cleanup = await enqueue_delete(
        db,
        idempotency_key=f"upload-cleanup:{upload.id}",
        source_key=upload.temp_storage_key,
        upload_id=upload.id,
        available_at=_now_like() + timedelta(seconds=cleanup_delay_seconds),
    )
    return upload, cleanup


async def activate_upload_promote(db, upload_id: str) -> MediaStorageJob:
    """完整 temp 写入后取消兜底清理，并原子激活 promote。"""
    snapshot = await db.get(MediaUpload, upload_id)
    if snapshot is None:
        raise ValueError("媒体上传记录不存在")
    script = await db.get(Script, snapshot.owner_script_id, with_for_update=True)
    if script is None:
        raise ConflictError("媒体上传所属剧本已删除")
    upload = (
        await db.execute(
            select(MediaUpload)
            .where(MediaUpload.id == upload_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if upload is None:
        raise ValueError("媒体上传记录不存在")
    if upload.status != "staging":
        raise ConflictError("媒体上传状态不允许晋升")
    cleanup = (
        await db.execute(
            select(MediaStorageJob)
            .where(
                MediaStorageJob.upload_id == upload.id,
                MediaStorageJob.idempotency_key == f"upload-cleanup:{upload.id}",
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if cleanup is None:
        raise ValueError("媒体上传清理意图不存在")
    if cleanup.status in {"pending", "retry"}:
        cleanup.status = "canceled"
        cleanup.completed_at = _now_like(cleanup.created_at)
    elif cleanup.status != "canceled":
        raise ConflictError("媒体上传清理任务已开始执行")
    promote = await enqueue_promote(
        db,
        idempotency_key=f"upload-promote:{upload.id}",
        source_key=upload.temp_storage_key,
        final_key=upload.final_storage_key,
        upload_id=upload.id,
        expected_sha256=upload.sha256,
        expected_size_bytes=upload.size_bytes,
    )
    await db.flush()
    return promote


async def make_upload_cleanup_due(db, upload_id: str, exc: Exception) -> None:
    """上传写入或激活失败后启用已持久化的清理意图。"""
    upload = await db.get(MediaUpload, upload_id, with_for_update=True)
    if upload is None:
        return
    if upload.status == "staging":
        upload.status = "failed"
        upload.last_error_type = type(exc).__name__
        upload.completed_at = _now_like(upload.created_at)
    cleanup = (
        await db.execute(
            select(MediaStorageJob)
            .where(
                MediaStorageJob.upload_id == upload.id,
                MediaStorageJob.idempotency_key == f"upload-cleanup:{upload.id}",
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if cleanup is not None and cleanup.status in {"pending", "retry", "canceled"}:
        cleanup.status = "pending"
        cleanup.available_at = _now_like(cleanup.created_at)
        cleanup.completed_at = None
        cleanup.lease_owner = None
        cleanup.lease_token = None
        cleanup.lease_until = None
    await db.flush()


async def _verify_object(
    storage,
    storage_key: str,
    *,
    expected_sha256: str,
    expected_size_bytes: int,
) -> None:
    if not await storage.exists(storage_key):
        raise StorageObjectNotFoundError(storage_key)
    size = await storage.get_size(storage_key)
    if size != expected_size_bytes:
        raise StorageObjectIntegrityError(storage_key)
    digest = hashlib.sha256()
    streamed = 0
    async for chunk in storage.iter_chunks(
        storage_key,
        chunk_size=1024 * 1024,
        start=0,
        end=None,
    ):
        streamed += len(chunk)
        digest.update(chunk)
    if streamed != expected_size_bytes or digest.hexdigest() != expected_sha256:
        raise StorageObjectIntegrityError(storage_key)


async def promote_with_integrity(storage, claim: MediaStorageJobClaim) -> str:
    """以持久化摘要区分真实重放和无关 final，禁止静默覆盖。"""
    if not claim.final_key:
        raise ValueError("promote 任务缺少 final_key")
    if claim.expected_sha256 is None or claim.expected_size_bytes is None:
        published = await storage.promote_if_absent(claim.source_key, claim.final_key)
        if not published:
            # 旧任务没有持久化摘要，无法证明既有目标属于本次任务，必须失败关闭。
            raise StorageObjectConflictError(claim.final_key)
        return claim.final_key

    if await storage.exists(claim.final_key):
        try:
            await _verify_object(
                storage,
                claim.final_key,
                expected_sha256=claim.expected_sha256,
                expected_size_bytes=claim.expected_size_bytes,
            )
        except StorageObjectIntegrityError as exc:
            raise StorageObjectConflictError(claim.final_key) from exc
        if await storage.exists(claim.source_key):
            await storage.delete(claim.source_key)
        return claim.final_key

    await _verify_object(
        storage,
        claim.source_key,
        expected_sha256=claim.expected_sha256,
        expected_size_bytes=claim.expected_size_bytes,
    )
    published = await storage.promote_if_absent(claim.source_key, claim.final_key)
    if not published:
        try:
            await _verify_object(
                storage,
                claim.final_key,
                expected_sha256=claim.expected_sha256,
                expected_size_bytes=claim.expected_size_bytes,
            )
        except StorageObjectIntegrityError as exc:
            raise StorageObjectConflictError(claim.final_key) from exc
        if await storage.exists(claim.source_key):
            await storage.delete(claim.source_key)
        return claim.final_key
    await _verify_object(
        storage,
        claim.final_key,
        expected_sha256=claim.expected_sha256,
        expected_size_bytes=claim.expected_size_bytes,
    )
    return claim.final_key


async def finalize_upload_ready(db, claim: MediaStorageJobClaim) -> str | None:
    """对象验证成功后，与候选资产和任务成功状态在同一事务提交。"""
    if claim.upload_id is None:
        return claim.asset_id
    snapshot = await db.get(MediaUpload, claim.upload_id)
    if snapshot is None:
        raise ValueError("媒体上传记录不存在")

    subject = None
    creates_candidate = snapshot.subject_id is not None and (
        snapshot.mime_type.startswith("image/") or snapshot.mime_type.startswith("audio/")
    )
    if creates_candidate:
        from app.models.subject import Subject

        # 删除使用 Script→Subject→Upload→Job；成功落库沿用同一顺序，避免外键检查
        # 隐式请求 Script 锁时与删除事务形成反转。
        script = await db.get(Script, snapshot.owner_script_id, with_for_update=True)
        if script is None:
            raise ValueError("媒体上传所属剧本不存在")
        subject = await db.get(Subject, snapshot.subject_id, with_for_update=True)

    upload = (
        await db.execute(
            select(MediaUpload)
            .where(MediaUpload.id == claim.upload_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if upload is None:
        raise ValueError("媒体上传记录不存在")
    if upload.status == "ready":
        return upload.asset_id
    if upload.status != "staging":
        raise ConflictError("媒体上传已结束，拒绝迟到晋升")

    asset_id: str | None = None
    if upload.subject_id is not None and (
        upload.mime_type.startswith("image/") or upload.mime_type.startswith("audio/")
    ):
        from app.crud.asset import add_candidate

        if (
            subject is None
            or subject.id != upload.subject_id
            or subject.script_id != upload.owner_script_id
        ):
            raise ValueError("媒体上传主体不存在")
        kind = "audio" if upload.mime_type.startswith("audio/") else "image"
        entry = await add_candidate(
            db,
            script_id=upload.owner_script_id,
            subject_id=subject.id,
            kind=kind,
            storage_key=f"/{upload.final_storage_key}",
            mime_type=upload.mime_type,
            size_bytes=upload.size_bytes,
            source="upload_images",
            sha256=upload.sha256,
            subject=subject,
        )
        asset_id = entry.id
        upload.asset_id = asset_id
    upload.status = "ready"
    upload.completed_at = _now_like(upload.created_at)
    upload.last_error_type = None
    await db.flush()
    return asset_id


async def load_upload_promote_state(db, job_id: str) -> MediaUploadPromoteState | None:
    """绕过身份映射缓存读取上传和晋升任务的权威已提交状态。"""
    job = (
        await db.execute(
            select(MediaStorageJob)
            .where(MediaStorageJob.id == job_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if job is None or job.job_type != "promote" or job.upload_id is None:
        return None
    upload = (
        await db.execute(
            select(MediaUpload)
            .where(MediaUpload.id == job.upload_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    return MediaUploadPromoteState(
        job_status=job.status,
        upload_status=upload.status if upload is not None else None,
        asset_id=upload.asset_id if upload is not None else None,
    )


async def _upload_success_invariant(
    db,
    job: MediaStorageJob,
    upload: MediaUpload,
) -> bool:
    if (
        upload.status != "ready"
        or job.upload_id != upload.id
        or job.asset_id != upload.asset_id
        or validate_key_prefix(job.source_key) != validate_key_prefix(upload.temp_storage_key)
        or job.final_key is None
        or validate_key_prefix(job.final_key) != validate_key_prefix(upload.final_storage_key)
    ):
        return False

    creates_candidate = upload.subject_id is not None and (
        upload.mime_type.startswith("image/") or upload.mime_type.startswith("audio/")
    )
    if not creates_candidate:
        return True
    if upload.asset_id is None:
        return False

    from app.models.asset_entry import AssetEntry

    asset = (
        await db.execute(
            select(AssetEntry)
            .where(AssetEntry.id == upload.asset_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    return bool(
        asset is not None
        and asset.status != "deleted"
        and asset.script_id == upload.owner_script_id
        and asset.subject_id == upload.subject_id
        and validate_key_prefix(asset.storage_key) == validate_key_prefix(upload.final_storage_key)
    )


async def is_authoritative_claim_succeeded(db, claim: MediaStorageJobClaim) -> bool:
    """以已提交数据库状态判定不确定结果是否确已成功。"""
    job = (
        await db.execute(
            select(MediaStorageJob)
            .where(MediaStorageJob.id == claim.id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if job is None or job.status != "succeeded":
        return False
    if (
        job.idempotency_key != claim.idempotency_key
        or job.job_type != claim.job_type
        or validate_key_prefix(job.source_key) != validate_key_prefix(claim.source_key)
        or job.final_key != claim.final_key
        or job.upload_id != claim.upload_id
        or job.expected_sha256 != claim.expected_sha256
        or job.expected_size_bytes != claim.expected_size_bytes
        or job.attempt != claim.attempt
        or job.max_attempts != claim.max_attempts
    ):
        return False
    if job.job_type != "promote" or job.upload_id is None:
        return job.asset_id == claim.asset_id

    upload = (
        await db.execute(
            select(MediaUpload)
            .where(MediaUpload.id == job.upload_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    return upload is not None and await _upload_success_invariant(db, job, upload)


async def is_authoritative_job_succeeded(db, job_id: str) -> bool:
    """批次边界的防御性成功复读，拒绝不完整的 upload/asset 状态。"""
    job = (
        await db.execute(
            select(MediaStorageJob)
            .where(MediaStorageJob.id == job_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if job is None or job.status != "succeeded":
        return False
    if job.job_type != "promote" or job.upload_id is None:
        return True
    upload = (
        await db.execute(
            select(MediaUpload)
            .where(MediaUpload.id == job.upload_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    return upload is not None and await _upload_success_invariant(db, job, upload)


async def finalize_upload_delete(db, claim: MediaStorageJobClaim) -> None:
    if claim.upload_id is None:
        return
    upload = await db.get(MediaUpload, claim.upload_id, with_for_update=True)
    if upload is not None and upload.status == "staging":
        upload.status = "failed"
        upload.last_error_type = "AbandonedUpload"
        upload.completed_at = _now_like(upload.created_at)
        await db.flush()


async def prepare_delete_effect(db, claim: MediaStorageJobClaim) -> bool:
    """物理删除前重验引用并持久化 per-key deleting tombstone。"""
    if claim.job_type != "delete":
        raise ValueError("仅 delete 任务允许执行删除前引用校验")

    from app.models.storage_reference_guard import acquire_storage_key_locks
    from app.services.storage_references import (
        StorageReferenceExclusions,
        decide_storage_key_deletes,
    )

    source_key = validate_key_prefix(claim.source_key)
    await acquire_storage_key_locks(db, {source_key})
    row = await db.get(MediaStorageJob, claim.id, with_for_update=True)
    if (
        row is None
        or row.job_type != "delete"
        or row.lease_token != claim.lease_token
        or row.status not in {"running", "deleting"}
    ):
        raise ConflictError("媒体存储任务 lease 已失效")
    if row.status == "deleting":
        return True

    exclusions = StorageReferenceExclusions()
    if claim.upload_id is not None:
        upload = await db.get(MediaUpload, claim.upload_id)
        if upload is not None:
            temp_key = validate_key_prefix(upload.temp_storage_key)
            if source_key == temp_key or upload.status in {"failed", "deleted"}:
                exclusions.media_upload_ids.add(upload.id)
    if claim.asset_id is not None:
        from app.models.asset_entry import AssetEntry

        asset = await db.get(AssetEntry, claim.asset_id)
        if asset is not None and asset.status == "deleted":
            exclusions.asset_entry_ids.add(asset.id)

    decision = await decide_storage_key_deletes(
        db,
        {source_key},
        exclusions=exclusions,
    )
    active_promote_id = (
        await db.execute(
            select(MediaStorageJob.id)
            .where(
                MediaStorageJob.job_type == "promote",
                MediaStorageJob.status.in_(["pending", "retry", "running"]),
                or_(
                    MediaStorageJob.source_key == source_key,
                    MediaStorageJob.final_key == source_key,
                ),
            )
            .order_by(MediaStorageJob.id.asc())
            .limit(1)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if source_key in decision.deletable_keys and active_promote_id is None:
        row.status = "deleting"
        row.last_error_type = None
        row.last_error_message = None
        await db.flush()
        return True

    row.status = "canceled"
    row.attempt = max(row.attempt - 1, 0)
    row.available_at = _now_like() + timedelta(seconds=_DELETE_REFERENCE_RECHECK_SECONDS)
    row.lease_owner = None
    row.lease_token = None
    row.lease_until = None
    row.last_error_type = "StorageKeyStillReferenced"
    row.last_error_message = "媒体对象仍被引用，取消本次删除"
    row.completed_at = _now_like(row.created_at)
    await db.flush()
    return False


async def cancel_promotes_for_delete(
    db,
    *,
    asset_ids: set[str],
    final_keys: set[str],
) -> set[str]:
    """在业务上传行之后锁定并取消相关晋升任务。

    worker 领取后立即提交并释放任务行锁；删除事务因此可以取消正在执行存储 I/O
    的 lease。迟到 worker 只能在 Upload→Job CAS 结果事务中观察取消并回滚业务状态。
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
                # 故意不按状态过滤：仍须读取最新完成态并清理两端键。
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


def _claim_snapshot(
    row: MediaStorageJob,
    *,
    status: str,
    lease_token: str,
    attempt: int | None = None,
) -> MediaStorageJobClaim:
    return MediaStorageJobClaim(
        id=row.id,
        idempotency_key=row.idempotency_key,
        job_type=row.job_type,
        source_key=row.source_key,
        final_key=row.final_key,
        asset_id=row.asset_id,
        upload_id=row.upload_id,
        expected_sha256=row.expected_sha256,
        expected_size_bytes=row.expected_size_bytes,
        status=status,
        attempt=row.attempt if attempt is None else attempt,
        max_attempts=row.max_attempts,
        lease_token=lease_token,
    )


async def _terminalize_exhausted_claim(
    db,
    candidate: MediaStorageJob,
) -> MediaStorageJobClaim | None:
    if candidate.attempt < candidate.max_attempts:
        return None
    terminal_status = "effect_failed" if candidate.status == "deleting" else "failed"
    candidate.status = terminal_status
    candidate.lease_owner = None
    candidate.lease_token = None
    candidate.lease_until = None
    candidate.completed_at = _now_like(candidate.created_at)
    await db.flush()
    return _claim_snapshot(candidate, status=terminal_status, lease_token="")


async def claim_next_job(
    db,
    *,
    owner: str,
    lease_seconds: int = MEDIA_STORAGE_JOB_LEASE_SECONDS,
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
                    (MediaStorageJob.status == "deleting")
                    & (
                        MediaStorageJob.lease_until.is_(None)
                        | (MediaStorageJob.lease_until <= now)
                    ),
                ),
            )
            .order_by(MediaStorageJob.available_at.asc(), MediaStorageJob.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
    ).scalar_one_or_none()
    if candidate is None:
        return None
    exhausted = await _terminalize_exhausted_claim(db, candidate)
    if exhausted is not None:
        return exhausted
    token = new_id()
    next_attempt = candidate.attempt + 1
    next_status = "deleting" if candidate.status == "deleting" else "running"
    result = await db.execute(
        update(MediaStorageJob)
        .where(
            MediaStorageJob.id == candidate.id,
            MediaStorageJob.status == candidate.status,
            MediaStorageJob.lease_token == candidate.lease_token,
        )
        .values(
            status=next_status,
            attempt=MediaStorageJob.attempt + 1,
            lease_owner=owner,
            lease_token=token,
            lease_until=now + timedelta(seconds=lease_seconds),
        )
    )
    await db.flush()
    if result.rowcount != 1:
        return None
    return _claim_snapshot(
        candidate,
        status=next_status,
        attempt=next_attempt,
        lease_token=token,
    )


async def claim_job(
    db,
    job_id: str,
    *,
    owner: str,
    lease_seconds: int = MEDIA_STORAGE_JOB_LEASE_SECONDS,
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
    exhausted = await _terminalize_exhausted_claim(db, candidate)
    if exhausted is not None:
        return exhausted
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
    return _claim_snapshot(
        candidate,
        status="running",
        attempt=next_attempt,
        lease_token=token,
    )


async def mark_succeeded(
    db,
    job_id: str,
    lease_token: str,
    *,
    asset_id: str | None | object = _UNCHANGED_ASSET_ID,
    expected_status: str = "running",
) -> bool:
    now = _now_like()
    values: dict[str, object] = {
        "status": "succeeded",
        "lease_owner": None,
        "lease_token": None,
        "lease_until": None,
        "completed_at": now,
    }
    if asset_id is not _UNCHANGED_ASSET_ID:
        values["asset_id"] = asset_id
    result = await db.execute(
        update(MediaStorageJob)
        .where(
            MediaStorageJob.id == job_id,
            MediaStorageJob.status == expected_status,
            MediaStorageJob.lease_token == lease_token,
        )
        .values(**values)
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
    now = _now_like()
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
            completed_at=now,
        )
    )
    await db.flush()
    return result.rowcount == 1


async def mark_execution_error(
    db,
    claim: MediaStorageJobClaim,
    exc: Exception,
) -> bool:
    """按 Upload→Job 顺序及持久化 lease 落 retry/failed。"""
    upload = None
    if claim.job_type == "promote" and claim.upload_id is not None:
        upload = await db.get(MediaUpload, claim.upload_id, with_for_update=True)
    row = await db.get(MediaStorageJob, claim.id, with_for_update=True)
    if row is None or row.lease_token != claim.lease_token:
        return False
    if row.job_type == "delete" and row.status == "deleting":
        error_type, error_message = _sanitize_error(exc)
        row.lease_owner = None
        row.lease_token = None
        row.lease_until = None
        row.last_error_type = error_type
        row.last_error_message = error_message
        if row.attempt < row.max_attempts:
            row.available_at = _now_like(row.created_at) + timedelta(
                seconds=_backoff_seconds(row.attempt)
            )
        else:
            row.status = "effect_failed"
            row.completed_at = _now_like(row.created_at)
        await db.flush()
        return True
    if row.status != "running":
        return False
    permanent = isinstance(
        exc,
        (
            ConflictError,
            StorageObjectConflictError,
            StorageObjectIntegrityError,
            StorageObjectNotFoundError,
            ValueError,
        ),
    )
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code") or "")
        permanent = permanent or code in {
            "AccessDenied",
            "InvalidAccessKeyId",
            "InvalidBucketName",
            "NoSuchBucket",
            "SignatureDoesNotMatch",
        }
    error_type, error_message = _sanitize_error(exc)
    if not permanent and row.attempt < row.max_attempts:
        row.status = "retry"
        row.available_at = _now_like(row.created_at) + timedelta(
            seconds=_backoff_seconds(row.attempt)
        )
        row.lease_owner = None
        row.lease_token = None
        row.lease_until = None
        row.last_error_type = error_type
        row.last_error_message = error_message
        await db.flush()
        return True

    row.status = "failed"
    row.lease_owner = None
    row.lease_token = None
    row.lease_until = None
    row.last_error_type = error_type
    row.last_error_message = error_message
    row.completed_at = _now_like(row.created_at)
    if row.job_type == "promote" and row.upload_id is not None:
        if upload is not None and upload.status == "staging":
            upload.status = "failed"
            upload.last_error_type = type(exc).__name__
            upload.completed_at = _now_like(upload.created_at)
        cleanup_keys = {row.source_key}
        if row.final_key and not isinstance(exc, StorageObjectConflictError):
            cleanup_keys.add(row.final_key)
        for key in cleanup_keys:
            digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
            await enqueue_delete(
                db,
                idempotency_key=f"failed-upload-delete:{row.id}:{digest}",
                source_key=key,
                upload_id=row.upload_id,
            )
    await db.flush()
    return True
