"""生成任务 Celery worker — 文生图异步执行。

generate_images_task(job_id) 流程：
1. 读 input_json.resolved_prompts.final（回退 input_json.prompt）作为生图 prompt
2. 解析 script_id（job.script_id 优先，兜底从 material.script_id 查询）
3. get_image_provider(model).generate() 调用对应供应商生成图片（可生成多张选一）
4. 本地存储 (/generate_images/<mid>/<id>.png) + 计算 sha256
5. 全部生成成功后统一调 crud.asset.add_candidate 创建候选资产（补全 script_id/kind 位置参数）
6. 更新 subject.review_status=pending（候选待审，驱动 NotDesigned->CandidateReady 状态迁移）
7. 更新 job.output_json.imageOutputs[] / primaryImageIndex
8. credits.settle(actual) 结算积分（settle 失败则 release 释放冻结，避免永久冻结泄漏）
9. 失败：清理已落盘图片、credits.release 释放冻结、回退 subject.review_status、写 error_message，不重试

说明：
- Celery 任务体内通过 asyncio.run 执行异步主体，复用 async DB session 与 async provider。
- 存储统一经 app.providers.storage.get_storage() 抽象（与 video_generation/timeline_render 一致），
  支持 local / s3 两种 driver，S3 模式下生成图正确写入对象存储、media 代理经同一 provider 读取。
- 积分 actual 按实际成功张数差异化结算：actual = settings.price_image_gen *
  len(image_outputs)，Provider 少图时不对未生成图片收费。
  后续接入 provider 返回的真实计费字段时，用真实值替换 actual。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Any

from PIL import Image
from sqlalchemy import or_, select
from sqlalchemy import update as sa_update
from sqlalchemy.orm import noload

from app.core.config import settings
from app.core.database import async_session_factory
from app.core.errors import ConflictError, ForbiddenError, NotFoundError
from app.core.ids import new_id
from app.crud.asset import add_candidate
from app.crud.generation import get_job
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.subject import Subject
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.providers.image_gen import ImageResult, get_image_provider
from app.providers.storage import get_storage
from app.services.credits import release as credits_release
from app.services.credits import settle as credits_settle
from app.services.media_storage_jobs import enqueue_delete
from app.workers.celery_app import celery_app

logger = logging.getLogger(__name__)
_LEASE_SECONDS = 300
_TERMINAL_STATUSES = {"succeeded", "failed", "canceled"}


@dataclass(frozen=True)
class GenerationJobClaim:
    job_id: str
    lease_token: str
    attempt: int
    input_version: int


@dataclass(frozen=True)
class ValidatedImage:
    data: bytes
    mime_type: str
    extension: str


@dataclass(frozen=True)
class PersistedImage:
    temp_key: str
    final_key: str
    sha256: str
    image: ValidatedImage


def _is_expired(value: datetime | None, now: datetime) -> bool:
    if value is None:
        return False
    comparable_now = now if value.tzinfo is not None else now.replace(tzinfo=None)
    return value <= comparable_now


async def claim_generation_job(
    db,
    job_id: str,
    owner: str,
    *,
    lease_seconds: int = _LEASE_SECONDS,
) -> GenerationJobClaim | None:
    """原子领取 starting 或租约过期的 processing job。"""
    now = datetime.now(UTC)
    candidate = (
        await db.execute(
            select(GenerationJob)
            .where(
                GenerationJob.job_id == job_id,
                or_(
                    GenerationJob.status == "starting",
                    (GenerationJob.status == "processing")
                    & (GenerationJob.lease_until.is_not(None))
                    & (GenerationJob.lease_until <= now),
                ),
            )
            .options(noload(GenerationJob.material))
            .with_for_update()
        )
    ).scalar_one_or_none()
    if candidate is None:
        return None
    token = new_id()
    lease_token_filter = (
        GenerationJob.lease_token.is_(None)
        if candidate.lease_token is None
        else GenerationJob.lease_token == candidate.lease_token
    )
    result = await db.execute(
        sa_update(GenerationJob)
        .where(
            GenerationJob.job_id == candidate.job_id,
            GenerationJob.status == candidate.status,
            lease_token_filter,
        )
        .values(
            status="processing",
            progress=max(int(candidate.progress or 0), 10),
            progress_stage="starting",
            progress_message="开始生成",
            lease_owner=owner,
            lease_token=token,
            lease_until=now + timedelta(seconds=lease_seconds),
            heartbeat_at=now,
            started_at=candidate.started_at or now,
        )
    )
    await db.flush()
    if result.rowcount != 1:
        return None
    return GenerationJobClaim(
        job_id=candidate.job_id,
        lease_token=token,
        attempt=candidate.attempt,
        input_version=candidate.input_version,
    )


async def _get_generation_job(db: Any, job_id: str) -> GenerationJob | None:
    return (
        await db.execute(
            select(GenerationJob)
            .where(GenerationJob.job_id == job_id)
            .options(noload(GenerationJob.material))
        )
    ).scalar_one_or_none()


async def heartbeat_generation_job(
    db, job_id: str, lease_token: str, *, lease_seconds: int = _LEASE_SECONDS
) -> bool:
    now = datetime.now(UTC)
    result = await db.execute(
        sa_update(GenerationJob)
        .where(
            GenerationJob.job_id == job_id,
            GenerationJob.status == "processing",
            GenerationJob.lease_token == lease_token,
        )
        .values(heartbeat_at=now, lease_until=now + timedelta(seconds=lease_seconds))
    )
    await db.flush()
    return result.rowcount == 1


async def finish_generation_job(
    db,
    job_id: str,
    lease_token: str,
    status: str,
    *,
    output_json: dict | None = None,
    metrics: dict | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    retryable: bool | None = None,
) -> bool:
    if status not in _TERMINAL_STATUSES:
        raise ValueError("无效 GenerationJob 终态")
    values: dict[str, object | None] = {
        "status": status,
        "lease_owner": None,
        "lease_token": None,
        "lease_until": None,
        "finished_at": datetime.now(UTC),
    }
    if status == "succeeded":
        values.update(
            progress=100,
            progress_stage="succeeded",
            progress_message="生成完成",
        )
    if status == "canceled":
        values["canceled_at"] = datetime.now(UTC)
    if output_json is not None:
        values["output_json"] = output_json
    if metrics is not None:
        values["metrics"] = metrics
    if error_type is not None:
        values["error_type"] = error_type
    if error_message is not None:
        values["error_message"] = error_message[:1024]
    if retryable is not None:
        values["retryable"] = retryable
    result = await db.execute(
        sa_update(GenerationJob)
        .where(
            GenerationJob.job_id == job_id,
            GenerationJob.status == "processing",
            GenerationJob.lease_token == lease_token,
        )
        .values(**values)
    )
    await db.flush()
    return result.rowcount == 1


async def cancel_generation_job(db, job_id: str, *, actor_user_id: str) -> bool:
    job = (
        await db.execute(
            select(GenerationJob)
            .where(GenerationJob.job_id == job_id)
            .options(noload(GenerationJob.material))
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        raise NotFoundError("任务不存在")
    if not job.owner_user_id:
        raise ForbiddenError("无权取消该任务")
    if job.owner_user_id and job.owner_user_id != actor_user_id:
        raise ForbiddenError("无权取消该任务")
    if job.status in _TERMINAL_STATUSES:
        raise ConflictError("任务已终态，不可取消")
    reservation_id = (job.input_json or {}).get("reservation_id")
    if reservation_id:
        await credits_release(
            db,
            reservation_id=reservation_id,
            reason="generation canceled",
            actor_user_id=actor_user_id,
            commit=False,
        )
    job.status = "canceled"
    job.canceled_at = datetime.now(UTC)
    job.finished_at = job.canceled_at
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    await db.flush()
    return True


def validate_image_result(result: ImageResult) -> ValidatedImage:
    """解码图片真实内容并以内容类型为准。"""
    if not result or not result.data:
        raise ValueError("Provider 返回空图片")
    try:
        with Image.open(BytesIO(result.data)) as image:
            image.verify()
            fmt = (image.format or "").upper()
    except Exception as exc:
        raise ValueError("Provider 返回的图片无法解码") from exc
    mapping = {
        "PNG": ("image/png", "png"),
        "JPEG": ("image/jpeg", "jpg"),
        "WEBP": ("image/webp", "webp"),
    }
    if fmt not in mapping:
        raise ValueError("Provider 返回了不支持的图片格式")
    mime_type, extension = mapping[fmt]
    return ValidatedImage(data=result.data, mime_type=mime_type, extension=extension)


def ensure_provider_result_count(results: list[ImageResult], *, expected: int) -> None:
    """允许 Provider 少图成功，但拒绝空结果和超出请求数的异常结果。"""
    if not results:
        raise ValueError("Provider 未返回图片")
    if len(results) > expected:
        raise ValueError("Provider 返回图片数量超过请求数")


async def _persist_validated_image(
    material_id: str, job_id: str, lease_token: str, image: ValidatedImage, storage: Any
) -> PersistedImage:
    file_id = new_id()
    final_key = f"generate_images/{material_id}/{file_id}.{image.extension}"
    temp_key = f"generate_images/{material_id}/tmp/{job_id}/{lease_token}/{file_id}.{image.extension}"
    await storage.put(temp_key, image.data)
    return PersistedImage(
        temp_key=temp_key,
        final_key=final_key,
        sha256=hashlib.sha256(image.data).hexdigest(),
        image=image,
    )


async def _promote_generated_objects(storage: Any, generated: list[PersistedImage]) -> None:
    for item in generated:
        if hasattr(storage, "promote"):
            await storage.promote(item.temp_key, item.final_key)
        else:
            await storage.put(item.final_key, item.image.data)
            if hasattr(storage, "delete"):
                await storage.delete(item.temp_key)


async def _delete_generated_objects(
    storage: Any, generated: list[PersistedImage], db: Any | None = None
) -> None:
    for item in generated:
        for storage_key in (item.temp_key, item.final_key):
            try:
                await storage.delete(storage_key)
            except Exception:
                logger.warning("清理生成对象失败")
                if db is not None:
                    try:
                        async with async_session_factory() as compensation_db:
                            await enqueue_delete(
                                compensation_db,
                                idempotency_key=f"generation-delete:{storage_key}",
                                source_key=storage_key,
                            )
                            await compensation_db.commit()
                    except Exception:
                        logger.exception("写入生成对象删除补偿失败")


async def _heartbeat_or_stop(db: Any, job_id: str, lease_token: str) -> bool:
    if not await heartbeat_generation_job(db, job_id, lease_token):
        await db.rollback()
        return False
    await db.commit()
    return True


async def _provider_generate_with_heartbeat(
    provider: Any,
    *,
    prompt: str,
    model: str,
    size: str,
    aspect: str,
    count: int,
    job_id: str,
    lease_token: str,
    heartbeat_interval: float = 60.0,
) -> list[Any]:
    lost = asyncio.Event()
    provider_task = asyncio.create_task(
        provider.generate(prompt=prompt, model=model, size=size, aspect=aspect, count=count)
    )

    async def heartbeat_loop() -> None:
        try:
            while not provider_task.done():
                await asyncio.sleep(heartbeat_interval)
                if provider_task.done():
                    return
                async with async_session_factory() as heartbeat_db:
                    ok = await heartbeat_generation_job(
                        heartbeat_db, job_id, lease_token
                    )
                    if ok:
                        await heartbeat_db.commit()
                        continue
                    await heartbeat_db.rollback()
                    lost.set()
                    provider_task.cancel()
                    return
        except asyncio.CancelledError:
            return

    heartbeat_task = asyncio.create_task(heartbeat_loop())
    try:
        results = await provider_task
        if lost.is_set():
            return []
        return results
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


async def _input_version_is_current(db: Any, job: GenerationJob) -> bool:
    input_json = job.input_json or {}
    subject = await _load_subject_for_candidate(db, job.subject_id)
    if subject is None:
        return False
    expected_subject_version = input_json.get("subject_version")
    if (
        expected_subject_version is not None
        and int(subject.subject_version or 1) != int(expected_subject_version)
    ):
        return False
    expected_enrich_version = input_json.get("enrich_version")
    if (
        expected_enrich_version is not None
        and int(subject.enrich_version or 1) != int(expected_enrich_version)
    ):
        return False
    episode_snapshot = input_json.get("episode_snapshot") or {}
    if episode_snapshot:
        normalized_snapshot = {int(key): value for key, value in episode_snapshot.items()}
        episode_ids = [str(item["episode_id"]) for item in normalized_snapshot.values()]
        current_rows = list(
            (
                await db.execute(
                    select(Episode)
                    .where(
                        Episode.script_id == job.script_id,
                        Episode.episode_id.in_(episode_ids),
                    )
                    .options(noload(Episode.script), noload(Episode.scenes))
                )
            )
            .scalars()
            .all()
        )
        current_snapshot = {
            int(episode.episode_no): {
                "episode_id": episode.episode_id,
                "episode_version": int(episode.episode_version or 1),
                "client_save_seq": int(episode.client_save_seq or 0),
            }
            for episode in current_rows
        }
        return current_snapshot == normalized_snapshot
    stmt = select(Episode).options(noload(Episode.script), noload(Episode.scenes))
    if job.episode_id:
        stmt = stmt.where(Episode.episode_id == job.episode_id)
    else:
        episode_nos = subject.episode_nos if subject else None
        if not episode_nos:
            return True
        stmt = stmt.where(
            Episode.script_id == job.script_id,
            Episode.episode_no == sorted(int(no) for no in episode_nos)[0],
        )
    episode = (await db.execute(stmt.limit(1))).scalar_one_or_none()
    if episode is None:
        return False
    if int(episode.episode_version or 1) != job.input_version:
        return False
    expected_client_save_seq = input_json.get("input_client_save_seq")
    return (
        expected_client_save_seq is None
        or int(episode.client_save_seq or 0) == int(expected_client_save_seq)
    )


async def _load_subject_for_candidate(db: Any, subject_id: str | None) -> Subject | None:
    if not subject_id:
        return None
    return (
        await db.execute(
            select(Subject)
            .where(Subject.id == subject_id)
            .options(
                noload(Subject.script),
                noload(Subject.parent_subject),
                noload(Subject.asset_entries),
                noload(Subject.image_entry),
            )
        )
    ).scalar_one_or_none()


async def _job_token_is_current(db: Any, job_id: str, lease_token: str) -> bool:
    job = await _get_generation_job(db, job_id)
    return bool(
        job
        and job.status == "processing"
        and job.lease_token == lease_token
    )


async def _lock_current_generation_job(
    db: Any, job_id: str, lease_token: str
) -> GenerationJob | None:
    """先锁任务行并校验租约，统一所有积分终态路径的锁序。"""
    return (
        await db.execute(
            select(GenerationJob)
            .where(
                GenerationJob.job_id == job_id,
                GenerationJob.status == "processing",
                GenerationJob.lease_token == lease_token,
            )
            .options(noload(GenerationJob.material))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _fail_generation_with_release(
    db: Any,
    job_id: str,
    lease_token: str,
    reservation_id: str | None,
    *,
    error_type: str,
    retryable: bool,
) -> bool:
    if await _lock_current_generation_job(db, job_id, lease_token) is None:
        await db.rollback()
        return False
    release_failed = False
    if reservation_id:
        try:
            await credits_release(
                db,
                reservation_id=reservation_id,
                reason=error_type,
                commit=False,
            )
        except Exception:
            await db.rollback()
            if await _lock_current_generation_job(db, job_id, lease_token) is None:
                await db.rollback()
                return False
            release_failed = True
    final_error_type = "financial_cleanup" if release_failed else error_type
    metrics = None
    if release_failed:
        metrics = {
            "financial_cleanup": {
                "action": "release",
                "reservation_id": reservation_id,
                "actual_points": None,
            }
        }
    completed = await finish_generation_job(
        db,
        job_id,
        lease_token,
        "failed",
        error_type=final_error_type,
        error_message=final_error_type,
        retryable=retryable or release_failed,
        metrics=metrics,
    )
    if not completed:
        await db.rollback()
        return False
    await db.commit()
    return True


async def _mark_financial_cleanup(
    db: Any,
    job_id: str,
    lease_token: str,
    *,
    action: str,
    reservation_id: str | None,
    actual_points: int | None,
) -> bool:
    await db.rollback()
    if await _lock_current_generation_job(db, job_id, lease_token) is None:
        await db.rollback()
        return False
    completed = await finish_generation_job(
        db,
        job_id,
        lease_token,
        "failed",
        error_type="financial_cleanup",
        error_message="financial_cleanup",
        retryable=True,
        metrics={
            "financial_cleanup": {
                "action": action,
                "reservation_id": reservation_id,
                "actual_points": actual_points,
            }
        },
    )
    if not completed:
        await db.rollback()
        return False
    await db.commit()
    return True


async def run_generation_once(
    db,
    job_id: str,
    *,
    owner: str,
    provider: Any | None = None,
    storage: Any | None = None,
) -> None:
    """执行一次生图任务；测试可注入 fake provider/storage。"""
    claim = await claim_generation_job(db, job_id, owner)
    await db.commit()
    if claim is None:
        return
    job = await _get_generation_job(db, job_id)
    if job is None:
        return
    input_json = job.input_json or {}
    reservation_id = input_json.get("reservation_id")
    if not await _input_version_is_current(db, job):
        try:
            if await _lock_current_generation_job(db, job_id, claim.lease_token) is None:
                await db.rollback()
                return
            if reservation_id:
                await credits_release(
                    db,
                    reservation_id=reservation_id,
                    reason="superseded_input",
                    commit=False,
                )
            if not await finish_generation_job(
                db, job_id, claim.lease_token, "canceled"
            ):
                await db.rollback()
                return
            await db.commit()
        except Exception:
            await db.rollback()
            await _fail_generation_with_release(
                db,
                job_id,
                claim.lease_token,
                reservation_id,
                error_type="financial_cleanup",
                retryable=True,
            )
        return

    prompt = (input_json.get("resolved_prompts") or {}).get("final") or input_json.get("prompt") or ""
    model = input_json.get("model", "banana-2")
    size = input_json.get("size", "2k")
    aspect = input_json.get("aspect", "16:9")
    count = int(input_json.get("count", 1) or 1)
    subject_id = input_json.get("subject_id") or job.subject_id
    material_id = job.material_id
    script_id = await _resolve_script_id(db, job)
    if not material_id or not script_id or not subject_id:
        await _fail_generation_with_release(
            db,
            job_id,
            claim.lease_token,
            reservation_id,
            error_type="invalid_job_input",
            retryable=False,
        )
        return
    provider = provider or get_image_provider(model)
    storage = storage or get_storage()
    generated: list[PersistedImage] = []
    try:
        if not await _job_token_is_current(db, job_id, claim.lease_token):
            return
        if not await _heartbeat_or_stop(db, job_id, claim.lease_token):
            return
        raw_results = await _provider_generate_with_heartbeat(
            provider,
            prompt=prompt,
            model=model,
            size=size,
            aspect=aspect,
            count=count,
            job_id=job_id,
            lease_token=claim.lease_token,
        )
        if not await _heartbeat_or_stop(db, job_id, claim.lease_token):
            return
        normalized = [
            item
            if isinstance(item, ImageResult)
            else ImageResult(data=item, mime_type="image/png")
            for item in raw_results
        ]
        ensure_provider_result_count(normalized, expected=count)
        validated = [validate_image_result(item) for item in normalized]
        for image in validated:
            if not await _input_version_is_current(db, job) or not await _job_token_is_current(
                db, job_id, claim.lease_token
            ):
                await db.rollback()
                await _delete_generated_objects(storage, generated, db)
                return
            persisted = await _persist_validated_image(
                material_id, job_id, claim.lease_token, image, storage
            )
            generated.append(persisted)

        image_outputs: list[dict] = []
        for item in generated:
            if not await _input_version_is_current(db, job) or not await _job_token_is_current(
                db, job_id, claim.lease_token
            ):
                await db.rollback()
                await _delete_generated_objects(storage, generated, db)
                return
            asset = await add_candidate(
                db,
                script_id=script_id,
                subject_id=subject_id,
                kind="image",
                storage_key=item.final_key,
                mime_type=item.image.mime_type,
                size_bytes=len(item.image.data),
                source="generate_images",
                sha256=item.sha256,
                subject=await _load_subject_for_candidate(db, subject_id),
            )
            image_outputs.append(
                {"status": "succeeded", "assetId": asset.id, "storageKey": item.final_key}
            )
        actual = settings.price_image_gen * len(image_outputs)
        if await _lock_current_generation_job(db, job_id, claim.lease_token) is None:
            await db.rollback()
            await _delete_generated_objects(storage, generated, db)
            return
        if reservation_id:
            try:
                await credits_settle(
                    db, reservation_id=reservation_id, actual=actual, commit=False
                )
            except Exception:
                await _delete_generated_objects(storage, generated, db)
                await _mark_financial_cleanup(
                    db,
                    job_id,
                    claim.lease_token,
                    action="settle",
                    reservation_id=reservation_id,
                    actual_points=actual,
                )
                return
        if not await finish_generation_job(
            db,
            job_id,
            claim.lease_token,
            "succeeded",
            output_json={"imageOutputs": image_outputs, "primaryImageIndex": 0},
            metrics={"actual_points": actual, "count": len(image_outputs)},
        ):
            await db.rollback()
            await _delete_generated_objects(storage, generated, db)
            return
        try:
            await _promote_generated_objects(storage, generated)
            await db.commit()
        except Exception:
            await db.rollback()
            await _delete_generated_objects(storage, generated, db)
            await _fail_generation_with_release(
                db,
                job_id,
                claim.lease_token,
                reservation_id,
                error_type="provider_or_storage_error",
                retryable=False,
            )
    except Exception:
        await db.rollback()
        await _delete_generated_objects(storage, generated, db)
        await _fail_generation_with_release(
            db,
            job_id,
            claim.lease_token,
            reservation_id,
            error_type="provider_or_storage_error",
            retryable=False,
        )


async def reconcile_generation_jobs(db, *, limit: int = 100) -> int:
    """执行失败后处于 financial_cleanup 的任务，供后续 beat 接线。"""
    rows = list(
        (
            await db.execute(
                select(GenerationJob)
                .where(
                    GenerationJob.status == "failed",
                    GenerationJob.error_type == "financial_cleanup",
                    GenerationJob.retryable.is_(True),
                )
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    recovered = 0
    for job in rows:
        try:
            cleanup = (job.metrics or {}).get("financial_cleanup") or {}
            action = cleanup.get("action") or "release"
            reservation_id = cleanup.get("reservation_id") or (job.input_json or {}).get("reservation_id")
            if not reservation_id:
                job.retryable = False
                await db.commit()
                recovered += 1
                continue
            if job.status != "failed" or job.error_type != "financial_cleanup":
                await db.rollback()
                continue
            await credits_release(
                db,
                reservation_id=reservation_id,
                reason=f"generation financial cleanup: {action}",
                commit=False,
            )
            job.retryable = False
            await db.commit()
            recovered += 1
        except Exception:
            logger.exception("generation financial cleanup reconcile failed")
            await db.rollback()
    return recovered


async def reconcile_stale_generation_jobs(db, *, limit: int = 100) -> int:
    """恢复租约过期的 processing 任务，使 outbox 重投递后可重新领取。"""
    now = datetime.now(UTC)
    rows = list(
        (
            await db.execute(
                select(GenerationJob)
                .where(
                    GenerationJob.status == "processing",
                    GenerationJob.lease_until.is_not(None),
                    GenerationJob.lease_until <= now,
                )
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    recovered = 0
    for job in rows:
        old_token = job.lease_token
        result = await db.execute(
            sa_update(GenerationJob)
            .where(
                GenerationJob.job_id == job.job_id,
                GenerationJob.status == "processing",
                GenerationJob.lease_token == old_token,
                GenerationJob.lease_until <= now,
            )
            .values(
                status="starting",
                progress_stage="starting",
                progress_message="等待重新生成",
                lease_owner=None,
                lease_token=None,
                lease_until=None,
            )
            .execution_options(synchronize_session=False)
        )
        if result.rowcount != 1:
            continue
        outbox = (
            await db.execute(
                select(TaskDispatchOutbox)
                .where(
                    TaskDispatchOutbox.task_type == "generation.generate_images",
                    TaskDispatchOutbox.job_id == job.job_id,
                    TaskDispatchOutbox.attempt == job.attempt,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if outbox is None:
            outbox = TaskDispatchOutbox(
                task_type="generation.generate_images",
                job_id=job.job_id,
                attempt=job.attempt,
                payload={
                    "job_id": job.job_id,
                    "script_id": job.script_id,
                    "subject_id": job.subject_id,
                    "input_version": job.input_version,
                    "dispatch_id": new_id(),
                    "run_id": job.job_id,
                },
                status="pending",
                available_at=now,
            )
            db.add(outbox)
        else:
            outbox.status = "pending"
            outbox.available_at = now
            outbox.published_at = None
            outbox.lease_owner = None
            outbox.lease_token = None
            outbox.lease_until = None
        recovered += 1
    await db.commit()
    return recovered


async def _persist_image(material_id: str, data: bytes) -> tuple[str, str]:
    """将生成图片经 StorageProvider 写入存储，返回 (storage_key, sha256)。

    storage_key 形如 /generate_images/<mid>/<file_id>.png，与 sapi 代理路径一致。
    统一走 get_storage() 抽象（与 video_generation/timeline_render worker 一致），
    local 写本地磁盘、s3 写对象存储，S3 模式下 media 代理经同一 provider 读取不会 404。
    """
    file_id = new_id()
    storage_key = f"/generate_images/{material_id}/{file_id}.png"
    storage = get_storage()
    await storage.put(storage_key, data)
    sha256 = hashlib.sha256(data).hexdigest()
    return storage_key, sha256


async def _remove_persisted(storage_key: str) -> None:
    """删除已落盘的生成图片（失败回滚清理，避免孤儿文件）。"""
    try:
        storage = get_storage()
        if await storage.exists(storage_key):
            await storage.delete(storage_key)
    except Exception:
        logger.warning("清理生成图片失败: %s", storage_key, exc_info=True)


async def _resolve_script_id(db: Any, job: Any) -> str | None:
    """解析任务所属 script_id —— job.script_id 优先，兜底从 material.script_id 查询。

    job.script_id 可能为 None（历史数据或未回填），需从 material.script_id 兜底，
    否则 add_candidate 缺少必填的 script_id 位置参数会抛 TypeError。
    """
    script_id = job.script_id
    if script_id:
        return script_id
    if not job.material_id:
        return None
    result = await db.execute(
        select(Materials.script_id).where(Materials.id == job.material_id)
    )
    return result.scalar_one_or_none()


async def _lookup_reservation_id(db: Any, job_id: str) -> str | None:
    """按 (source_type='generation_job', source_id=job_id) 反查 reservation_id。

    优先从 job.input_json.reservation_id 读取（service 创建时写入），
    缺失时按幂等键反查 CreditReservation，兼容历史数据。
    """
    job = await get_job(db, job_id)
    if job and job.input_json:
        rid = (job.input_json or {}).get("reservation_id")
        if rid:
            return str(rid)
    result = await db.execute(
        select(CreditReservation.id).where(
            CreditReservation.source_type == "generation_job",
            CreditReservation.source_id == job_id,
        )
    )
    return result.scalar_one_or_none()


async def _lookup_actor(
    db: Any, reservation_id: str | None
) -> tuple[str | None, str | None]:
    """从 reserve 流水取操作者(actor_user_id/actor_username)。

    reserve 在 service 创建任务时已传 actor_user_id=user.user_id 写入 CreditLog，
    worker 的 settle/release 须沿用同一操作者，满足 08 §4.1「积分变更须有
    操作者/时间审计」要求（worker 内无法直接拿到发起用户，故从 reserve 流水反查）。
    """
    if not reservation_id:
        return None, None
    result = await db.execute(
        select(CreditLog.actor_user_id, CreditLog.actor_username).where(
            CreditLog.reservation_id == reservation_id,
            CreditLog.log_type == "reserve",
        ).limit(1)
    )
    row = result.first()
    if row is None:
        return None, None
    return row[0], row[1]


async def _set_subject_review_status(
    db: Any, subject_id: str | None, status: str
) -> None:
    """更新主体审核状态（生成成功 -> pending 候选待审；失败 -> not_designed）。"""
    if not subject_id:
        return
    await db.execute(
        sa_update(Subject).where(Subject.id == subject_id).values(review_status=status)
    )


async def _revert_subject_on_failure(
    db: Any, subject_id: str | None
) -> None:
    """生成失败时回退主体审核状态。

    仅对 review_status=='not_designed' 的主体回退（实际为 no-op，保留原状态）；
    pending（存在候选图，08 §2.3 CandidateReady 应持续存在）与 approved（已通过
    正式图）一律保留，不因本次重新生成失败而丢失既有候选/正式状态。
    与成功路径 mark_pending_if_has_candidates（仅 not_designed->pending）形成对称。
    """
    if not subject_id:
        return
    # 仅 not_designed 可回退（此处为 no-op，语义上保持未设计）；
    # pending/approved 保留，避免丢失既有候选图或已通过状态。
    await db.execute(
        sa_update(Subject)
        .where(
            Subject.id == subject_id,
            Subject.review_status == "not_designed",
        )
        .values(review_status="not_designed")
    )


async def _run_generation(job_id: str) -> None:
    """旧执行路径已禁用；真实 Celery 入口只能走 run_generation_once。"""
    raise RuntimeError("legacy_generation_runner_disabled")


@celery_app.task(
    name="app.workers.generation.generate_images_task",
    bind=True,
    max_retries=0,
)
def generate_images_task(self, job_id: str) -> None:
    """文生图 Celery 任务入口 — 在独立事件循环中执行异步主体。"""
    async def _run() -> None:
        async with async_session_factory() as db:
            await run_generation_once(
                db,
                job_id,
                owner="celery:generation.generate_images",
            )

    asyncio.run(_run())


@celery_app.task(
    name="generation.reconcile-financial-cleanup",
    bind=True,
    max_retries=0,
)
def reconcile_generation_financial_cleanup_task(self) -> int:
    async def _run() -> int:
        async with async_session_factory() as db:
            return await reconcile_generation_jobs(db)

    return asyncio.run(_run())


@celery_app.task(
    name="generation.reconcile-stale",
    bind=True,
    max_retries=0,
)
def reconcile_generation_stale_task(self) -> int:
    async def _run() -> int:
        async with async_session_factory() as db:
            return await reconcile_stale_generation_jobs(db)

    return asyncio.run(_run())
