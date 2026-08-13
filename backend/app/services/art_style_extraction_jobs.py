"""参考图画风提取异步任务服务。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import BadRequestError, ConflictError, NotFoundError
from app.core.ids import new_id
from app.models.art_style import ArtStyle
from app.models.art_style_extraction_job import ArtStyleExtractionJob
from app.models.asset_entry import AssetEntry
from app.providers.storage import StorageProvider, get_storage, validate_key_prefix
from app.providers.vision_style import VisionStyleProvider, get_vision_style_provider
from app.services.task_dispatch import enqueue_task

ART_STYLE_EXTRACTION_TASK_TYPE = "art_style.extract"
_MAX_STYLE_IMAGE_BYTES = 20 * 1024 * 1024
_LEASE_SECONDS = 15 * 60
_SAFE_PROVIDER_ERROR = "画风提取失败，请使用新请求重试"
_SAFE_INVALID_RESPONSE_ERROR = "画风提取结果格式不符合要求，请使用新请求重试"


@dataclass(frozen=True)
class ArtStyleExtractionClaim:
    job_id: str
    attempt: int
    lease_token: str
    script_id: str
    actor_user_id: str
    name: str
    input_snapshot: list[dict]
    input_version_hash: str


def _stable_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _validate_extracted_payload(extracted: dict) -> tuple[str, str, str, str | None]:
    prompt_zh = extracted.get("prompt_zh")
    prompt_en = extracted.get("prompt_en")
    negative_prompt = extracted.get("negative_prompt", "")
    visual_style = extracted.get("visual_style")
    if not isinstance(prompt_zh, str) or not prompt_zh.strip():
        raise ValueError("missing_prompt_zh")
    if not isinstance(prompt_en, str) or not prompt_en.strip():
        raise ValueError("missing_prompt_en")
    if not isinstance(negative_prompt, str):
        raise ValueError("invalid_negative_prompt")
    if visual_style not in {None, "live_action", "anime"}:
        raise ValueError("invalid_visual_style")
    return prompt_zh.strip(), prompt_en.strip(), negative_prompt.strip(), visual_style


async def queue_art_style_extraction(
    db: AsyncSession,
    *,
    script_id: str,
    name: str,
    asset_ids: list[str],
    actor_user_id: str,
    idempotency_key: str,
    storage: StorageProvider | None = None,
) -> ArtStyleExtractionJob:
    """校验并冻结参考图，在同一事务中持久化 Job 与 Outbox。"""

    normalized_key = idempotency_key.strip()
    if not normalized_key:
        raise BadRequestError("Idempotency-Key 必填")
    if normalized_key != idempotency_key or len(normalized_key) > 128:
        raise BadRequestError("Idempotency-Key 格式无效或过长")
    normalized_name = name.strip()
    unique_asset_ids = list(dict.fromkeys(asset_ids))
    if not normalized_name or not unique_asset_ids:
        raise BadRequestError("画风名称和参考图不能为空")
    request_hash = _stable_hash({"name": normalized_name, "asset_ids": unique_asset_ids})
    existing = (
        await db.execute(
            select(ArtStyleExtractionJob).where(
                ArtStyleExtractionJob.script_id == script_id,
                ArtStyleExtractionJob.actor_user_id == actor_user_id,
                ArtStyleExtractionJob.idempotency_key == normalized_key,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.request_hash != request_hash:
            raise ConflictError("Idempotency-Key 已用于不同的画风提取请求")
        return existing

    duplicate_style = (
        await db.execute(
            select(ArtStyle.id).where(ArtStyle.script_id == script_id, ArtStyle.name == normalized_name)
        )
    ).scalar_one_or_none()
    if duplicate_style is not None:
        raise ConflictError("同名画风已存在")

    assets = list(
        (
            await db.execute(
                select(AssetEntry).where(
                    AssetEntry.id.in_(unique_asset_ids),
                    AssetEntry.script_id == script_id,
                    AssetEntry.kind == "image",
                    AssetEntry.status.in_(["candidate", "approved"]),
                )
            )
        ).scalars().all()
    )
    by_id = {asset.id: asset for asset in assets}
    if set(unique_asset_ids) != set(by_id):
        raise BadRequestError("参考图不存在、不属于当前剧本或不是有效图片")

    resolved_storage = storage or get_storage()
    snapshot: list[dict] = []
    for asset_id in unique_asset_ids:
        asset = by_id[asset_id]
        if not asset.mime_type.startswith("image/") or asset.size_bytes > _MAX_STYLE_IMAGE_BYTES:
            raise BadRequestError("参考图格式或大小不符合画风提取要求")
        storage_key = validate_key_prefix(asset.storage_key)
        data = await resolved_storage.get(storage_key)
        if not data or len(data) > _MAX_STYLE_IMAGE_BYTES:
            raise BadRequestError("参考图内容无效或过大")
        actual_hash = hashlib.sha256(data).hexdigest()
        if asset.sha256 and asset.sha256 != actual_hash:
            raise BadRequestError("参考图内容与资产摘要不一致")
        snapshot.append(
            {
                "asset_id": asset.id,
                "storage_key": storage_key,
                "mime_type": asset.mime_type,
                "size_bytes": len(data),
                "sha256": actual_hash,
                "asset_updated_at": asset.updated_at.isoformat(),
            }
        )
    input_version_hash = _stable_hash(snapshot)
    row = ArtStyleExtractionJob(
        id=new_id(),
        script_id=script_id,
        actor_user_id=actor_user_id,
        idempotency_key=normalized_key,
        request_hash=request_hash,
        name=normalized_name,
        source_asset_ids=unique_asset_ids,
        source_hashes=[item["sha256"] for item in snapshot],
        input_snapshot=snapshot,
        input_version_hash=input_version_hash,
        status="queued",
        attempt_count=1,
        billing_mode="unmetered",
    )
    try:
        async with db.begin_nested():
            db.add(row)
            await db.flush()
    except IntegrityError:
        raced = (
            await db.execute(
                select(ArtStyleExtractionJob).where(
                    ArtStyleExtractionJob.script_id == script_id,
                    ArtStyleExtractionJob.actor_user_id == actor_user_id,
                    ArtStyleExtractionJob.idempotency_key == normalized_key,
                )
            )
        ).scalar_one_or_none()
        if raced is not None:
            if raced.request_hash != request_hash:
                raise ConflictError("Idempotency-Key 已用于不同的画风提取请求") from None
            return raced
        raise ConflictError("同名画风正在提取中") from None

    await enqueue_task(
        db,
        task_type=ART_STYLE_EXTRACTION_TASK_TYPE,
        job_id=row.id,
        attempt=1,
        payload={"job_id": row.id, "attempt": 1},
    )
    return row


async def get_art_style_extraction_job(
    db: AsyncSession,
    *,
    script_id: str,
    job_id: str,
) -> ArtStyleExtractionJob:
    row = (
        await db.execute(
            select(ArtStyleExtractionJob).where(
                ArtStyleExtractionJob.id == job_id,
                ArtStyleExtractionJob.script_id == script_id,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise NotFoundError("画风提取任务不存在")
    return row


async def claim_art_style_extraction(
    db: AsyncSession,
    *,
    job_id: str,
    expected_attempt: int,
    owner: str,
    lease_seconds: int = _LEASE_SECONDS,
) -> ArtStyleExtractionClaim | None:
    """以行锁和租约令牌领取任务；过期 running 不会再次调用模型。"""

    row = (
        await db.execute(
            select(ArtStyleExtractionJob)
            .where(ArtStyleExtractionJob.id == job_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if row is None or row.status != "queued" or row.attempt_count != expected_attempt:
        return None
    now = datetime.now(UTC)
    token = new_id()
    row.status = "running"
    row.lease_owner = owner
    row.lease_token = token
    row.leased_until = now + timedelta(seconds=lease_seconds)
    row.heartbeat_at = now
    row.started_at = now
    row.error_code = None
    row.error_message = None
    await db.flush()
    return ArtStyleExtractionClaim(
        job_id=row.id,
        attempt=row.attempt_count,
        lease_token=token,
        script_id=row.script_id,
        actor_user_id=row.actor_user_id,
        name=row.name,
        input_snapshot=list(row.input_snapshot),
        input_version_hash=row.input_version_hash,
    )


async def load_frozen_images(
    claim: ArtStyleExtractionClaim,
    *,
    storage: StorageProvider | None = None,
) -> list[tuple[bytes, str]]:
    """从冻结快照读取图片，并在模型调用前复核内容摘要。"""

    if _stable_hash(claim.input_snapshot) != claim.input_version_hash:
        raise ValueError("input_snapshot_changed")
    resolved_storage = storage or get_storage()
    images: list[tuple[bytes, str]] = []
    for item in claim.input_snapshot:
        data = await resolved_storage.get(validate_key_prefix(str(item["storage_key"])))
        if (
            not data
            or len(data) != int(item["size_bytes"])
            or len(data) > _MAX_STYLE_IMAGE_BYTES
            or hashlib.sha256(data).hexdigest() != item["sha256"]
        ):
            raise ValueError("frozen_asset_changed")
        mime_type = str(item["mime_type"])
        if not mime_type.startswith("image/"):
            raise ValueError("invalid_mime_type")
        images.append((data, mime_type))
    return images


async def complete_art_style_extraction(
    db: AsyncSession,
    *,
    claim: ArtStyleExtractionClaim,
    extracted: dict,
    provider_name: str,
    model_name: str,
) -> ArtStyleExtractionJob | None:
    """使用 attempt + lease token 栅栏原子创建 ArtStyle 并完成任务。"""

    row = (
        await db.execute(
            select(ArtStyleExtractionJob)
            .where(ArtStyleExtractionJob.id == claim.job_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        row is None
        or row.status != "running"
        or row.attempt_count != claim.attempt
        or row.lease_token != claim.lease_token
    ):
        return row
    try:
        prompt_zh, prompt_en, negative_prompt, visual_style = _validate_extracted_payload(extracted)
    except ValueError:
        await _fail_locked_job(
            row,
            error_code="invalid_provider_response",
            error_message=_SAFE_INVALID_RESPONSE_ERROR,
        )
        await db.flush()
        return row
    style = ArtStyle(
        id=new_id(),
        script_id=row.script_id,
        name=row.name,
        prompt_zh=prompt_zh,
        prompt_en=prompt_en,
        negative_prompt=negative_prompt,
        visual_style=visual_style,
        source_asset_ids=list(row.source_asset_ids),
        source_hashes=list(row.source_hashes),
        thumbnail_storage_key=str(row.input_snapshot[0]["storage_key"]),
        extraction_provider=provider_name,
        extraction_model=model_name,
        extraction_version=1,
        is_active=True,
        created_by_user_id=row.actor_user_id,
        updated_by_user_id=row.actor_user_id,
    )
    try:
        async with db.begin_nested():
            db.add(style)
            await db.flush()
    except IntegrityError:
        await db.refresh(row)
        await _fail_locked_job(
            row,
            error_code="style_name_conflict",
            error_message="同名画风已存在，请更换名称后重试",
        )
        await db.flush()
        return row
    now = datetime.now(UTC)
    row.status = "succeeded"
    row.art_style_id = style.id
    row.extraction_provider = provider_name
    row.extraction_model = model_name
    row.finished_at = now
    row.lease_owner = None
    row.lease_token = None
    row.leased_until = None
    row.heartbeat_at = now
    await db.flush()
    return row


async def mark_art_style_extraction_failed(
    db: AsyncSession,
    *,
    claim: ArtStyleExtractionClaim,
    error_code: str,
    error_message: str,
) -> bool:
    row = (
        await db.execute(
            select(ArtStyleExtractionJob)
            .where(ArtStyleExtractionJob.id == claim.job_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        row is None
        or row.status != "running"
        or row.attempt_count != claim.attempt
        or row.lease_token != claim.lease_token
    ):
        return False
    await _fail_locked_job(row, error_code=error_code, error_message=error_message)
    await db.flush()
    return True


async def _fail_locked_job(
    row: ArtStyleExtractionJob,
    *,
    error_code: str,
    error_message: str,
) -> None:
    now = datetime.now(UTC)
    row.status = "failed"
    row.error_code = error_code
    row.error_message = error_message
    row.finished_at = now
    row.lease_owner = None
    row.lease_token = None
    row.leased_until = None
    row.heartbeat_at = now


async def fail_expired_art_style_extractions(
    db: AsyncSession,
    *,
    limit: int = 100,
) -> int:
    """将失联 running 安全终止，避免未知完成状态下重复调用供应商。"""

    now = datetime.now(UTC)
    rows = list(
        (
            await db.execute(
                select(ArtStyleExtractionJob)
                .where(
                    ArtStyleExtractionJob.status == "running",
                    ArtStyleExtractionJob.leased_until.is_not(None),
                    ArtStyleExtractionJob.leased_until <= now,
                )
                .order_by(ArtStyleExtractionJob.leased_until.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).scalars().all()
    )
    for row in rows:
        await _fail_locked_job(
            row,
            error_code="worker_timeout",
            error_message="画风提取任务执行超时，请使用新请求重试",
        )
    await db.flush()
    return len(rows)


async def execute_claimed_art_style_extraction(
    db: AsyncSession,
    *,
    claim: ArtStyleExtractionClaim,
    provider: VisionStyleProvider | None = None,
    storage: StorageProvider | None = None,
) -> ArtStyleExtractionJob | None:
    """执行已领取任务；所有外部错误统一降噪，任务明确不计费。"""

    try:
        images = await load_frozen_images(claim, storage=storage)
    except Exception:  # noqa: BLE001 - 存储实现正文不得进入错误契约
        await mark_art_style_extraction_failed(
            db,
            claim=claim,
            error_code="source_asset_changed",
            error_message="参考图已变化或不可读取，请使用新请求重试",
        )
        return await db.get(ArtStyleExtractionJob, claim.job_id)
    try:
        resolved_provider = provider or get_vision_style_provider()
        extracted = await resolved_provider.extract_style(images)
    except Exception:  # noqa: BLE001 - Provider 原始正文不得写库或返回给客户端
        await mark_art_style_extraction_failed(
            db,
            claim=claim,
            error_code="provider_failed",
            error_message=_SAFE_PROVIDER_ERROR,
        )
        return await db.get(ArtStyleExtractionJob, claim.job_id)
    return await complete_art_style_extraction(
        db,
        claim=claim,
        extracted=extracted,
        provider_name=resolved_provider.provider_name,
        model_name=resolved_provider.model_name,
    )
