"""视频生成编排服务 — create_video_job。

流程:
1. 幂等去重（Idempotency-Key 或同 shot_group 活动任务复用，doc 08 §4.2）
2. 取 shot_group，校验存在与归属
3. aggregate_references 汇聚参考图 + 参考音频（audio_elements，doc 06 §9.6）
4. 首尾帧链式衔接：取上一组 last_frame → 当前组 first_frame（doc 06 §10.3）
5. 取激活供应商配置 (provider_key + model)，缺失则兜底默认
6. 落库 VideoGenerationJob（status=pending, input_json 含 model/参考图/prompt/first_frame）
7. credits.reserve (source_type='generation_job', source_id=job_id,
   estimated=settings.price_video_gen_frozen — 冻结量与实际结算分离，doc 08 §2.4)
   — reserve 失败回滚任务
8. 投递 Celery worker (run_video_generation.delay(job_id))
9. 返回 job

权限校验由 API 层在调用前完成（check_script_access）。
"""

import asyncio
import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any, cast

from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from sqlalchemy import select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.config import settings
from app.core.errors import BadRequestError, ConflictError, ForbiddenError
from app.crud import video as video_crud
from app.models.episode import Episode
from app.models.script import Script
from app.models.shot_group import ShotGroup, shot_group_content_hash
from app.models.video_generation_job import VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH
from app.providers.video_gen import (
    VideoCancellationOutcome,
    VideoCancellationResult,
    get_video_provider,
)
from app.services.reference_aggregator import aggregate_references
from app.services.reference_image_audit import (
    audit_not_required_state,
    ensure_references_approved,
    include_first_frame,
)
from app.services.runtime_models import resolve_video_model
from app.services.task_dispatch import cancel_outbox_task, enqueue_task
from app.services.video_remote_cancellation import (
    merge_pending_provider_cancellation,
    merge_video_task_checkpoint_lease_authority,
    persist_locked_provider_cancellation_result,
    persist_provider_cancellation_result,
    provider_submission_is_unresolved,
)

if TYPE_CHECKING:
    from app.models.video_generation_job import VideoGenerationJob

logger = logging.getLogger(__name__)
_IDEMPOTENCY_CONFLICT_DETAIL = "幂等键已用于不同请求载荷"

# 跨模块依赖（T2 积分服务可能未就绪，统一从 PLAN 路径 import）
try:
    from app.services.credits import reserve as _credits_reserve
except ImportError:  # pragma: no cover - T2 未就绪兜底
    logger.warning(
        "积分服务 app.services.credits 不可导入，视频生成将不冻结预扣积分 — "
        "任务可在无积分约束下进入 worker，计费被旁路。请尽快接入 T2 积分模块。"
    )
    _credits_reserve = None  # type: ignore[assignment]


async def create_video_job(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    shot_group_id: str,
    user_id: str,
    idempotency_key: str | None = None,
    prompt: str | None = None,
    style_id: str | None = None,
) -> "VideoGenerationJob":
    """创建视频生成任务 — 幂等去重 → 汇聚参考图/音频 → 首帧衔接 → 落库 → reserve 积分 → 投递 worker。

    Args:
        db: 异步会话。
        script_id: 剧本 ID。
        episode_id: 分集 ID。
        shot_group_id: 镜头组 ID（绑定生成单元）。
        user_id: 触发用户（积分扣减归属，按 user owner 维度账户）。
        idempotency_key: 可选幂等键（来自 Idempotency-Key 头）。
        prompt: 客户端显式视频提示词；缺省时回退镜头组 prompt。
        style_id: 客户端选择的风格 ID，仅记录并参与幂等判断。

    Returns:
        VideoGenerationJob ORM 对象（重复创建时返回已存在任务）。

    Raises:
        HTTPException 404: 镜头组不存在。
        ForbiddenError 403: 镜头组不属于该剧本/分集。
        InsufficientCreditsError 402: 积分不足（由 credits.reserve 抛出）。
    """
    from fastapi import HTTPException, status

    if not idempotency_key or not idempotency_key.strip():
        raise BadRequestError("Idempotency-Key 必填")
    if len(idempotency_key) > VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH:
        raise BadRequestError("Idempotency-Key 过长")
    client_prompt = prompt.strip() if isinstance(prompt, str) else None
    if prompt is not None and not client_prompt:
        raise BadRequestError("视频提示词不能为空")
    unit_id = f"group:{shot_group_id}"
    request_payload = {
        "script_id": script_id,
        "episode_id": episode_id,
        "shot_group_id": shot_group_id,
    }
    # 缺省时保持历史 hash 结构，兼容部署中的旧客户端和旧幂等绑定。
    if client_prompt is not None:
        request_payload["prompt"] = client_prompt
    if style_id is not None:
        request_payload["style_id"] = style_id
    request_hash = _request_hash(request_payload)

    # 1. 幂等去重（doc 08 §4.2）
    existing_before_lock = await _resolve_idempotent_job(
        db, script_id, idempotency_key, request_hash
    )

    # 2. 固定创建锁序为 Script -> Episode -> ShotGroup，与父资源删除保持一致。
    # SQLAlchemy 需同时设置 read/key_share，PostgreSQL 才会生成 FOR KEY SHARE。
    script_result = await db.execute(
        select(Script.id)
        .where(Script.id == script_id)
        .with_for_update(read=True, key_share=True)
    )
    locked_script_id = script_result.scalar_one_or_none()
    episode_result = await db.execute(
        select(Episode)
        .where(
            Episode.episode_id == episode_id,
            Episode.script_id == script_id,
        )
        .options(noload(Episode.script), noload(Episode.scenes))
        .with_for_update(read=True, key_share=True)
    )
    episode = episode_result.scalar_one_or_none()

    # 首次命中只用于保留幂等冲突优先级；父锁后必须重查，避免返回正被级联删除的旧任务。
    if existing_before_lock is not None:
        existing = await _resolve_idempotent_job(
            db, script_id, idempotency_key, request_hash
        )
        if existing is not None:
            logger.info(
                "视频生成任务幂等复用 job_id=%s unit_id=%s",
                existing.job_id,
                unit_id,
            )
            return existing

    # 父行锁之后再独占生成单元；不存在/跨资源错误仍以镜头组归属为先。
    sg_result = await db.execute(
        select(ShotGroup)
        .where(
            ShotGroup.shot_group_id == shot_group_id,
            ShotGroup.script_id == script_id,
            ShotGroup.episode_id == episode_id,
        )
        .with_for_update()
    )
    shot_group = sg_result.scalar_one_or_none()
    if shot_group is None:
        ownership = (
            await db.execute(
                select(ShotGroup.script_id, ShotGroup.episode_id).where(
                    ShotGroup.shot_group_id == shot_group_id
                )
            )
        ).one_or_none()
        if ownership is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="镜头组不存在")
        raise ForbiddenError("镜头组不属于该剧本/分集")
    if locked_script_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="镜头组不存在")
    if episode is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="分集不存在")
    if shot_group.unit_id != unit_id:
        raise ConflictError("请先完成镜头组合并，再创建视频生成任务")

    prompt_text = _ensure_constraint_lock(
        client_prompt if client_prompt is not None else shot_group.prompt or ""
    )

    # 等待镜头组锁期间，另一个事务可能已建立同 key 映射，锁后必须重新读取。
    existing = await _resolve_idempotent_job(
        db, script_id, idempotency_key, request_hash
    )
    if existing is not None:
        return existing

    active_job = await video_crud.find_active_job_by_unit(db, unit_id)
    if active_job is not None:
        active_input = active_job.input_json or {}
        active_prompt = active_input.get("prompt")
        if client_prompt is not None and active_prompt != prompt_text:
            raise ConflictError("该镜头组已有使用不同提示词的生成任务，请等待完成或取消后重试")
        if style_id is not None and active_input.get("style_id") != style_id:
            raise ConflictError("该镜头组已有使用不同风格的生成任务，请等待完成或取消后重试")
        logger.info(
            "视频生成任务活动单元复用 job_id=%s unit_id=%s",
            active_job.job_id,
            unit_id,
        )
        return await _bind_existing_job(
            db,
            script_id=script_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            job=active_job,
        )

    # 3. 汇聚参考图 + 参考音频
    aggregated = await aggregate_references(db, shot_group_id)

    # 4. 首尾帧链式衔接：取上一组 last_frame 作为当前组首帧参考（doc 06 §10.3）
    first_frame = await _resolve_first_frame(db, shot_group)
    if first_frame:
        # 持久化到当前组 reference_frames.first_frame
        await _persist_first_frame(db, shot_group, first_frame)
    # 任一实际传给视频模型的图片都必须绑定当前审核快照；首帧也在其中。
    include_first_frame(aggregated, first_frame)

    # 5. 取实际激活供应商配置。审核要求由规范化后的实际供应商决定：只有
    # 火山 Seedance 才调用 veImageX；百炼等供应商不展示也不强制该审核流程。
    runtime_video_model = await resolve_video_model(db, settings)
    if runtime_video_model.reference_image_audit_required:
        audit_state = ensure_references_approved(shot_group.reference_image_audit, aggregated)
    else:
        audit_state = audit_not_required_state(aggregated)
    # 创建任务的组级版本快照必须来自所有镜头组变更落库之后；否则自动补首帧
    # 会更新 updated_at，导致刚创建的任务立即被工作台判定为历史结果。
    await db.flush()
    await db.refresh(shot_group, attribute_names=["updated_at"])

    provider_key = runtime_video_model.task_provider_key
    model = runtime_video_model.model

    # 6. 构造 input_json 并落库任务
    # prompt 兜底：约束锁9条属上游 enrich/分镜产物，video 模块仅透传；但上游漏写
    # 时补齐，保证传入 provider 的 prompt 含约束段（doc06 §8.1）。
    input_json = {
        "model": model,
        "prompt": prompt_text,
        "style_id": style_id,
        "reference_images": aggregated.reference_images,
        "reference_image_names": aggregated.reference_image_names,
        "reference_audios": aggregated.reference_audios,
        "reference_audio_names": aggregated.reference_audio_names,
        "reference_videos": [],
        "reference_video_names": [],
        # 首帧参考（上一组尾帧，链式衔接；传给 provider 作首帧约束）
        "first_frame": first_frame,
        "reference_image_audit": audit_state,
        # 未通过审核的主体名/角色（供 [检查素材] 接口标红）
        "missing_subjects": aggregated.missing_subjects,
        "missing_audio": aggregated.missing_audio,
        "duration_seconds": shot_group.duration_seconds,
        "shot_group_version": {
            "episode_version": int(episode.episode_version or 1),
            "shot_group_updated_at": shot_group.updated_at.isoformat()
            if shot_group.updated_at is not None
            else None,
            "shot_group_content_hash": shot_group_content_hash(shot_group),
        },
    }

    attempt = await video_crud.next_attempt_for_unit(db, unit_id)

    try:
        job = await video_crud.create_video_job(
            db,
            script_id=script_id,
            episode_id=episode_id,
            shot_group_id=shot_group_id,
            owner_user_id=user_id,
            scene_no=shot_group.scene_no,
            unit_id=unit_id,
            unit_runtime_key=shot_group.unit_runtime_key
            or f"{episode_id}:{shot_group.scene_no}:group:{shot_group_id}",
            provider_key=provider_key,
            job_type="all_reference",
            model=model,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            input_version=int(episode.episode_version or 1),
            attempt=attempt,
            reservation_id=None,
            input_json=input_json,
        )
        await video_crud.bind_idempotency_request(
            db,
            script_id=script_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            job_id=job.job_id,
        )

        # 7. 积分 reserve（source_id=job_id 幂等；冻结量=price_video_gen_frozen）
        reservation_id: str | None = None
        if _credits_reserve is not None:
            account_id = await _resolve_user_account_id(db, user_id)
            reservation_id = await _credits_reserve(
                db,
                account_id=account_id,
                source_type="generation_job",
                source_id=job.job_id,
                estimated=settings.price_video_gen_frozen,
                actor_user_id=user_id,
                commit=False,
            )
            job.reservation_id = reservation_id
            input_json["reservation_id"] = reservation_id

        dispatch = await enqueue_task(
            db,
            task_type="video.generate",
            job_id=job.job_id,
            attempt=job.attempt,
            payload={
                "job_id": job.job_id,
                "script_id": script_id,
                "episode_id": episode_id,
                "shot_group_id": shot_group_id,
                "input_version": job.input_version,
            },
        )
        job.dispatch_id = dispatch.id
        # updated_at 由数据库侧 onupdate 生成；flush 后会被 SQLAlchemy 标记为过期。
        # 提交前定向刷新，避免响应序列化在同步属性访问中触发异步隐式查询。
        await db.flush()
        await db.refresh(job, attribute_names=["updated_at"])
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        existing = await _resolve_idempotent_job(
            db, script_id, idempotency_key, request_hash
        )
        if existing is not None:
            return existing
        raise ConflictError(_IDEMPOTENCY_CONFLICT_DETAIL) from exc
    except Exception:
        await db.rollback()
        raise

    return job


async def cancel_video_job(
    db: AsyncSession,
    *,
    script_id: str,
    job_id: str,
    actor_user_id: str,
) -> "VideoGenerationJob":
    """取消视频生成任务 — starting/processing 转 canceled，重复 canceled 幂等返回。

    doc04 §1.19 / doc08 §2.5：canceled 为终态。
    权限校验由 API 层在调用前完成（check_script_access require_write=True）。

    Raises:
        HTTPException 404: 任务不存在。
        ConflictError 409: 任务处于 canceled 之外的终态，不可取消。
    """
    from fastapi import HTTPException, status

    from app.core.errors import ConflictError
    from app.models.video_generation_job import VideoGenerationJob

    result = await db.execute(
        select(VideoGenerationJob)
        .where(VideoGenerationJob.job_id == job_id)
        .options(noload(VideoGenerationJob.script))
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="视频生成任务不存在")
    if job.script_id != script_id:
        raise ConflictError("任务不属于该剧本")
    if job.status == "canceled":
        return job
    if job.status not in ("starting", "processing"):
        raise ConflictError(f"任务当前状态 {job.status}，不可取消")
    if provider_submission_is_unresolved(job.output_json, job.metrics):
        raise ConflictError("远端提交状态未知，任务需保持冻结并等待人工核对")
    provider_task_id = str(
        (job.output_json or {}).get("provider_task_id")
        or (job.metrics or {}).get("provider_task_id")
        or ""
    )
    provider_key = job.provider_key
    canceled_output, canceled_metrics = merge_video_task_checkpoint_lease_authority(
        job.output_json,
        job.metrics,
        job.lease_token,
    )
    if provider_task_id:
        canceled_output, canceled_metrics = merge_pending_provider_cancellation(
            canceled_output,
            canceled_metrics,
            provider_task_id,
        )

    # 释放冻结积分（按 source_type='generation_job' 反查 reservation）
    if _credits_reserve is not None:
        reservation_id = await _lookup_reservation_for_cancel(db, job_id)
        if reservation_id:
            from app.services.credits import release as _credits_release

            await _credits_release(
                db,
                reservation_id=reservation_id,
                reason=f"视频任务取消 job_id={job_id}",
                actor_user_id=actor_user_id,
                commit=False,
            )
        else:
            logger.warning("取消任务未找到预扣预约，跳过 release job_id=%s", job_id)

    await cancel_outbox_task(
        db,
        task_type="video.generate",
        job_id=job.job_id,
        attempt=job.attempt,
    )
    now = __import__("datetime").datetime.now(__import__("datetime").UTC)
    result = cast(
        CursorResult[Any],
        await db.execute(
            update(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                VideoGenerationJob.status.in_(("starting", "processing")),
                VideoGenerationJob.lease_token == job.lease_token,
            )
            .values(
                status="canceled",
                progress_stage="canceled",
                progress_message="用户取消",
                canceled_at=now,
                finished_at=now,
                lease_owner=None,
                lease_token=None,
                lease_until=None,
                output_json=canceled_output,
                metrics=canceled_metrics,
                version=VideoGenerationJob.version + 1,
            )
        ),
    )
    if result.rowcount != 1:
        await db.rollback()
        raise ConflictError("任务状态已变化，请刷新后重试")
    try:
        await db.commit()
    except Exception:
        await _best_effort_cancel_rollback(db, job_id=job_id, stage="local_cancel_commit")
        authoritative = await _load_authoritative_canceled_job(
            script_id=script_id,
            job_id=job_id,
        )
        if authoritative is None:
            raise
        logger.warning(
            "视频本地取消提交结果未知，已由独立会话确认终态 job_id=%s",
            job_id,
        )
        # 无法证明本次请求拥有远端取消执行权，保留同事务写入的 pending debt，
        # 交给 reconciler 唯一接管，避免并发请求重复调用 Provider。
        return authoritative

    # 先提交本地终态，使 worker 的取消探针可见；远端取消失败不得回滚本地取消。
    if provider_task_id:
        cancellation_result = await _best_effort_cancel_remote_video_task(
            provider_key,
            provider_task_id,
        )
        primary_persisted = False
        primary_error_type: str | None = None
        try:
            persisted = await persist_provider_cancellation_result(
                db,
                job_id,
                cancellation_result,
            )
            if persisted:
                await db.commit()
                primary_persisted = True
            else:
                await _best_effort_cancel_rollback(
                    db,
                    job_id=job_id,
                    stage="remote_cancel_result_cas",
                )
        except Exception as exc:  # noqa: BLE001 - 本地 canceled 已是权威成功
            primary_error_type = type(exc).__name__
            await _best_effort_cancel_rollback(
                db,
                job_id=job_id,
                stage="remote_cancel_result_commit",
            )
        if not primary_persisted:
            authoritative = await _persist_cancellation_result_independently(
                script_id=script_id,
                job_id=job_id,
                result=cancellation_result,
            )
            if authoritative is not None:
                logger.warning(
                    "远端视频取消结果已由独立事务确认 job_id=%s task_id=%s",
                    job_id,
                    provider_task_id,
                )
                return authoritative
            canceled = await _load_authoritative_canceled_job(
                script_id=script_id,
                job_id=job_id,
            )
            logger.warning(
                "远端视频取消结果无法通过主事务或独立事务确认，保留 pending debt；"
                "reconciler 后续可能再次调用 Provider，依赖 Provider cancel 幂等 "
                "job_id=%s task_id=%s primary_error=%s",
                job_id,
                provider_task_id,
                primary_error_type or "cas_not_applied",
            )
            return canceled or job

    # 重新读取返回最新状态
    r2 = await db.execute(
        select(VideoGenerationJob).where(VideoGenerationJob.job_id == job_id)
    )
    return r2.scalar_one()  # type: ignore[return-value]


async def _best_effort_cancel_rollback(
    db: AsyncSession,
    *,
    job_id: str,
    stage: str,
) -> None:
    """提交结果未知时尽力结束原事务，回滚异常不得覆盖原始事实。"""
    try:
        await db.rollback()
    except Exception as exc:  # noqa: BLE001 - 随后使用独立会话判权
        logger.warning(
            "视频取消事务回滚失败 job_id=%s stage=%s error=%s",
            job_id,
            stage,
            type(exc).__name__,
        )


async def _load_authoritative_canceled_job(
    *,
    script_id: str,
    job_id: str,
) -> "VideoGenerationJob | None":
    """使用独立会话读取 canceled 权威终态，原会话可能已不可继续使用。"""
    from app.core.database import async_session_factory
    from app.models.video_generation_job import VideoGenerationJob

    try:
        async with async_session_factory() as authority_db:
            job = (
                await authority_db.execute(
                    select(VideoGenerationJob)
                    .where(VideoGenerationJob.job_id == job_id)
                    .options(noload(VideoGenerationJob.script))
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
    except Exception as exc:  # noqa: BLE001 - 未能证明成功时调用方 fail closed
        logger.warning(
            "视频取消权威终态读取失败 job_id=%s error=%s",
            job_id,
            type(exc).__name__,
        )
        return None
    if job is None or job.script_id != script_id or job.status != "canceled":
        return None
    return job


def _cancellation_result_is_persisted(
    job: "VideoGenerationJob",
    result: VideoCancellationResult,
) -> bool:
    """判断数据库是否已有同等或更强的远端取消事实。"""
    record = (job.output_json or {}).get("provider_cancellation")
    if not isinstance(record, dict):
        return False
    if str(record.get("provider_task_id") or "") != result.provider_task_id:
        return False
    outcome = str(record.get("outcome") or "")
    retryable = record.get("retryable")
    if outcome == VideoCancellationOutcome.CANCELED.value:
        return retryable is False
    if outcome == VideoCancellationOutcome.UNSUPPORTED.value:
        return (
            result.outcome
            in (VideoCancellationOutcome.UNSUPPORTED, VideoCancellationOutcome.FAILED)
            and retryable is False
        )
    return (
        outcome == VideoCancellationOutcome.FAILED.value
        and result.outcome is VideoCancellationOutcome.FAILED
        and retryable is True
    )


async def _load_authoritative_cancellation_result(
    *,
    script_id: str,
    job_id: str,
    result: VideoCancellationResult,
) -> "VideoGenerationJob | None":
    job = await _load_authoritative_canceled_job(
        script_id=script_id,
        job_id=job_id,
    )
    if job is None or not _cancellation_result_is_persisted(job, result):
        return None
    return job


async def _persist_cancellation_result_independently(
    *,
    script_id: str,
    job_id: str,
    result: VideoCancellationResult,
) -> "VideoGenerationJob | None":
    """用独立事务补写已取得的 Provider 结果，不再次访问 Provider。"""
    from app.core.database import async_session_factory
    from app.models.video_generation_job import VideoGenerationJob

    for attempt in range(1, 3):
        current: VideoGenerationJob | None = None
        try:
            async with async_session_factory() as repair_db:
                current = (
                    await repair_db.execute(
                        select(VideoGenerationJob)
                        .where(
                            VideoGenerationJob.job_id == job_id,
                            VideoGenerationJob.status == "canceled",
                        )
                        .options(noload(VideoGenerationJob.script))
                        .execution_options(populate_existing=True)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if current is None or current.script_id != script_id:
                    await _best_effort_cancel_rollback(
                        repair_db,
                        job_id=job_id,
                        stage="independent_cancel_result_missing",
                    )
                    return None
                if _cancellation_result_is_persisted(current, result):
                    return current
                persisted = await persist_locked_provider_cancellation_result(
                    repair_db,
                    current,
                    result,
                )
                if not persisted:
                    await _best_effort_cancel_rollback(
                        repair_db,
                        job_id=job_id,
                        stage="independent_cancel_result_cas",
                    )
                else:
                    try:
                        await repair_db.commit()
                    except Exception as exc:  # noqa: BLE001 - 提交结果需独立判权
                        logger.warning(
                            "独立保存远端取消结果提交未知 job_id=%s attempt=%s error=%s",
                            job_id,
                            attempt,
                            type(exc).__name__,
                        )
                        await _best_effort_cancel_rollback(
                            repair_db,
                            job_id=job_id,
                            stage="independent_cancel_result_commit",
                        )
                    else:
                        authoritative = await _load_authoritative_cancellation_result(
                            script_id=script_id,
                            job_id=job_id,
                            result=result,
                        )
                        return authoritative or current
        except Exception as exc:  # noqa: BLE001 - 下一独立事务仍可补写同一结果
            logger.warning(
                "独立保存远端取消结果失败 job_id=%s attempt=%s error=%s",
                job_id,
                attempt,
                type(exc).__name__,
            )

        authoritative = await _load_authoritative_cancellation_result(
            script_id=script_id,
            job_id=job_id,
            result=result,
        )
        if authoritative is not None:
            return authoritative
    return None


async def _best_effort_cancel_remote_video_task(
    provider_key: str,
    provider_task_id: str,
) -> VideoCancellationResult:
    try:
        result = await get_video_provider(provider_key).cancel(provider_task_id)
    except (asyncio.CancelledError, SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001 - 本地 canceled 终态不能被远端异常覆盖
        logger.warning(
            "远端视频任务取消调用失败 provider=%s task_id=%s error=%s",
            provider_key,
            provider_task_id,
            type(exc).__name__,
        )
        return VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.FAILED,
            detail=f"远端取消调用失败: {type(exc).__name__}",
        )
    if result.outcome is VideoCancellationOutcome.CANCELED:
        logger.info(
            "远端视频任务已请求取消 provider=%s task_id=%s",
            provider_key,
            provider_task_id,
        )
        return result
    logger.warning(
        "远端视频任务未确认取消 provider=%s task_id=%s outcome=%s detail_present=%s",
        provider_key,
        provider_task_id,
        result.outcome.value,
        result.detail is not None,
    )
    return result


async def _lookup_reservation_for_cancel(db: AsyncSession, job_id: str) -> str | None:
    """按 source_type='generation_job' 反查 reservation_id（与 worker 命名空间一致）。"""
    from app.models.credit_reservation import CreditReservation

    result = await db.execute(
        select(CreditReservation.id).where(
            CreditReservation.source_type == "generation_job",
            CreditReservation.source_id == job_id,
            CreditReservation.status == "reserved",
        )
    )
    return result.scalar_one_or_none()


async def _resolve_idempotent_job(
    db: AsyncSession,
    script_id: str,
    idempotency_key: str,
    request_hash: str,
) -> "VideoGenerationJob | None":
    """从权威映射读取幂等结果，并统一校验请求载荷。

    doc 08 §4.2：同一幂等键重复创建任务，不产生重复作业或重复冻结。

    幂等键查询加 script_id 过滤，防止跨剧本 IDOR：用户 B 使用与用户 A 相同的
    幂等键命中 A 的任务（doc06 §8 权限边界 — 任务归属剧本维度隔离）。
    """
    binding = await video_crud.find_idempotency_binding(
        db, script_id=script_id, idempotency_key=idempotency_key
    )
    if binding is None:
        return None
    if binding.request_hash != request_hash:
        raise ConflictError(_IDEMPOTENCY_CONFLICT_DETAIL)
    return binding.job


async def _bind_existing_job(
    db: AsyncSession,
    *,
    script_id: str,
    idempotency_key: str,
    request_hash: str,
    job: "VideoGenerationJob",
) -> "VideoGenerationJob":
    """为活动任务持久化别名 key；唯一冲突时返回权威赢家。"""
    try:
        await video_crud.bind_idempotency_request(
            db,
            script_id=script_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            job_id=job.job_id,
        )
        await db.commit()
        return job
    except IntegrityError as exc:
        await db.rollback()
        winner = await _resolve_idempotent_job(
            db, script_id, idempotency_key, request_hash
        )
        if winner is not None:
            return winner
        raise ConflictError(_IDEMPOTENCY_CONFLICT_DETAIL) from exc


async def _resolve_first_frame(db: AsyncSession, shot_group: ShotGroup) -> str | None:
    """取上一镜头组的 reference_frames.last_frame 作为当前组首帧参考。

    上一组定义：同剧本/分集/场景，group_no 顺序在当前组之前，且已生成尾帧
    （reference_frames.last_frame 非空）的最近一组。无则返回 None（首组无首帧）。
    doc 06 §10.3 尾帧 → 首帧衔接。
    """
    if not shot_group.group_no:
        return None
    stmt = select(ShotGroup).where(
        ShotGroup.script_id == shot_group.script_id,
        ShotGroup.episode_id == shot_group.episode_id,
        ShotGroup.scene_no == shot_group.scene_no,
        ShotGroup.shot_group_id != shot_group.shot_group_id,
    )
    result = await db.execute(stmt)
    candidates = list(result.scalars().all())
    # 取 group_no 严格小于当前且 last_frame 非空的最近一组
    cur_no = _natural_group_number(shot_group.group_no)
    prev_groups = [
        g for g in candidates if g.group_no and _natural_group_number(g.group_no) < cur_no
    ]
    prev_groups.sort(key=lambda g: _natural_group_number(g.group_no), reverse=True)
    for g in prev_groups:
        ref = g.reference_frames or {}
        last_frame = ref.get("last_frame")
        if last_frame:
            return str(last_frame)
    return None


async def _persist_first_frame(
    db: AsyncSession, shot_group: ShotGroup, first_frame: str
) -> None:
    """将首帧参考写入 shot_group.reference_frames.first_frame。"""
    ref_frames = dict(shot_group.reference_frames or {}) or {}
    if ref_frames.get("first_frame") == first_frame:
        return
    ref_frames["first_frame"] = first_frame
    shot_group.reference_frames = ref_frames


# 约束锁兜底文本（doc06 §8.1 原文）。上游 enrich/分镜已注入则透传，漏写时追加。
_CONSTRAINT_LOCK_TEXT = (
    "【约束】"
    "人物特征高度一致，角色不脸崩、肢体不畸形，多镜头切镜丝滑平顺，画面质量4K稳定。"
    "语音清晰，情感表达贴合角色性格，台词与唇形/动作精准对位。"
    "禁止出现任何形式的字幕。"
)


def _ensure_constraint_lock(prompt: str) -> str:
    """若 prompt 未含【约束】段则追加约束锁9条兜底文本（doc06 §8.1）。

    video 模块仅透传上游 enrich 产出的 prompt；此处仅作上游漏写兜底，
    不替代 enrich 阶段的 12 段落扩展（doc06 §12 LLM-3，属上游职责）。
    """
    if not prompt:
        return _CONSTRAINT_LOCK_TEXT
    if _CONSTRAINT_LOCK_TEXT in prompt:
        return prompt
    return f"{prompt}\n{_CONSTRAINT_LOCK_TEXT}"


def _natural_group_number(group_no: str | None) -> int:
    import re

    match = re.search(r"\d+", group_no or "")
    return int(match.group(0)) if match else -1


def _request_hash(payload: dict) -> str:
    stable = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


async def _resolve_user_account_id(db: AsyncSession, user_id: str) -> str:
    """按 user owner 维度解析积分账户 ID。

    CreditAccount(owner_type='user', owner_id=user_id)。无账户则抛冲突。
    """
    from sqlalchemy import select

    from app.core.errors import ConflictError
    from app.models.credit import CreditAccount

    result = await db.execute(
        select(CreditAccount.id).where(
            CreditAccount.owner_type == "user",
            CreditAccount.owner_id == user_id,
        )
    )
    account_id = result.scalar_one_or_none()
    if account_id is None:
        raise ConflictError("当前用户无积分账户")
    return account_id
