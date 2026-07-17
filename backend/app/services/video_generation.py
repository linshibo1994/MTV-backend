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

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.config import settings
from app.crud import video as video_crud
from app.models.episode import Episode
from app.models.shot_group import ShotGroup
from app.providers.registry import ProviderConfigurationError, normalize_provider
from app.services.reference_aggregator import aggregate_references
from app.services.task_dispatch import enqueue_task

if TYPE_CHECKING:
    from app.models.video_generation_job import VideoGenerationJob

logger = logging.getLogger(__name__)

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
) -> "VideoGenerationJob":
    """创建视频生成任务 — 幂等去重 → 汇聚参考图/音频 → 首帧衔接 → 落库 → reserve 积分 → 投递 worker。

    Args:
        db: 异步会话。
        script_id: 剧本 ID。
        episode_id: 分集 ID。
        shot_group_id: 镜头组 ID（绑定生成单元）。
        user_id: 触发用户（积分扣减归属，按 user owner 维度账户）。
        idempotency_key: 可选幂等键（来自 Idempotency-Key 头）。

    Returns:
        VideoGenerationJob ORM 对象（重复创建时返回已存在任务）。

    Raises:
        HTTPException 404: 镜头组不存在。
        ForbiddenError 403: 镜头组不属于该剧本/分集。
        InsufficientCreditsError 402: 积分不足（由 credits.reserve 抛出）。
    """
    from fastapi import HTTPException, status

    from app.core.errors import BadRequestError, ConflictError, ForbiddenError

    if (
        settings.video_provider_selection_mode == "env"
        and normalize_provider(settings.video_gen_provider) == "gemini"
    ):
        raise ProviderConfigurationError(
            "Gemini Veo Provider 已实现，但当前数据库约束尚未启用 provider_key=gemini；"
            "需先执行经确认的 Alembic 迁移"
        )

    if not idempotency_key:
        raise BadRequestError("Idempotency-Key 必填")
    unit_id = f"group:{shot_group_id}"
    request_hash = _request_hash(
        {
            "script_id": script_id,
            "episode_id": episode_id,
            "shot_group_id": shot_group_id,
        }
    )

    # 1. 幂等去重（doc 08 §4.2）
    existing = await _find_idempotent_job(db, script_id, idempotency_key)
    if existing is not None:
        if existing.request_hash != request_hash:
            raise ConflictError("幂等键已用于不同请求载荷")
        logger.info("视频生成任务幂等复用 job_id=%s unit_id=%s", existing.job_id, unit_id)
        return existing

    # 2. 取 shot_group 并校验归属
    sg_result = await db.execute(
        select(ShotGroup).where(ShotGroup.shot_group_id == shot_group_id)
    )
    shot_group = sg_result.scalar_one_or_none()
    if shot_group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="镜头组不存在")
    if shot_group.script_id != script_id or shot_group.episode_id != episode_id:
        raise ForbiddenError("镜头组不属于该剧本/分集")
    episode_result = await db.execute(
        select(Episode)
        .where(Episode.episode_id == episode_id)
        .options(noload(Episode.script), noload(Episode.scenes))
    )
    episode = episode_result.scalar_one_or_none()
    if episode is None or episode.script_id != script_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="分集不存在")

    # 3. 汇聚参考图 + 参考音频
    aggregated = await aggregate_references(db, shot_group_id)

    # 4. 首尾帧链式衔接：取上一组 last_frame 作为当前组首帧参考（doc 06 §10.3）
    first_frame = await _resolve_first_frame(db, shot_group)
    if first_frame:
        # 持久化到当前组 reference_frames.first_frame
        await _persist_first_frame(db, shot_group, first_frame)

    # 5. 取激活供应商配置，缺失兜底
    provider_cfg = await video_crud.get_active_provider(db, "all_reference")
    if settings.video_provider_selection_mode == "env":
        # env 模式下部署级环境变量是权威配置，不再被数据库历史 active 行覆盖。
        provider_key = settings.video_gen_provider
        model = settings.video_gen_model or settings.seedance_model
    elif provider_cfg is None:
        provider_key = settings.video_default_provider
        model = settings.video_gen_model or settings.seedance_model
    else:
        provider_key = provider_cfg.provider_key
        model = provider_cfg.model

    # 6. 构造 input_json 并落库任务
    # prompt 兜底：约束锁9条属上游 enrich/分镜产物，video 模块仅透传；但上游漏写
    # 时补齐，保证传入 provider 的 prompt 含约束段（doc06 §8.1）。
    prompt_text = _ensure_constraint_lock(shot_group.prompt or "")
    input_json = {
        "model": model,
        "prompt": prompt_text,
        "reference_images": aggregated.reference_images,
        "reference_image_names": aggregated.reference_image_names,
        "reference_audios": aggregated.reference_audios,
        "reference_audio_names": aggregated.reference_audio_names,
        "reference_videos": [],
        "reference_video_names": [],
        # 首帧参考（上一组尾帧，链式衔接；传给 provider 作首帧约束）
        "first_frame": first_frame,
        # 未通过审核的主体名/角色（供 [检查素材] 接口标红）
        "missing_subjects": aggregated.missing_subjects,
        "missing_audio": aggregated.missing_audio,
        "duration_seconds": shot_group.duration_seconds,
        "shot_group_version": {
            "episode_version": int(episode.episode_version or 1),
            "shot_group_updated_at": shot_group.updated_at.isoformat()
            if shot_group.updated_at is not None
            else None,
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
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        existing = await _find_idempotent_job(db, script_id, idempotency_key)
        if existing is not None and existing.request_hash == request_hash:
            return existing
        raise ConflictError("视频生成任务幂等冲突") from exc
    except Exception:
        await db.rollback()
        raise

    return job


async def cancel_video_job(
    db: AsyncSession,
    *,
    script_id: str,
    job_id: str,
) -> "VideoGenerationJob":
    """取消视频生成任务 — 仅 starting/processing 可取消，置 canceled 并释放冻结积分。

    doc04 §1.19 / doc08 §2.5：canceled 为终态。
    权限校验由 API 层在调用前完成（check_script_access require_write=True）。

    Raises:
        HTTPException 404: 任务不存在。
        ConflictError 409: 任务已终态，不可取消。
    """
    from fastapi import HTTPException, status

    from app.core.errors import ConflictError
    from app.models.video_generation_job import VideoGenerationJob

    result = await db.execute(
        select(VideoGenerationJob)
        .where(VideoGenerationJob.job_id == job_id)
        .with_for_update()
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="视频生成任务不存在")
    if job.script_id != script_id:
        raise ConflictError("任务不属于该剧本")
    if job.status not in ("starting", "processing"):
        raise ConflictError(f"任务当前状态 {job.status}，不可取消")

    # 释放冻结积分（按 source_type='generation_job' 反查 reservation）
    if _credits_reserve is not None:
        reservation_id = await _lookup_reservation_for_cancel(db, job_id)
        if reservation_id:
            from app.services.credits import release as _credits_release

            await _credits_release(
                db,
                reservation_id=reservation_id,
                reason=f"视频任务取消 job_id={job_id}",
                commit=False,
            )
        else:
            logger.warning("取消任务未找到预扣预约，跳过 release job_id=%s", job_id)

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
                version=VideoGenerationJob.version + 1,
            )
        ),
    )
    if result.rowcount != 1:
        await db.rollback()
        raise ConflictError("任务状态已变化，请刷新后重试")
    await db.commit()

    # 重新读取返回最新状态
    r2 = await db.execute(
        select(VideoGenerationJob).where(VideoGenerationJob.job_id == job_id)
    )
    return r2.scalar_one()  # type: ignore[return-value]


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


async def _find_idempotent_job(
    db: AsyncSession, script_id: str, idempotency_key: str
):
    """幂等去重：优先按 Idempotency-Key 查历史 job，否则按 unit_id 查活动任务。

    doc 08 §4.2：同一幂等键重复创建任务，不产生重复作业或重复冻结。

    幂等键查询加 script_id 过滤，防止跨剧本 IDOR：用户 B 使用与用户 A 相同的
    幂等键命中 A 的任务（doc06 §8 权限边界 — 任务归属剧本维度隔离）。
    """
    return await video_crud.find_job_by_idempotency(
        db, script_id=script_id, idempotency_key=idempotency_key
    )


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
    if "【约束】" in prompt:
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
