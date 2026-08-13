"""关键场次导演方案生成、持久化与确认。"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

from pydantic import ValidationError
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import BadRequestError, ConflictError, NotFoundError
from app.core.ids import new_id
from app.models.episode import Episode
from app.models.scene_audition import SceneAudition
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.providers import LLMProvider, get_llm_provider
from app.schemas.scene_audition import SceneAuditionPlanPayload

_SYSTEM = """你是影视导演。请针对一个关键场次给出 2 到 3 个真正不同的视觉导演方案。
每个方案必须回答 audience_knowledge_timing、alignment_target、performance_space、
strongest_image、landing、tradeoff 六个维度。严格返回 JSON，不要 Markdown。"""

_GENERATION_ERROR_CODE = "model_generation_failed"
_GENERATION_ERROR_MESSAGE = "导演方案生成失败，请重试"
_INVALID_RESPONSE_ERROR_CODE = "invalid_model_response"
_INVALID_RESPONSE_ERROR_MESSAGE = "导演方案返回格式不符合要求，请重试"
_WORKER_TIMEOUT_ERROR_CODE = "worker_timeout"
_WORKER_TIMEOUT_ERROR_MESSAGE = "导演方案任务执行超时，请重试"
_KEY_SCENE_TYPES = frozenset(
    {
        "character_introduction",
        "identity_reveal",
        "relationship_reversal",
        "climax_ending",
    }
)
_RUN_TIMEOUT = timedelta(minutes=10)


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _is_recoverable_timeout(row: SceneAudition, *, now: datetime) -> bool:
    if row.status == "running":
        return row.started_at is not None and _as_utc(row.started_at) <= now - _RUN_TIMEOUT
    if row.status == "draft":
        # 已发布但 Worker 尚未标记 running 的丢失消息，用更新时间作为保守恢复窗口。
        return _as_utc(row.updated_at) <= now - _RUN_TIMEOUT
    return False


def _validate_key_scene_type(scene_type: str) -> None:
    """普通场次不进入高成本多方案流程，调用方必须提交明确的关键类型。"""

    if scene_type not in _KEY_SCENE_TYPES:
        raise BadRequestError("仅人物亮相、身份揭示、关系反转或高潮结尾可生成导演方案")


async def fail_expired_auditions(
    db: AsyncSession,
    *,
    limit: int = 100,
    now: datetime | None = None,
) -> int:
    """将失联 draft/running 终结为可重试失败，避免前端永久轮询。

    未知供应商调用是否已经发生时不自动重放；用户再次发起生成会沿既有 attempt
    栅栏创建新的 Outbox。``skip_locked`` 避开仍在正常执行且持有行锁的 Worker。
    """

    current = now or datetime.now(UTC)
    threshold = current - _RUN_TIMEOUT
    rows = list(
        (
            await db.execute(
                select(SceneAudition)
                .where(
                    or_(
                        and_(
                            SceneAudition.status == "running",
                            SceneAudition.started_at.is_not(None),
                            SceneAudition.started_at <= threshold,
                        ),
                        and_(
                            SceneAudition.status == "draft",
                            SceneAudition.updated_at <= threshold,
                        ),
                    )
                )
                .order_by(SceneAudition.updated_at.asc(), SceneAudition.id.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).scalars().all()
    )
    for row in rows:
        row.status = "failed"
        row.error_code = _WORKER_TIMEOUT_ERROR_CODE
        row.error_message = _WORKER_TIMEOUT_ERROR_MESSAGE
        row.finished_at = current
    await db.flush()
    return len(rows)


def _source_hash(
    *,
    scene_no: int,
    episode_version: int,
    scene_type: str,
    script_text: str,
) -> str:
    payload = json.dumps(
        {
            "scene_no": scene_no,
            "episode_version": episode_version,
            "scene_type": scene_type,
            "script_text": script_text,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _legacy_source_hash(*, scene_no: int, episode_version: int, script_text: str) -> str:
    """兼容尚未纳入 ``scene_type`` 的历史指纹，命中后由调用方原位升级。"""

    payload = json.dumps(
        {"scene_no": scene_no, "episode_version": episode_version, "script_text": script_text},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


async def list_auditions(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
) -> tuple[list[SceneAudition], int]:
    conditions = [
        SceneAudition.script_id == script_id,
        SceneAudition.episode_id == episode_id,
    ]
    total = int(
        (await db.execute(select(func.count()).select_from(SceneAudition).where(*conditions))).scalar_one()
    )
    rows = (
        await db.execute(
            select(SceneAudition)
            .where(*conditions)
            .order_by(SceneAudition.scene_no.asc(), SceneAudition.input_version.desc())
        )
    ).scalars().all()
    return list(rows), total


async def generate_audition(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    scene_no: int,
    expected_episode_version: int,
    scene_type: str,
    provider: LLMProvider | None = None,
) -> SceneAudition:
    _validate_key_scene_type(scene_type)
    episode = (
        await db.execute(
            select(Episode)
            .where(Episode.episode_id == episode_id, Episode.script_id == script_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if episode is None:
        raise NotFoundError("分集不存在")
    if episode.episode_version != expected_episode_version:
        raise ConflictError("分集版本已变化，请刷新后重新生成导演方案")
    source_hash = _source_hash(
        scene_no=scene_no,
        episode_version=episode.episode_version,
        scene_type=scene_type,
        script_text=episode.script_text or "",
    )
    existing = (
        await db.execute(
            select(SceneAudition).where(
                SceneAudition.episode_id == episode_id,
                SceneAudition.scene_no == scene_no,
                SceneAudition.input_version == episode.episode_version,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.source_hash == source_hash:
            return existing
        if existing.scene_type == scene_type and existing.source_hash == _legacy_source_hash(
            scene_no=scene_no,
            episode_version=episode.episode_version,
            script_text=episode.script_text or "",
        ):
            existing.source_hash = source_hash
            await db.flush()
            return existing
        # 唯一键仍绑定同一场次版本；输入正文或关键场次类型变化时保留旧记录并显式过期。
        existing.status = "stale"
        existing.finished_at = datetime.now(UTC)
        await db.flush()
        return existing

    llm = provider or get_llm_provider()
    raw = await llm.complete_json(
        _SYSTEM,
        json.dumps(
            {
                "scene_no": scene_no,
                "scene_type": scene_type,
                "episode_script": episode.script_text or "",
                "output_shape": {"plans": "2-3 structured plans"},
            },
            ensure_ascii=False,
        ),
        response_schema=SceneAuditionPlanPayload.model_json_schema(),
    )
    plans = SceneAuditionPlanPayload.model_validate(raw).model_dump()["plans"]
    row = SceneAudition(
        id=new_id(),
        script_id=script_id,
        episode_id=episode_id,
        scene_no=scene_no,
        input_version=episode.episode_version,
        scene_type=scene_type,
        source_hash=source_hash,
        plans=plans,
        status="ready",
    )
    db.add(row)
    await db.flush()
    return row


async def queue_audition(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    scene_no: int,
    expected_episode_version: int,
    scene_type: str,
) -> SceneAudition:
    """创建 draft 并与 Outbox 同事务入队；重复请求返回同版本记录。"""

    _validate_key_scene_type(scene_type)
    episode = (
        await db.execute(
            select(Episode)
            .where(Episode.episode_id == episode_id, Episode.script_id == script_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if episode is None:
        raise NotFoundError("分集不存在")
    if episode.episode_version != expected_episode_version:
        raise ConflictError("分集版本已变化，请刷新后重新生成导演方案")
    source_hash = _source_hash(
        scene_no=scene_no,
        episode_version=episode.episode_version,
        scene_type=scene_type,
        script_text=episode.script_text or "",
    )
    row = (
        await db.execute(
            select(SceneAudition).where(
                SceneAudition.episode_id == episode_id,
                SceneAudition.scene_no == scene_no,
                SceneAudition.input_version == episode.episode_version,
            ).with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        row = SceneAudition(
            id=new_id(),
            script_id=script_id,
            episode_id=episode_id,
            scene_no=scene_no,
            input_version=episode.episode_version,
            scene_type=scene_type,
            source_hash=source_hash,
            plans=[],
            status="draft",
            attempt_count=1,
        )
        db.add(row)
        await db.flush()
        dispatch_attempt = 1
    else:
        legacy_source_hash = _legacy_source_hash(
            scene_no=scene_no,
            episode_version=episode.episode_version,
            script_text=episode.script_text or "",
        )
        if row.source_hash == legacy_source_hash and row.scene_type == scene_type:
            row.source_hash = source_hash
        elif row.source_hash != source_hash:
            # 版本号未变但正文或关键场次类型变化时不允许覆盖旧输入，保留 stale 诊断供调用方处理。
            row.status = "stale"
            row.finished_at = datetime.now(UTC)
            await db.flush()
            return row

        if row.status == "failed" or _is_recoverable_timeout(row, now=datetime.now(UTC)):
            # 重试沿用同一业务记录，但必须创建新的 Outbox attempt；行锁与唯一键共同防止双投递。
            latest_attempt = int(
                (
                    await db.execute(
                        select(func.max(TaskDispatchOutbox.attempt)).where(
                            TaskDispatchOutbox.task_type == "scene_audition.generate",
                            TaskDispatchOutbox.job_id == row.id,
                        )
                    )
                ).scalar_one_or_none()
                or 0
            )
            dispatch_attempt = max(row.attempt_count, latest_attempt) + 1
            row.status = "draft"
            row.attempt_count = dispatch_attempt
            row.plans = []
            row.selected_plan_id = None
            row.error_code = None
            row.error_message = None
            row.started_at = None
            row.finished_at = None
            await db.flush()
        else:
            # draft/ready/selected/stale 都是幂等状态，不制造额外投递。
            await db.flush()
            return row

    if row.status == "draft":
        from app.services.task_dispatch import enqueue_task

        await enqueue_task(
            db,
            task_type="scene_audition.generate",
            job_id=row.id,
            attempt=dispatch_attempt,
            payload={"audition_id": row.id, "attempt": dispatch_attempt},
        )
    return row


async def run_queued_audition(
    db: AsyncSession,
    *,
    audition_id: str,
    expected_attempt: int | None = None,
    provider: LLMProvider | None = None,
    allow_running: bool = False,
) -> SceneAudition | None:
    """Worker 幂等完成 draft；输入变更时标 stale，不覆盖新版本。"""

    row = (
        await db.execute(
            select(SceneAudition).where(SceneAudition.id == audition_id).with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    if expected_attempt is not None and row.attempt_count != expected_attempt:
        return row
    if row.status in {"ready", "selected", "stale", "failed"}:
        return row
    if row.status == "running" and not allow_running:
        return row
    episode = (
        await db.execute(
            select(Episode).where(
                Episode.episode_id == row.episode_id,
                Episode.script_id == row.script_id,
            )
        )
    ).scalar_one_or_none()
    current_source_hash = (
        _source_hash(
            scene_no=row.scene_no,
            episode_version=episode.episode_version,
            scene_type=row.scene_type,
            script_text=episode.script_text or "",
        )
        if episode is not None
        else None
    )
    legacy_source_hash = (
        _legacy_source_hash(
            scene_no=row.scene_no,
            episode_version=episode.episode_version,
            script_text=episode.script_text or "",
        )
        if episode is not None
        else None
    )
    if (
        episode is None
        or episode.episode_version != row.input_version
        or row.source_hash not in {current_source_hash, legacy_source_hash}
    ):
        row.status = "stale"
        row.finished_at = datetime.now(UTC)
        await db.flush()
        return row
    if row.source_hash == legacy_source_hash:
        assert current_source_hash is not None
        row.source_hash = current_source_hash
    row.status = "running"
    row.started_at = row.started_at or datetime.now(UTC)
    row.finished_at = None
    row.error_code = None
    row.error_message = None
    try:
        llm = provider or get_llm_provider()
        raw = await llm.complete_json(
            _SYSTEM,
            json.dumps(
                {
                    "scene_no": row.scene_no,
                    "scene_type": row.scene_type,
                    "episode_script": episode.script_text or "",
                    "output_shape": {"plans": "2-3 structured plans"},
                },
                ensure_ascii=False,
            ),
            response_schema=SceneAuditionPlanPayload.model_json_schema(),
        )
        row.plans = SceneAuditionPlanPayload.model_validate(raw).model_dump()["plans"]
    except ValidationError:
        row.status = "failed"
        row.error_code = _INVALID_RESPONSE_ERROR_CODE
        row.error_message = _INVALID_RESPONSE_ERROR_MESSAGE
    except Exception:  # noqa: BLE001 - Provider 原始错误不得写入数据库或 API
        row.status = "failed"
        row.error_code = _GENERATION_ERROR_CODE
        row.error_message = _GENERATION_ERROR_MESSAGE
    else:
        row.status = "ready"
    row.finished_at = datetime.now(UTC)
    await db.flush()
    return row


async def claim_queued_audition(
    db: AsyncSession,
    *,
    audition_id: str,
    expected_attempt: int | None = None,
) -> tuple[SceneAudition | None, bool]:
    """Worker 短事务领取 draft；返回值指示本次消息是否应调用模型。"""

    row = (
        await db.execute(
            select(SceneAudition).where(SceneAudition.id == audition_id).with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        return None, False
    if expected_attempt is not None and row.attempt_count != expected_attempt:
        return row, False
    if row.status != "draft":
        return row, False
    episode = (
        await db.execute(
            select(Episode).where(
                Episode.episode_id == row.episode_id,
                Episode.script_id == row.script_id,
            )
        )
    ).scalar_one_or_none()
    current_source_hash = (
        _source_hash(
            scene_no=row.scene_no,
            episode_version=episode.episode_version,
            scene_type=row.scene_type,
            script_text=episode.script_text or "",
        )
        if episode is not None
        else None
    )
    legacy_source_hash = (
        _legacy_source_hash(
            scene_no=row.scene_no,
            episode_version=episode.episode_version,
            script_text=episode.script_text or "",
        )
        if episode is not None
        else None
    )
    if (
        episode is None
        or episode.episode_version != row.input_version
        or row.source_hash not in {current_source_hash, legacy_source_hash}
    ):
        row.status = "stale"
        row.finished_at = datetime.now(UTC)
        await db.flush()
        return row, False
    if row.source_hash == legacy_source_hash:
        assert current_source_hash is not None
        row.source_hash = current_source_hash
    row.status = "running"
    row.started_at = datetime.now(UTC)
    row.finished_at = None
    row.error_code = None
    row.error_message = None
    await db.flush()
    return row, True


async def confirm_audition(
    db: AsyncSession,
    *,
    audition_id: str,
    plan_id: str,
    expected_input_version: int,
    actor_user_id: str,
) -> SceneAudition:
    row = (
        await db.execute(
            select(SceneAudition)
            .where(SceneAudition.id == audition_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if row is None:
        raise NotFoundError("导演方案不存在")
    if row.input_version != expected_input_version:
        raise ConflictError("导演方案输入版本已变化")
    episode = (
        await db.execute(
            select(Episode).where(
                Episode.episode_id == row.episode_id,
                Episode.script_id == row.script_id,
            )
        )
    ).scalar_one_or_none()
    if episode is None:
        raise NotFoundError("分集不存在")
    current_source_hash = _source_hash(
        scene_no=row.scene_no,
        episode_version=episode.episode_version,
        scene_type=row.scene_type,
        script_text=episode.script_text or "",
    )
    legacy_source_hash = _legacy_source_hash(
        scene_no=row.scene_no,
        episode_version=episode.episode_version,
        script_text=episode.script_text or "",
    )
    if (
        episode.episode_version != row.input_version
        or row.source_hash not in {current_source_hash, legacy_source_hash}
    ):
        row.status = "stale"
        await db.flush()
        raise ConflictError("分集内容已变化，请重新生成导演方案")
    if row.source_hash == legacy_source_hash:
        row.source_hash = current_source_hash
    if row.status == "draft":
        raise ConflictError("导演方案仍在生成中")
    if row.status == "failed":
        raise ConflictError("导演方案生成失败，请重新生成")
    if row.status == "stale":
        raise ConflictError("导演方案已过期，请重新生成")
    plan_ids = {str(plan.get("plan_id")) for plan in row.plans if isinstance(plan, dict)}
    if plan_id not in plan_ids:
        raise NotFoundError("导演候选方案不存在")
    if row.status == "selected" and row.selected_plan_id == plan_id:
        return row
    row.selected_plan_id = plan_id
    row.status = "selected"
    row.selected_by_user_id = actor_user_id
    row.selected_at = datetime.now(UTC)
    await db.flush()
    return row
