"""生成任务业务服务 — 创建生图任务并投递异步 worker。

create_generation_job 流程：
1. 校验画布项目归属与主体归属（统一走 check_script_access，接入组织+内容两轴权限模型）
2. 创建 GenerationJob 记录（status=starting）
3. 调 app.services.credits.reserve(source_type='generation_job', source_id=job_id, estimated=price_image_gen)
   预扣积分（不足抛 InsufficientCreditsError，事务回滚不落库）
4. 将 reservation_id 写入 input_json
5. 投递 Celery worker generate_images_task.delay(job_id)
6. 返回 job（status=starting, progress=0）

create_batch_generation_jobs 流程（一键设计 N，06 §3.5）：
1. 校验画布项目归属（check_script_access action=Action.RUN_AI）
2. 确定目标主体集合：显式 subject_ids 或自动选取该剧本下未设计(not_designed)且非声音的主体
3. 预检积分账户存在
4. 逐主体构造 prompt（以主体 description 为基础 prompt）并创建任务 + 预扣积分 + 投递 worker
5. 返回已创建任务列表与跳过主体列表
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.config import settings
from app.core.errors import (
    BadRequestError,
    ConflictError,
    ForbiddenError,
    InsufficientCreditsError,
    NotFoundError,
)
from app.core.ids import new_id
from app.core.permissions import Action, check_script_access
from app.crud.generation import create_job, update_job_status
from app.models.credit import CreditAccount
from app.models.episode import Episode
from app.models.generation_batch_request import GenerationBatchRequest
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.pipeline_stage import PipelineStage
from app.models.script import Script
from app.models.subject import Subject
from app.models.user import User
from app.schemas.generation import BatchCreateJobIn, CreateJobIn
from app.schemas.pipeline_contracts import normalize_enrich_prompt
from app.services.credits import reserve
from app.services.pipeline.enrich import build_base_prompt
from app.services.prompt_style import apply_style, find_style, get_styles
from app.services.task_dispatch import enqueue_task

_SINGLE_IDEMPOTENCY_NAMESPACE = "single:"
_BATCH_JOB_IDEMPOTENCY_NAMESPACE = "batch-job:"
_MAX_CLIENT_IDEMPOTENCY_KEY_LENGTH = 128
_BATCH_GENERATION_GROUP_SIZE = 2
_BATCH_GENERATION_GROUP_INTERVAL_SECONDS = 5

_CHARACTER_SHEET_MARKER = "【人物设定图服务端构图硬约束】"
_CHARACTER_SHEET_PROMPT_TEMPLATE = (
    f"{_CHARACTER_SHEET_MARKER}生成单张 {{aspect}} 比例、风格统一的人物设定图，所有视图必须位于同一张图内。"
    "左侧约占 45%，展示人物头肩大图；右侧约占 55%，恰好依次展示正面、标准侧面、背面三幅"
    "等比例全身视图。四个呈现必须是同一人物，身份、脸部、发型、服装和体型完全一致。"
    "三幅全身视图从头到脚完整可见、无遮挡、无裁切，使用浅色纯背景。"
    "画面不得出现文字、标签、水印、边框、分栏线或额外人物。"
)
_CHARACTER_SHEET_NEGATIVE_PROMPT = (
    "额外人物，多余视图，缺少正面视图，缺少标准侧面视图，缺少背面视图，"
    "头部裁切，脚部裁切，身体遮挡，身份不一致，脸部不一致，发型不一致，"
    "服装不一致，体型不一致，文字，标签，水印，边框，分栏线"
)


def _batch_dispatch_available_at(batch_started_at: datetime, created_count: int) -> datetime:
    """按供应商实测容量错峰发布批量生图任务。"""
    group_index = created_count // _BATCH_GENERATION_GROUP_SIZE
    return batch_started_at + timedelta(
        seconds=group_index * _BATCH_GENERATION_GROUP_INTERVAL_SECONDS
    )


def _normalize_idempotency_key(value: str | None) -> str:
    normalized = (value or "").strip()
    if not normalized:
        raise BadRequestError("缺少 Idempotency-Key")
    if len(normalized) > _MAX_CLIENT_IDEMPOTENCY_KEY_LENGTH:
        raise BadRequestError("Idempotency-Key 过长")
    return normalized


def _validate_image_style(style_id: str | None) -> dict | None:
    """校验显式风格，并返回可用于图片生成的风格定义。"""
    if style_id is None:
        return None
    style = find_style(style_id)
    if style is None:
        raise BadRequestError(f"未知风格: {style_id}")
    if "image" not in style["target"]:
        raise BadRequestError(f"风格不支持图片生成: {style_id}")
    return style


def _is_style_template_applied(prompt: str, style: dict) -> bool:
    """仅按模板固定边界识别已包裹 prompt，避免用模糊关键词误判。"""
    template = str(style["prompt_template"])
    if "{prompt}" not in template:
        suffix = f", {template}"
        return prompt.endswith(suffix) and bool(prompt[: -len(suffix)].strip())

    prefix, suffix = template.split("{prompt}", 1)
    if not prefix and not suffix:
        return False
    if not prompt.startswith(prefix) or not prompt.endswith(suffix):
        return False
    end = len(prompt) - len(suffix) if suffix else len(prompt)
    return bool(prompt[len(prefix) : end].strip())


def _resolve_image_prompt(prompt: str, explicit_style: dict | None) -> tuple[str, str, str | None]:
    """解析并应用一次图片风格；兼容前端只提交完整风格 prompt 的情况。"""
    style = explicit_style
    if style is None:
        matches = [
            candidate
            for candidate in get_styles("image")
            if _is_style_template_applied(prompt, candidate)
        ]
        style = matches[0] if len(matches) == 1 else None
    if style is None:
        return prompt, "", None

    style_id = str(style["id"])
    if _is_style_template_applied(prompt, style):
        return prompt, str(style["negative_prompt"]), style_id
    positive_prompt, negative_prompt = apply_style(style_id, prompt, "image")
    return positive_prompt, negative_prompt, style_id


def _apply_character_sheet_constraints(
    prompt: str,
    negative_prompt: str,
    subject_type: str,
    aspect: str,
) -> tuple[str, str]:
    """为人物生成追加一次固定设定图构图约束，并合并固定负面提示。"""
    if subject_type != "character":
        return prompt, negative_prompt

    resolved_prompt = prompt.rstrip()
    if _CHARACTER_SHEET_MARKER not in resolved_prompt:
        character_sheet_prompt = _CHARACTER_SHEET_PROMPT_TEMPLATE.format(aspect=aspect)
        resolved_prompt = f"{resolved_prompt}\n\n{character_sheet_prompt}"

    resolved_negative = negative_prompt.strip()
    if _CHARACTER_SHEET_NEGATIVE_PROMPT not in resolved_negative:
        resolved_negative = ", ".join(
            part for part in (resolved_negative, _CHARACTER_SHEET_NEGATIVE_PROMPT) if part
        )
    return resolved_prompt, resolved_negative


def _resolve_subject_image_aspect(aspect: str, subject_type: str) -> str:
    """将 auto 解析为主体类型对应比例，显式比例保持不变。"""
    normalized = aspect.strip()
    if normalized.lower() == "auto":
        return "3:4" if subject_type == "character" else "16:9"
    return normalized


async def _load_material_script(
    db: AsyncSession, material_id: str
) -> tuple[Materials, Script]:
    """加载画布项目及其关联剧本。"""
    material = (
        await db.execute(
            select(Materials)
            .where(Materials.id == material_id)
            .options(noload(Materials.nodes), noload(Materials.edges))
        )
    ).scalar_one_or_none()
    if material is None:
        raise NotFoundError("画布项目不存在")
    script = (
        await db.execute(
            select(Script)
            .where(Script.id == material.script_id)
            .options(
                noload(Script.episodes),
                noload(Script.subjects),
                noload(Script.parent_subjects),
                noload(Script.materials),
            )
        )
    ).scalar_one_or_none()
    if script is None:
        raise NotFoundError("剧本不存在")
    return material, script


async def _load_credit_account(db: AsyncSession, user: User) -> CreditAccount:
    """查用户积分账户（owner_type=user），不存在则抛 InsufficientCreditsError。"""
    account = (
        await db.execute(
            select(CreditAccount).where(
                CreditAccount.owner_type == "user",
                CreditAccount.owner_id == user.user_id,
            )
        )
    ).scalar_one_or_none()
    if account is None:
        raise InsufficientCreditsError("用户无积分账户")
    return account


async def _create_one_job(
    db: AsyncSession,
    *,
    user: User,
    material: Materials,
    script: Script,
    subject: Subject,
    account: CreditAccount,
    idempotency_key: str,
    prompt: str,
    resolved_prompt: str,
    negative_prompt: str,
    style_id: str | None,
    input_version: int,
    input_client_save_seq: int | None,
    episode_id: str | None,
    episode_snapshot: dict[int, dict] | None = None,
    request_hash: str,
    model: str,
    size: str,
    aspect: str,
    count: int,
    commit: bool = True,
    dispatch_available_at: datetime | None = None,
) -> GenerationJob:
    """创建单条生图任务并投递异步 worker（权限已由调用方校验）。

    返回 GenerationJob ORM 对象。积分预扣失败由 reserve 抛出，事务回滚不落库。
    """
    job_id = new_id()
    input_json: dict = {
        "prompt": prompt,
        "resolved_prompts": {"final": resolved_prompt or prompt},
        "negative_prompt": negative_prompt,
        "style_id": style_id,
        "model": model,
        "size": size,
        "aspect": aspect,
        "count": count,
        "subject_id": subject.id,
        "subject_name": subject.name,
        "subject_version": int(subject.subject_version or 1),
        "enrich_version": int(subject.enrich_version or 1),
        "input_version": input_version,
        "input_client_save_seq": input_client_save_seq,
        "episode_snapshot": episode_snapshot or {},
    }

    async with db.begin_nested():
        # 创建任务记录并 flush 取得 job_id（用于积分幂等 source_id）
        job = await create_job(
            db,
            job_id=job_id,
            material_id=material.id,
            script_id=script.id,
            owner_user_id=user.user_id,
            subject_id=subject.id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            input_version=input_version,
            attempt=1,
            job_type="generate_images",
            input_json=input_json,
            episode_id=episode_id,
        )
        await db.flush()

        # 积分预扣（不足由 reserve 抛 InsufficientCreditsError，事务回滚）
        # commit=False：reservation 与 input_json 回写合并为单事务，单次 commit，
        # 避免「积分已冻结但任务拿不到 reservation_id」的孤立冻结（08 §4.2）。
        # 预扣按张数差异化：estimated = 单张单价 * 生成张数，与 worker settle
        # actual=price_image_gen*count 口径一致，防止低余额用户利用 count=多触发免费生图。
        estimated = settings.price_image_gen * count
        reservation_id = await reserve(
            db,
            account_id=account.id,
            source_type="generation_job",
            source_id=job_id,
            estimated=estimated,
            actor_user_id=user.user_id,
            commit=False,
        )
        # 将 reservation_id 写回 input_json，供 worker settle/release 使用
        input_json["reservation_id"] = reservation_id
        await update_job_status(db, job_id, status="starting", progress=0, input_json=input_json)
        await enqueue_task(
            db,
            task_type="generation.generate_images",
            job_id=job_id,
            attempt=1,
            payload={
                "job_id": job_id,
                "script_id": script.id,
                "subject_id": subject.id,
                "input_version": input_version,
            },
            available_at=dispatch_available_at,
        )
    if commit:
        await db.commit()
        return (await _load_batch_jobs(db, [job_id]))[0]

    return job


async def _subject_episode_snapshot(
    db: AsyncSession, script_id: str, subject: Subject
) -> tuple[str | None, int, int, dict[int, dict]]:
    episode_nos = sorted(int(no) for no in (subject.episode_nos or []) if no is not None)
    stmt = select(Episode).where(Episode.script_id == script_id)
    if episode_nos:
        stmt = stmt.where(Episode.episode_no.in_(episode_nos))
    episodes = list(
        (
            await db.execute(
            stmt.options(noload(Episode.script), noload(Episode.scenes))
            .order_by(Episode.episode_no.asc())
            )
        )
        .scalars()
        .all()
    )
    if not episodes:
        return None, 1, 0, {}
    primary = episodes[0]
    snapshot = {
        int(episode.episode_no): {
            "episode_id": episode.episode_id,
            "episode_version": int(episode.episode_version or 1),
            "client_save_seq": int(episode.client_save_seq or 0),
        }
        for episode in episodes
    }
    return (
        primary.episode_id,
        int(primary.episode_version or 1),
        int(primary.client_save_seq or 0),
        snapshot,
    )


def _validate_twelve_section_prompt(prompt: str) -> str:
    try:
        return normalize_enrich_prompt(prompt)
    except ValueError as exc:
        raise ConflictError(f"enrich resolved prompt 无效：{exc}") from exc


async def _load_server_resolved_prompt(
    db: AsyncSession,
    script_id: str,
    subject_id: str,
    input_version: int,
    input_client_save_seq: int | None = None,
    episode_snapshot: dict[int, dict] | None = None,
    subject_version: int | None = None,
    enrich_version: int | None = None,
) -> str:
    stage = (
        await db.execute(
            select(PipelineStage).where(
                PipelineStage.script_id == script_id,
                PipelineStage.stage == "enrich",
                PipelineStage.status == "done",
            ).options(noload(PipelineStage.script))
        )
    ).scalar_one_or_none()
    detail = stage.detail if stage else None
    if not detail:
        raise ConflictError("enrich 快照不是当前输入版本")
    prompt_entry = (detail.get("prompts") or {}).get(subject_id, {})
    if subject_version is not None and prompt_entry.get("subject_version") != subject_version:
        raise ConflictError("enrich 快照不是当前输入版本")
    if enrich_version is not None and prompt_entry.get("enrich_version") != enrich_version:
        raise ConflictError("enrich 快照不是当前输入版本")
    stored_snapshot = prompt_entry.get("episode_snapshot") or {}
    expected_snapshot = episode_snapshot or {}
    if stored_snapshot:
        normalized_stored = {int(key): value for key, value in stored_snapshot.items()}
        if normalized_stored != expected_snapshot:
            raise ConflictError("enrich 快照不是当前输入版本")
        episode_ids = [str(item["episode_id"]) for item in normalized_stored.values()]
        current_rows = list(
            (
                await db.execute(
                    select(Episode)
                    .where(
                        Episode.script_id == script_id,
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
        if current_snapshot != normalized_stored:
            raise ConflictError("enrich 快照不是当前输入版本")
    elif (
        detail.get("input_version") != input_version
        or (
            input_client_save_seq is not None
            and "input_client_save_seq" in detail
            and detail.get("input_client_save_seq") != input_client_save_seq
        )
    ):
        raise ConflictError("enrich 快照不是当前输入版本")
    prompt = (prompt_entry.get("resolved_prompts") or {}).get("final")
    if not isinstance(prompt, str) or not prompt.strip():
        raise ConflictError("缺少服务端 enrich resolved prompt")
    return _validate_twelve_section_prompt(prompt)


def _request_hash(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def _find_idempotent_job(
    db: AsyncSession, owner_user_id: str, idempotency_key: str
) -> GenerationJob | None:
    return (
        await db.execute(
            select(GenerationJob).where(
                GenerationJob.owner_user_id == owner_user_id,
                GenerationJob.idempotency_key == idempotency_key,
            ).options(noload(GenerationJob.material))
        )
    ).scalar_one_or_none()


async def _find_batch_request(
    db: AsyncSession, owner_user_id: str, idempotency_key: str
) -> GenerationBatchRequest | None:
    return (
        await db.execute(
            select(GenerationBatchRequest).where(
                GenerationBatchRequest.owner_user_id == owner_user_id,
                GenerationBatchRequest.idempotency_key == idempotency_key,
            )
        )
    ).scalar_one_or_none()


async def _load_batch_jobs(db: AsyncSession, job_ids: list[str]) -> list[GenerationJob]:
    if not job_ids:
        return []
    result = await db.execute(
        select(GenerationJob)
        .where(GenerationJob.job_id.in_(job_ids))
        .options(noload(GenerationJob.material))
    )
    loaded = list(result.scalars().all())
    by_id = {job.job_id: job for job in loaded}
    if len(loaded) != len(job_ids) or set(by_id) != set(job_ids):
        raise RuntimeError("generation job reload is incomplete")
    return [by_id[job_id] for job_id in job_ids]


async def _load_batch_manifest(
    db: AsyncSession, batch: GenerationBatchRequest
) -> tuple[list[GenerationJob], list[str], list[dict]]:
    return (
        await _load_batch_jobs(db, batch.job_ids or []),
        list(batch.skipped_subject_ids or []),
        list(batch.failed_items or []),
    )


async def _create_batch_request(
    db: AsyncSession,
    *,
    owner_user_id: str,
    idempotency_key: str,
    request_hash: str,
    target_subject_ids: list[str],
) -> GenerationBatchRequest:
    existing = await _find_batch_request(db, owner_user_id, idempotency_key)
    if existing is not None:
        return existing
    row = GenerationBatchRequest(
        owner_user_id=owner_user_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        target_subject_ids=target_subject_ids,
        job_ids=[],
        skipped_subject_ids=[],
        failed_items=[],
        status="created",
    )
    try:
        async with db.begin_nested():
            db.add(row)
            await db.flush()
    except IntegrityError:
        return await _find_batch_request(db, owner_user_id, idempotency_key)  # type: ignore[return-value]
    return row


async def create_generation_job(
    db: AsyncSession, *, user: User, payload: CreateJobIn, idempotency_key: str
):
    """创建生图任务，预扣积分并投递异步 worker。返回 GenerationJob ORM 对象。"""
    idempotency_key = f"{_SINGLE_IDEMPOTENCY_NAMESPACE}{_normalize_idempotency_key(idempotency_key)}"

    # 1. 校验画布项目归属与剧本访问权限（统一走 check_script_access 两轴权限模型）
    material, script = await _load_material_script(db, payload.material_id)
    await check_script_access(db, user, script, action=Action.RUN_AI)

    # 2. 校验主体归属（主体必须属于同一剧本）
    subject = (
        await db.execute(
            select(Subject)
            .where(Subject.id == payload.subject_id)
            .options(
                noload(Subject.script),
                noload(Subject.parent_subject),
                noload(Subject.asset_entries),
                noload(Subject.image_entry),
            )
        )
    ).scalar_one_or_none()
    if subject is None:
        raise NotFoundError("主体不存在")
    if subject.script_id != script.id:
        raise ForbiddenError("主体不存在或不属于该剧本")
    # 声音主体不生图（06 §6.6），单任务创建显式拒绝，返回 400
    if subject.type == "sound":
        raise BadRequestError("声音主体不支持生图")

    aspect = _resolve_subject_image_aspect(payload.aspect, subject.type)
    episode_id, input_version, input_client_save_seq, episode_snapshot = (
        await _subject_episode_snapshot(db, script.id, subject)
    )
    prompt = payload.prompt or build_base_prompt(subject)
    if not prompt.strip():
        raise BadRequestError("主体缺少描述，无法生图")
    if payload.prompt is not None:
        # 客户端显式 prompt 是用户本次确认的最终输入，不再套用 enrich 十二段格式校验。
        resolved_prompt = prompt
    else:
        # 兼容尚未升级的客户端：缺省时继续使用服务端 enrich 快照。
        resolved_prompt = await _load_server_resolved_prompt(
            db,
            script.id,
            subject.id,
            input_version,
            input_client_save_seq,
            episode_snapshot,
            int(subject.subject_version or 1),
            int(subject.enrich_version or 1),
        )
    request_payload = {
        "material_id": payload.material_id,
        "script_id": script.id,
        "subject_id": subject.id,
        "model": payload.model,
        "size": payload.size,
        "aspect": aspect,
        "count": payload.count,
        "prompt": prompt,
        "resolved_prompt": resolved_prompt,
        "input_version": input_version,
        "input_client_save_seq": input_client_save_seq,
        "episode_snapshot": episode_snapshot,
        "subject_version": int(subject.subject_version or 1),
        "enrich_version": int(subject.enrich_version or 1),
    }
    if payload.style_id is not None:
        request_payload["style_id"] = payload.style_id
    req_hash = _request_hash(request_payload)
    existing = await _find_idempotent_job(db, user.user_id, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise ConflictError("Idempotency-Key 已用于不同请求")
        return existing

    explicit_style = _validate_image_style(payload.style_id)
    resolved_prompt, negative_prompt, resolved_style_id = _resolve_image_prompt(
        resolved_prompt, explicit_style
    )
    resolved_prompt, negative_prompt = _apply_character_sheet_constraints(
        resolved_prompt,
        negative_prompt,
        subject.type,
        aspect,
    )

    # 3. 查用户积分账户
    account = await _load_credit_account(db, user)

    # 4. 创建任务 + 预扣 + 投递
    try:
        return await _create_one_job(
            db,
            user=user,
            material=material,
            script=script,
            subject=subject,
            account=account,
            idempotency_key=idempotency_key,
            prompt=prompt,
            resolved_prompt=resolved_prompt,
            negative_prompt=negative_prompt,
            style_id=resolved_style_id,
            input_version=input_version,
            input_client_save_seq=input_client_save_seq,
            episode_id=episode_id,
            episode_snapshot=episode_snapshot,
            request_hash=req_hash,
            model=payload.model,
            size=payload.size,
            aspect=aspect,
            count=payload.count,
        )
    except IntegrityError:
        raced = await _find_idempotent_job(db, user.user_id, idempotency_key)
        if raced is not None and raced.request_hash == req_hash:
            raced_job_id = raced.job_id
            await db.commit()
            return (await _load_batch_jobs(db, [raced_job_id]))[0]
        raise ConflictError("Idempotency-Key 已用于不同请求") from None


async def create_batch_generation_jobs(
    db: AsyncSession, *, user: User, payload: BatchCreateJobIn, idempotency_key: str | None = None
):
    """一键设计 N — 批量为未设计主体生成图（消耗积分），06 §3.5。

    - 显式传入 subject_ids 时按列表生成；为空时自动选取该剧本下
      review_status='not_designed' 且 type != 'sound' 的主体（声音不生图，06 §6.6）。
    - 权限校验仅做一次（同一剧本）。
    - 每个主体独立预扣积分（source_id=job_id），worker 各自 settle/release。
    - 循环内单条创建/预扣失败时回滚当前任务事务并收集到 failed，不整体冒泡，
      使客户端可知部分已运行（已 commit 的任务保留）。
    - 返回 (created_jobs, skipped_subject_ids, failed_items)：
      skipped=声音类型/无描述等前置跳过；failed=运行期创建/预扣失败。
    """
    idempotency_key = _normalize_idempotency_key(idempotency_key)

    material, script = await _load_material_script(db, payload.material_id)
    await check_script_access(db, user, script, action=Action.RUN_AI)
    batch_payload = {
        "material_id": material.id,
        "script_id": script.id,
        "subject_ids": list(payload.subject_ids) if payload.subject_ids else None,
        "prompts": payload.prompts,
        "model": payload.model,
        "size": payload.size,
        "aspect": payload.aspect,
        "count": payload.count,
    }
    if payload.style_id is not None:
        batch_payload["style_id"] = payload.style_id
    batch_hash = _request_hash(batch_payload)
    existing_batch = await _find_batch_request(db, user.user_id, idempotency_key)
    if existing_batch is not None:
        if existing_batch.request_hash != batch_hash:
            raise ConflictError("Idempotency-Key 已用于不同请求")
        return await _load_batch_manifest(db, existing_batch)
    explicit_style = _validate_image_style(payload.style_id)
    account = await _load_credit_account(db, user)

    # 确定目标主体集合
    if payload.subject_ids:
        requested_subject_ids = list(dict.fromkeys(payload.subject_ids))
        rows = list(
            (
                await db.execute(
                    select(Subject).where(Subject.id.in_(requested_subject_ids))
                )
            ).scalars().all()
        )
        by_id = {subject.id: subject for subject in rows}
        missing = [subject_id for subject_id in requested_subject_ids if subject_id not in by_id]
        if missing:
            raise NotFoundError("主体不存在")
        foreign = [subject_id for subject_id, subject in by_id.items() if subject.script_id != script.id]
        if foreign:
            raise ForbiddenError("主体不属于该剧本")
        subjects = [by_id[subject_id] for subject_id in requested_subject_ids]
    else:
        # 自动选取未设计且非声音的主体
        subjects = list(
            (
                await db.execute(
                    select(Subject).where(
                        Subject.script_id == script.id,
                        Subject.review_status == "not_designed",
                        Subject.type != "sound",
                    ).order_by(Subject.created_at.asc())
                )
            ).scalars().all()
        )

    target_subject_ids = {subject.id for subject in subjects}
    unknown_prompt_ids = set(payload.prompts or {}) - target_subject_ids
    if unknown_prompt_ids:
        raise BadRequestError("prompts 包含非目标主体")

    batch = await _create_batch_request(
        db,
        owner_user_id=user.user_id,
        idempotency_key=idempotency_key,
        request_hash=batch_hash,
        target_subject_ids=[subject.id for subject in subjects],
    )
    if batch.request_hash != batch_hash:
        raise ConflictError("Idempotency-Key 已用于不同请求")
    if batch.job_ids or batch.skipped_subject_ids or batch.failed_items:
        return await _load_batch_manifest(db, batch)

    created_job_ids: list[str] = []
    skipped: list[str] = []
    failed: list[dict] = []
    batch_started_at = datetime.now(UTC)
    for subject in subjects:
        # 声音类型不生图
        if subject.type == "sound":
            skipped.append(subject.id)
            continue
        client_prompt = (payload.prompts or {}).get(subject.id)
        # 旧客户端未提供 prompt 时，仍按主体描述构造基础 prompt。
        prompt = client_prompt or build_base_prompt(subject)
        if not prompt.strip():
            skipped.append(subject.id)
            continue
        try:
            episode_id, input_version, input_client_save_seq, episode_snapshot = (
                await _subject_episode_snapshot(db, script.id, subject)
            )
            if client_prompt is not None:
                resolved_prompt = prompt
            else:
                resolved_prompt = await _load_server_resolved_prompt(
                    db,
                    script.id,
                    subject.id,
                    input_version,
                    input_client_save_seq,
                    episode_snapshot,
                    int(subject.subject_version or 1),
                    int(subject.enrich_version or 1),
                )
            resolved_prompt, negative_prompt, resolved_style_id = _resolve_image_prompt(
                resolved_prompt, explicit_style
            )
            aspect = _resolve_subject_image_aspect(payload.aspect, subject.type)
            resolved_prompt, negative_prompt = _apply_character_sheet_constraints(
                resolved_prompt,
                negative_prompt,
                subject.type,
                aspect,
            )
            job = await _create_one_job(
                db,
                user=user,
                material=material,
                script=script,
                subject=subject,
                account=account,
                idempotency_key=f"{_BATCH_JOB_IDEMPOTENCY_NAMESPACE}{batch.id}:{subject.id}",
                prompt=prompt,
                resolved_prompt=resolved_prompt,
                negative_prompt=negative_prompt,
                style_id=resolved_style_id,
                input_version=input_version,
                input_client_save_seq=input_client_save_seq,
                episode_id=episode_id,
                episode_snapshot=episode_snapshot,
                request_hash=batch_hash,
                model=payload.model,
                size=payload.size,
                aspect=aspect,
                count=payload.count,
                commit=False,
                dispatch_available_at=_batch_dispatch_available_at(
                    batch_started_at,
                    len(created_job_ids),
                ),
            )
            job_id = job.job_id
            created_job_ids.append(job_id)
            batch.job_ids = [*batch.job_ids, job_id]
            await db.flush()
        except Exception:  # noqa: BLE001 - 收集失败而非冒泡
            failed.append(
                {
                    "subject_id": subject.id,
                    "subject_name": subject.name,
                    "reason": "generation_create_failed",
                }
            )

    batch.skipped_subject_ids = skipped
    batch.failed_items = failed
    batch.status = "failed" if failed else "created"
    await db.flush()
    await db.commit()
    created = await _load_batch_jobs(db, created_job_ids)
    return created, skipped, failed
