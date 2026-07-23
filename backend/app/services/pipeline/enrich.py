"""enrich 阶段 — 并发生成、可复用且可降级的视觉主体 prompt。"""

import asyncio
import logging
import math
from typing import TypedDict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.episode import Episode
from app.models.pipeline_job import PipelineJob
from app.models.pipeline_stage import PipelineStage
from app.models.subject import Subject
from app.providers import get_llm_provider
from app.schemas.pipeline_contracts import (
    EnrichOutput,
    complete_json,
    normalize_enrich_prompt,
    should_enrich_subject,
)
from app.services.pipeline import (
    commit_stage_progress,
    get_run_stage_output,
    input_episode_is_current,
    set_stage,
)

logger = logging.getLogger(__name__)

class _ResolvedSubject(TypedDict):
    subject_id: str
    target_enrich_version: int
    entry: dict[str, object]

# ---- 固定模板 prompt（06 文档第 5 节，4 插槽）----
_BASE_PROMPT_TEMPLATE = (
    "为以下主体设计一张清晰可用于短剧视觉资产库的参考图。\n"
    "主体名称：{subject_name}\n"
    "主体类型：{subject_type_label}\n"
    "主体描述：{subject_description}\n"
    "要求：主体清晰、特征稳定、背景干扰少、适合后续多图参考生成。"
)

_TYPE_LABELS = {
    "character": "角色",
    "scene": "场景",
    "prop": "道具",
    "sound": "声音",
}

# ---- 扩展 prompt 提示词骨架（06 文档第 12.3 节）----
_SYSTEM = "你是一名专业产品/角色摄影指导。请将主体的简单描述扩展为专业摄影级生图 prompt。"

_USER_TEMPLATE = """主体信息:
- 名称: {subject_name}
- 类型: {subject_type}
- 描述: {subject_description}

固定参考模板 prompt（请在此模板上下文基础上扩展，保持主体一致性与背景稳定）:
---
{base_prompt}
---

请输出一段 300～600 字的紧凑中文生图 prompt。应尽量覆盖外形结构、明确材质、
使用痕迹、摆放与物理支撑、时代、构图、写实画风、三点柔光白底、无水印、
文字降级和摄影参数。可以自由组织文字，不强制标题、编号、段落数量或顺序。
不要在 JSON 外添加说明。

输出格式: JSON，字段 final 为完整扩展 prompt 文本。
输出 JSON:
{{
  "schema_version": "pipeline.v1",
  "resolved_prompts": {{ "final": "<完整扩展 prompt 文本>" }}
}}
"""


def build_base_prompt(subject: Subject) -> str:
    """构造固定模板 prompt（06 文档第 5 节，enrich 的输入）。"""
    return _BASE_PROMPT_TEMPLATE.format(
        subject_name=subject.name,
        subject_type_label=_TYPE_LABELS.get(subject.type, subject.type),
        subject_description=subject.description or "",
    )


def _reusable_prompt(
    entry: object,
    *,
    enrich_version: int,
) -> str | None:
    """历史 prompt 只按主体 ID（由外层字典保证）和 enrich_version 复用。"""
    if not isinstance(entry, dict):
        return None
    # 基础模板只是供应商临时失败时的可用降级结果，不能永久阻止后续重试生成。
    if entry.get("source") == "base_fallback":
        return None
    raw_version = entry.get("enrich_version")
    if not isinstance(raw_version, (int, str)):
        return None
    try:
        entry_version = int(raw_version)
    except (TypeError, ValueError):
        return None
    resolved = entry.get("resolved_prompts")
    final = resolved.get("final") if isinstance(resolved, dict) else None
    if entry_version != enrich_version or not isinstance(final, str) or not final.strip():
        return None
    return normalize_enrich_prompt(final)


def _merge_prompt_history(
    target: dict[str, dict],
    payload: object,
) -> None:
    """将较新的历史输出合入缓存，已有主体不被更老结果覆盖。"""
    if not isinstance(payload, dict):
        return
    prompts = payload.get("prompts")
    if not isinstance(prompts, dict):
        return
    for subject_id, entry in prompts.items():
        if isinstance(subject_id, str) and isinstance(entry, dict):
            target.setdefault(subject_id, entry)


async def _load_prompt_history(db: AsyncSession, script_id: str) -> dict[str, dict]:
    """读取公共阶段快照及最近成功 run 的私有输出，避免重复调用模型。"""
    history: dict[str, dict] = {}

    stage_result = await db.execute(
        select(PipelineStage.detail).where(
            PipelineStage.script_id == script_id,
            PipelineStage.stage == "enrich",
        )
    )
    stage_detail = stage_result.scalar_one_or_none()

    jobs_result = await db.execute(
        select(PipelineJob.output_json)
        .where(
            PipelineJob.script_id == script_id,
            PipelineJob.stage == "enrich",
            PipelineJob.status.in_(["done", "ready"]),
            PipelineJob.output_json.is_not(None),
        )
        .order_by(PipelineJob.created_at.desc())
        .limit(5)
    )
    for payload in jobs_result.scalars().all():
        _merge_prompt_history(history, payload)
    _merge_prompt_history(history, stage_detail)
    return history


def _episode_snapshot(
    subject: Subject,
    episode_snapshot_by_no: dict[int, dict[str, object]],
) -> dict[str, dict[str, object]]:
    episode_nos = sorted(
        int(no)
        for no in (subject.episode_nos or [])
        if int(no) in episode_snapshot_by_no
    )
    return {str(no): episode_snapshot_by_no[no] for no in episode_nos}


async def run_enrich(
    db: AsyncSession,
    script_id: str,
    *,
    episode_id: str | None = None,
    input_version: int | None = None,
    input_client_save_seq: int | None = None,
    provider=None,
    provider_attempt: int | None = None,
    run_id: str | None = None,
    commit: bool = False,
    max_concurrency: int | None = None,
) -> int | dict:
    """并发增强视觉主体，返回已处理数量或 run 私有阶段产物。

    每个未命中缓存的主体最多调用模型一次。单项失败时写入可直接使用的基础
    prompt，不让失败扩散为阶段重试。``max_concurrency`` 可由调用方按供应商
    限流能力调整，默认 6。
    """
    # 阶段事务仍由 worker 统一提交；两个参数仅保留兼容既有调用。Enrich 单项
    # 最多外呼一次，不再把供应商格式异常升级为持久 attempt 重试。
    del commit, provider_attempt
    concurrency = max_concurrency or settings.pipeline_enrich_concurrency
    if concurrency < 1:
        raise ValueError("max_concurrency 必须大于 0")
    if not await input_episode_is_current(
        db, script_id, episode_id, input_version, input_client_save_seq
    ):
        return 0

    if run_id is None:
        subjects_result = await db.execute(
            select(Subject).where(
                Subject.script_id == script_id,
                Subject.is_active.is_(True),
            )
        )
        subjects = list(subjects_result.scalars().all())
    else:
        merge_output = await get_run_stage_output(db, run_id, "merge") or {}
        if not merge_output:
            raise ValueError("当前流水线 run 缺少 merge 暂存输出")
        subjects = [
            Subject(
                id=item["id"],
                script_id=script_id,
                name=item["name"],
                type=item["type"],
                description=item.get("description"),
                episode_nos=item.get("episode_nos") or [],
                subject_version=int(item.get("subject_version") or 1),
                enrich_version=int(item.get("enrich_version") or 1),
                is_active=True,
            )
            for item in merge_output.get("subjects", [])
        ]
    visual_subjects = [subject for subject in subjects if should_enrich_subject(subject.type)]
    total = len(visual_subjects)

    provider = provider or get_llm_provider()
    episode_rows = list(
        (
            await db.execute(
                select(Episode).where(Episode.script_id == script_id)
            )
        )
        .scalars()
        .all()
    )
    episode_rows = [episode for episode in episode_rows if hasattr(episode, "episode_no")]
    episode_snapshot_by_no = {
        int(episode.episode_no): {
            "episode_id": episode.episode_id,
            "episode_version": int(episode.episode_version or 1),
            "client_save_seq": int(episode.client_save_seq or 0),
        }
        for episode in episode_rows
    }

    prompt_history = await _load_prompt_history(db, script_id)

    # Worker 的 run 模式在进入服务前没有业务写；结束只读事务，避免等待模型的
    # 一两分钟内长期占用数据库连接。非 run 兼容旧的“由调用方统一提交”契约。
    if run_id is not None:
        await db.commit()

    semaphore = asyncio.Semaphore(concurrency)
    progress_lock = asyncio.Lock()
    provider_call_count = 0
    completed_count = 0
    completed_reused_count = 0
    completed_fallback_count = 0
    completed_prompts: dict[str, dict[str, object]] = {}
    checkpoint_step = max(1, math.ceil(total / 10))

    async def resolve_prompt(subject: Subject) -> _ResolvedSubject:
        nonlocal provider_call_count
        base_prompt = build_base_prompt(subject)
        current_enrich_version = int(subject.enrich_version or 1)
        cached = prompt_history.get(subject.id)
        reusable_prompt = _reusable_prompt(
            cached,
            enrich_version=current_enrich_version,
        )
        if reusable_prompt is not None:
            final_prompt = reusable_prompt
            source = "reused"
            fallback_error_type = None
            target_enrich_version = current_enrich_version
        else:
            target_enrich_version = (
                current_enrich_version
                if run_id is not None
                else current_enrich_version + 1
            )
            try:
                async with semaphore:
                    provider_call_count += 1
                    contract = await complete_json(
                        provider,
                        _SYSTEM,
                        _USER_TEMPLATE.format(
                            subject_name=subject.name,
                            subject_type=subject.type,
                            subject_description=subject.description or "",
                            base_prompt=base_prompt,
                        ),
                        EnrichOutput,
                        max_attempts=1,
                    )
                final_prompt = contract.resolved_prompts.final
                source = "generated"
                fallback_error_type = None
            except Exception as exc:  # noqa: BLE001
                final_prompt = base_prompt
                source = "base_fallback"
                fallback_error_type = getattr(exc, "error_type", type(exc).__name__)
                logger.warning(
                    "enrich 主体降级为基础 prompt: script_id=%s subject_id=%s error_type=%s",
                    script_id,
                    subject.id,
                    fallback_error_type,
                )

        entry: dict[str, object] = {
            "subject_version": int(subject.subject_version or 1),
            "enrich_version": target_enrich_version,
            "resolved_prompts": {"final": final_prompt},
            "episode_snapshot": _episode_snapshot(subject, episode_snapshot_by_no),
            "source": source,
        }
        if fallback_error_type is not None:
            entry["fallback_error_type"] = fallback_error_type
        return _ResolvedSubject(
            subject_id=subject.id,
            target_enrich_version=target_enrich_version,
            entry=entry,
        )

    async def resolve_and_checkpoint(subject: Subject) -> _ResolvedSubject:
        nonlocal completed_count, completed_fallback_count, completed_reused_count
        result = await resolve_prompt(subject)
        async with progress_lock:
            entry = result["entry"]
            if isinstance(entry, dict):
                completed_count += 1
                completed_reused_count += int(entry.get("source") == "reused")
                completed_fallback_count += int(entry.get("source") == "base_fallback")
                completed_prompts[str(result["subject_id"])] = entry
            checkpoint_current = completed_count
            checkpoint_reused = completed_reused_count
            checkpoint_fallback = completed_fallback_count
            checkpoint_provider_calls = provider_call_count

            # 生产 worker 使用真实 AsyncSession；独立短事务使轮询能立即看到进度，
            # 同时不提交本服务会话中的业务结果。最多发布十个中间检查点，避免主体
            # 数量很大时制造过多小事务；测试替身无需连接外部数据库。
            should_checkpoint = (
                checkpoint_current == total or checkpoint_current % checkpoint_step == 0
            )
            if isinstance(db, AsyncSession) and should_checkpoint:
                try:
                    await commit_stage_progress(
                        script_id,
                        "enrich",
                        status="running",
                        current=checkpoint_current,
                        total=total,
                        detail={
                            "count": total,
                            "completed_count": checkpoint_current,
                            "reused_count": checkpoint_reused,
                            "fallback_count": checkpoint_fallback,
                            "provider_call_count": checkpoint_provider_calls,
                            # 使 worker 重试能复用已经成功生成的主体，而不只是显示计数。
                            "prompts": dict(completed_prompts),
                            "input_version": input_version,
                            "input_client_save_seq": input_client_save_seq,
                        },
                    )
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "enrich 进度检查点写入失败: script_id=%s current=%s total=%s",
                        script_id,
                        checkpoint_current,
                        total,
                        exc_info=True,
                    )
        return result

    results = await asyncio.gather(
        *(resolve_and_checkpoint(subject) for subject in visual_subjects)
    )
    result_by_subject_id = {str(item["subject_id"]): item for item in results}

    # 按主体原始顺序组装，保证 JSON 输出稳定，便于缓存、审计与测试。
    prompts: dict[str, dict[str, object]] = {}
    target_enrich_versions: dict[str, int] = {}
    for subject in visual_subjects:
        result = result_by_subject_id[subject.id]
        prompts[subject.id] = result["entry"]
        target_enrich_versions[subject.id] = int(result["target_enrich_version"])

    reused_count = sum(entry.get("source") == "reused" for entry in prompts.values())
    fallback_count = sum(
        entry.get("source") == "base_fallback" for entry in prompts.values()
    )

    if not await input_episode_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
        lock=True,
    ):
        return 0

    if run_id is None:
        for subject in visual_subjects:
            subject.enrich_version = target_enrich_versions[subject.id]

    public_detail: dict[str, object] = {
        "count": total,
        "completed_count": total,
        "reused_count": reused_count,
        "fallback_count": fallback_count,
        "provider_call_count": provider_call_count,
        "input_version": input_version,
        "input_client_save_seq": input_client_save_seq,
    }
    if run_id is None:
        public_detail["prompts"] = prompts
    await set_stage(
        db,
        script_id,
        "enrich",
        status="running",
        current=total,
        total=total,
        detail=public_detail,
    )
    if run_id is not None:
        return {**public_detail, "prompts": prompts}
    return total
