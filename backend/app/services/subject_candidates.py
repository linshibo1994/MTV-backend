"""新增主体候选的人在回路状态机。"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import BadRequestError, ConflictError, NotFoundError
from app.core.ids import new_id
from app.models.script import Script
from app.models.subject import Subject
from app.models.subject_candidate import SubjectCandidate
from app.services.subject_candidate_evidence import normalize_candidate_evidence


def _stored_subject_type(value: str) -> str:
    return "scene" if value == "background" else value


def _resolution_request_hash(
    *,
    action: str,
    subject_id: str | None,
    visual_style: str | None,
) -> str:
    payload = json.dumps(
        {
            "action": action,
            "subject_id": subject_id,
            "visual_style": visual_style,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


async def list_candidates(
    db: AsyncSession,
    *,
    script_id: str,
    status: str | None,
    limit: int,
    offset: int,
) -> tuple[list[SubjectCandidate], int]:
    conditions = [SubjectCandidate.script_id == script_id]
    if status is not None:
        conditions.append(SubjectCandidate.status == status)
    total = int(
        (await db.execute(select(func.count()).select_from(SubjectCandidate).where(*conditions))).scalar_one()
    )
    rows = (
        await db.execute(
            select(SubjectCandidate)
            .where(*conditions)
            .order_by(SubjectCandidate.created_at.desc(), SubjectCandidate.id.asc())
            .limit(limit)
            .offset(offset)
        )
    ).scalars().all()
    return list(rows), total


async def create_candidate(
    db: AsyncSession,
    *,
    script_id: str,
    subject_type: str,
    name: str,
    description: str | None,
    aliases: list[str],
    episode_nos: list[int],
    evidence: list[dict[str, object]],
    source_run_id: str | None = None,
) -> SubjectCandidate:
    await db.execute(select(Script.id).where(Script.id == script_id).with_for_update())
    stored_type = _stored_subject_type(subject_type)
    existing = (
        await db.execute(
            select(SubjectCandidate).where(
                SubjectCandidate.script_id == script_id,
                SubjectCandidate.subject_type == stored_type,
                SubjectCandidate.pipeline_name == name,
                SubjectCandidate.status == "pending",
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing
    candidate = SubjectCandidate(
        id=new_id(),
        script_id=script_id,
        source_run_id=source_run_id,
        subject_type=stored_type,
        name=name,
        pipeline_name=name,
        description=description,
        aliases=list(dict.fromkeys(alias.strip() for alias in aliases if alias.strip())),
        episode_nos=sorted(set(episode_nos)),
        evidence=normalize_candidate_evidence(evidence),
        status="pending",
        version=1,
    )
    db.add(candidate)
    await db.flush()
    return candidate


async def resolve_candidate(
    db: AsyncSession,
    *,
    script_id: str,
    candidate_id: str,
    expected_version: int,
    action: str,
    actor_user_id: str,
    subject_id: str | None = None,
    visual_style: str | None = None,
) -> tuple[SubjectCandidate, bool]:
    await db.execute(select(Script.id).where(Script.id == script_id).with_for_update())
    candidate = (
        await db.execute(
            select(SubjectCandidate)
            .where(
                SubjectCandidate.id == candidate_id,
                SubjectCandidate.script_id == script_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if candidate is None:
        raise NotFoundError("主体候选不存在")
    request_hash = _resolution_request_hash(
        action=action,
        subject_id=subject_id,
        visual_style=visual_style,
    )
    if candidate.status != "pending":
        if (
            candidate.version == expected_version + 1
            and candidate.resolution_request_hash == request_hash
        ):
            return candidate, True
        raise ConflictError("主体候选已被处理，且当前请求与已完成操作不同")
    if candidate.version != expected_version:
        raise ConflictError("主体候选版本已变化，请刷新后重试")

    linked: Subject | None = None
    if action == "ignore":
        candidate.status = "ignored"
    elif action == "link_existing":
        if not subject_id:
            raise BadRequestError("关联已有主体时必须提供 subject_id")
        linked = (
            await db.execute(
                select(Subject).where(
                    Subject.id == subject_id,
                    Subject.script_id == script_id,
                    Subject.type == candidate.subject_type,
                )
            )
        ).scalar_one_or_none()
        if linked is None:
            raise BadRequestError("目标主体不存在、类型不匹配或不属于当前剧本")
    elif action == "create_subject":
        linked = (
            await db.execute(
                select(Subject).where(
                    Subject.script_id == script_id,
                    Subject.type == candidate.subject_type,
                    Subject.pipeline_name == candidate.pipeline_name,
                )
            )
        ).scalar_one_or_none()
        if linked is None:
            linked = Subject(
                id=new_id(),
                script_id=script_id,
                type=candidate.subject_type,
                name=candidate.name,
                pipeline_name=candidate.pipeline_name,
                description=candidate.description,
                visual_style=visual_style,
                subject_version=1,
                enrich_version=1,
                is_child=True,
                is_parent=False,
                episode_nos=candidate.episode_nos,
                is_active=True,
                review_status="not_designed",
            )
            db.add(linked)
            await db.flush()
    else:
        raise BadRequestError("不支持的候选操作")

    if linked is not None:
        candidate.linked_subject_id = linked.id
        candidate.status = "linked"
    candidate.version += 1
    candidate.resolved_by_user_id = actor_user_id
    candidate.resolution_action = action
    candidate.resolution_request_hash = request_hash
    candidate.resolved_at = datetime.now(UTC)
    await db.flush()
    return candidate, False
