"""正式镜头组的部分更新与版本保护。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.errors import BadRequestError, ConflictError, ForbiddenError, NotFoundError
from app.models.asset_entry import AssetEntry
from app.models.episode import Episode
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import FINAL_SHOT_GROUP_UNIT_PREFIX, ShotGroup
from app.models.subject import Subject
from app.models.video_generation_job import (
    VIDEO_SUBMISSION_MANUAL_REVIEW_ERROR_TYPE,
    VideoGenerationJob,
)
from app.services.shot_group_merge import _rewrite_shot_line

_ACTIVE_VIDEO_STATUSES = ("waiting_dependency", "starting", "processing")
_PROMPT_TIMELINE_PATTERN = re.compile(r"【镜头\s*(?P<display_no>\d+)\s*:\s*\d+\s*s\s*[～~\-—至]\s*\d+\s*s】")
_MATERIAL_KEYS = ("character", "background", "prop", "other")


@dataclass(frozen=True)
class ShotGroupUpdateResult:
    """镜头组更新后的版本与审计摘要。"""

    episode_version: int
    changed_fields: tuple[str, ...]


def _allocate_durations(shots: list[Shot], target_seconds: int) -> list[int]:
    """按原时长比例重分配整数秒，并保证每个子镜头至少 1 秒。"""

    if not shots:
        raise ConflictError("镜头组中没有可编辑的子镜头")
    if target_seconds < len(shots):
        raise BadRequestError(f"目标时长至少需要 {len(shots)} 秒，以保证每个子镜头不少于 1 秒")

    original = [max(1, int(shot.end_seconds) - int(shot.start_seconds)) for shot in shots]
    remaining = target_seconds - len(shots)
    extras = [duration - 1 for duration in original]
    extras_total = sum(extras)
    if extras_total == 0:
        extras = [1] * len(shots)
        extras_total = len(shots)

    allocations: list[int] = []
    remainders: list[tuple[int, int]] = []
    allocated = 0
    for index, weight in enumerate(extras):
        quotient, remainder = divmod(remaining * weight, extras_total)
        allocations.append(1 + quotient)
        allocated += quotient
        remainders.append((remainder, index))

    for _, index in sorted(remainders, key=lambda item: (-item[0], item[1]))[: remaining - allocated]:
        allocations[index] += 1
    return allocations


def _rewrite_prompt_timeline(prompt: str, shots: list[Shot]) -> str:
    """把 Prompt 中已有的镜头时间标签同步到当前子镜头时间轴。"""

    timeline = {shot.display_no: (int(shot.start_seconds), int(shot.end_seconds)) for shot in shots}

    def replace(match: re.Match[str]) -> str:
        display_no = int(match.group("display_no"))
        value = timeline.get(display_no)
        if value is None:
            return match.group(0)
        start, end = value
        return f"【镜头{display_no}:{start}s～{end}s】"

    return _PROMPT_TIMELINE_PATTERN.sub(replace, prompt)


def _retime_shots(shots: list[Shot], target_seconds: int) -> None:
    durations = _allocate_durations(shots, target_seconds)
    cursor = 0
    for shot, duration in zip(shots, durations, strict=True):
        start, end = cursor, cursor + duration
        cursor = end
        shot.start_seconds = start
        shot.end_seconds = end
        shot.shot_line = _rewrite_shot_line(shot, shot.display_no, start, end)


def _subject_matches_material(subject: Subject, category: str) -> bool:
    if category == "character":
        return subject.type == "character"
    if category == "background":
        return subject.type in {"scene", "background"}
    if category == "prop":
        return subject.type == "prop"
    return subject.type not in {"character", "scene", "background", "prop", "sound"}


def _resolve_subject(
    item: dict[str, Any],
    *,
    subjects_by_id: dict[str, Subject],
    subjects_by_name: dict[str, list[Subject]],
    approved_by_id: dict[str, AssetEntry],
    expected_kind: str,
    subject_matches: Any,
) -> tuple[Subject | None, AssetEntry | None]:
    subject: Subject | None = None
    explicit_subject_id = item.get("subject_id")
    explicit_asset_id = item.get("asset_entry_id")
    name = str(item.get("name") or "").strip()

    if explicit_subject_id:
        subject = subjects_by_id.get(str(explicit_subject_id))
        if subject is None:
            raise BadRequestError("素材主体不属于当前剧本")

    asset: AssetEntry | None = None
    if explicit_asset_id:
        asset = approved_by_id.get(str(explicit_asset_id))
        if asset is None or asset.kind != expected_kind:
            raise BadRequestError("素材资产不存在、未审核或类型不匹配")
        asset_subject = subjects_by_id.get(str(asset.subject_id))
        if asset_subject is None:
            raise BadRequestError("素材资产不属于当前剧本")
        if subject is not None and str(subject.id) != str(asset.subject_id):
            raise BadRequestError("素材主体与资产不匹配")
        subject = asset_subject

    if subject is None and name:
        candidates = [candidate for candidate in subjects_by_name.get(name, []) if subject_matches(candidate)]
        if candidates:
            subject = candidates[0]
        elif subjects_by_name.get(name):
            raise BadRequestError("素材主体类型与分组不匹配")

    if subject is not None and not subject_matches(subject):
        raise BadRequestError("素材主体类型与分组不匹配")
    return subject, asset


def _normalise_materials(
    raw: dict[str, list[dict[str, Any]]],
    *,
    subjects: list[Subject],
    approved_assets: list[AssetEntry],
) -> dict[str, list[dict[str, str]]]:
    subjects_by_id = {str(subject_model.id): subject_model for subject_model in subjects}
    subjects_by_name: dict[str, list[Subject]] = {}
    for subject_model in subjects:
        subjects_by_name.setdefault(subject_model.name, []).append(subject_model)
    approved_by_id = {str(asset_model.id): asset_model for asset_model in approved_assets}

    result: dict[str, list[dict[str, str]]] = {key: [] for key in _MATERIAL_KEYS}
    for category in _MATERIAL_KEYS:
        seen: set[tuple[str, str]] = set()
        for item in raw.get(category, []):
            resolved_subject, resolved_asset = _resolve_subject(
                item,
                subjects_by_id=subjects_by_id,
                subjects_by_name=subjects_by_name,
                approved_by_id=approved_by_id,
                expected_kind="image",
                subject_matches=lambda value, category=category: _subject_matches_material(value, category),
            )
            name = (
                resolved_subject.name if resolved_subject is not None else str(item.get("name") or "").strip()
            )
            if not name:
                raise BadRequestError("素材名称无法解析")
            normalised: dict[str, str] = {"name": name}
            if resolved_subject is not None:
                normalised["subject_id"] = str(resolved_subject.id)
                if resolved_asset is None and resolved_subject.image_entry_id:
                    candidate = approved_by_id.get(str(resolved_subject.image_entry_id))
                    if candidate is not None and candidate.kind == "image":
                        resolved_asset = candidate
            if resolved_asset is not None:
                normalised["asset_entry_id"] = str(resolved_asset.id)
            dedupe_key = (normalised.get("subject_id", ""), normalised["name"])
            if dedupe_key not in seen:
                seen.add(dedupe_key)
                result[category].append(normalised)
    return result


def _normalise_audio_elements(
    raw: list[dict[str, Any]],
    *,
    subjects: list[Subject],
    approved_assets: list[AssetEntry],
) -> dict[str, list[dict[str, str]]]:
    subjects_by_id = {str(subject_model.id): subject_model for subject_model in subjects}
    subjects_by_name: dict[str, list[Subject]] = {}
    for subject_model in subjects:
        subjects_by_name.setdefault(subject_model.name, []).append(subject_model)
    approved_by_id = {str(asset_model.id): asset_model for asset_model in approved_assets}
    approved_audio_by_subject: dict[str, AssetEntry] = {}
    for asset_model in approved_assets:
        if asset_model.kind == "audio":
            approved_audio_by_subject.setdefault(str(asset_model.subject_id), asset_model)

    result: dict[str, list[dict[str, str]]] = {"characters": [], "sounds": []}
    seen: set[tuple[str, str, str]] = set()
    for item in raw:
        item_type = str(item["type"])
        expected_subject_type = "character" if item_type == "character" else "sound"
        resolved_subject, resolved_asset = _resolve_subject(
            item,
            subjects_by_id=subjects_by_id,
            subjects_by_name=subjects_by_name,
            approved_by_id=approved_by_id,
            expected_kind="audio",
            subject_matches=lambda value, expected=expected_subject_type: value.type == expected,
        )
        name = resolved_subject.name if resolved_subject is not None else str(item.get("name") or "").strip()
        if not name:
            raise BadRequestError("音频元素名称无法解析")
        normalised: dict[str, str] = {"name": name, "type": item_type}
        if resolved_subject is not None:
            normalised["subject_id"] = str(resolved_subject.id)
            if resolved_asset is None:
                resolved_asset = approved_audio_by_subject.get(str(resolved_subject.id))
        if resolved_asset is not None:
            normalised["asset_entry_id"] = str(resolved_asset.id)
        dedupe_key = (item_type, normalised.get("subject_id", ""), name)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        target = "characters" if item_type == "character" else "sounds"
        result[target].append(normalised)
    return result


async def update_shot_group(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    shot_group_id: str,
    expected_episode_version: int,
    fields_set: set[str],
    duration_seconds: int | None = None,
    is_continuation: bool | None = None,
    prompt: str | None = None,
    materials: dict[str, list[dict[str, Any]]] | None = None,
    audio_elements: list[dict[str, Any]] | None = None,
) -> ShotGroupUpdateResult:
    """在父资源锁内编辑正式镜头组，并递增分集版本。"""

    locked_script_id = (
        await db.execute(
            select(Script.id).where(Script.id == script_id).with_for_update(read=True, key_share=True)
        )
    ).scalar_one_or_none()
    if locked_script_id is None:
        raise NotFoundError("剧本不存在")

    episode = (
        await db.execute(
            select(Episode)
            .where(
                Episode.episode_id == episode_id,
                Episode.script_id == script_id,
            )
            .options(noload(Episode.script), noload(Episode.scenes))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if episode is None:
        raise NotFoundError("分集不存在")
    if episode.episode_version != expected_episode_version:
        raise ConflictError("episode_version 冲突")

    group = (
        await db.execute(
            select(ShotGroup)
            .where(
                ShotGroup.shot_group_id == shot_group_id,
                ShotGroup.script_id == script_id,
                ShotGroup.episode_id == episode_id,
            )
            .options(noload(ShotGroup.shots))
            .with_for_update()
        )
    ).scalar_one_or_none()
    if group is None:
        existing_id = (
            await db.execute(
                select(ShotGroup.shot_group_id).where(ShotGroup.shot_group_id == shot_group_id).limit(1)
            )
        ).scalar_one_or_none()
        if existing_id is not None:
            raise ForbiddenError("镜头组不属于当前剧本/分集")
        raise NotFoundError("镜头组不存在")
    if group.unit_id != f"{FINAL_SHOT_GROUP_UNIT_PREFIX}{group.shot_group_id}":
        raise ConflictError("只有合并后的正式镜头组可以编辑")

    shots = list(
        (
            await db.execute(
                select(Shot)
                .where(Shot.shot_group_id == shot_group_id)
                .order_by(Shot.display_no, Shot.shot_id)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    if not shots:
        raise ConflictError("镜头组中没有可编辑的子镜头")

    duration_changed = False
    if "duration_seconds" in fields_set:
        assert duration_seconds is not None
        duration_changed = (
            duration_seconds != group.duration_seconds
            or duration_seconds != group.video_duration_seconds
        )

    continuation_changed = (
        "is_continuation" in fields_set
        and is_continuation is not None
        and bool(is_continuation) != bool(group.is_continuation)
    )

    candidate_prompt = group.prompt
    if "prompt" in fields_set:
        assert prompt is not None
        candidate_prompt = prompt
        # duration 改动后的时间标签会在真正重排后再次同步；此处先判断
        # 客户端提交的文本在当前时间轴下是否产生实际变化。
        candidate_prompt = _rewrite_prompt_timeline(candidate_prompt, shots)
    prompt_changed = candidate_prompt != group.prompt

    normalised_materials: dict[str, list[dict[str, str]]] | None = None
    normalised_audio: dict[str, list[dict[str, str]]] | None = None
    if "materials" in fields_set or "audio_elements" in fields_set:
        subjects = list(
            (await db.execute(select(Subject).where(Subject.script_id == script_id).order_by(Subject.id)))
            .scalars()
            .all()
        )
        subject_ids = [subject.id for subject in subjects]
        approved_assets: list[AssetEntry] = []
        if subject_ids:
            approved_assets = list(
                (
                    await db.execute(
                        select(AssetEntry).where(
                            AssetEntry.script_id == script_id,
                            AssetEntry.subject_id.in_(subject_ids),
                            AssetEntry.status == "approved",
                            AssetEntry.is_approved.is_(True),
                            AssetEntry.superseded.is_(False),
                        )
                    )
                )
                .scalars()
                .all()
            )
        if "materials" in fields_set:
            normalised_materials = _normalise_materials(
                materials or {},
                subjects=subjects,
                approved_assets=approved_assets,
            )
        if "audio_elements" in fields_set:
            normalised_audio = _normalise_audio_elements(
                audio_elements or [],
                subjects=subjects,
                approved_assets=approved_assets,
            )

    materials_changed = (
        normalised_materials is not None and normalised_materials != group.materials
    )
    audio_changed = (
        normalised_audio is not None and normalised_audio != group.audio_elements
    )
    if not (
        duration_changed
        or continuation_changed
        or prompt_changed
        or materials_changed
        or audio_changed
    ):
        return ShotGroupUpdateResult(
            episode_version=episode.episode_version,
            changed_fields=(),
        )

    active_job_id = (
        await db.execute(
            select(VideoGenerationJob.job_id)
            .where(
                VideoGenerationJob.shot_group_id == shot_group_id,
                or_(
                    VideoGenerationJob.status.in_(_ACTIVE_VIDEO_STATUSES),
                    (VideoGenerationJob.status == "failed")
                    & (
                        VideoGenerationJob.error_type
                        == VIDEO_SUBMISSION_MANUAL_REVIEW_ERROR_TYPE
                    ),
                ),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if active_job_id is not None:
        raise ConflictError("镜头组正在生成视频，暂不可编辑")

    changed_fields: list[str] = []
    if duration_changed:
        assert duration_seconds is not None
        if duration_seconds != group.duration_seconds:
            _retime_shots(shots, duration_seconds)
        group.duration_seconds = duration_seconds
        group.video_duration_seconds = duration_seconds
        changed_fields.append("duration_seconds")
    if continuation_changed:
        assert is_continuation is not None
        group.is_continuation = is_continuation
        changed_fields.append("is_continuation")

    final_prompt: str = (
        prompt
        if "prompt" in fields_set and prompt is not None
        else str(group.prompt)
    )
    if duration_changed or "prompt" in fields_set:
        final_prompt = _rewrite_prompt_timeline(final_prompt, shots)
    if final_prompt != group.prompt:
        group.prompt = final_prompt
    if prompt_changed:
        changed_fields.append("prompt")
    if materials_changed:
        assert normalised_materials is not None
        group.materials = normalised_materials
        changed_fields.append("materials")
    if audio_changed:
        assert normalised_audio is not None
        group.audio_elements = normalised_audio
        changed_fields.append("audio_elements")

    episode.episode_version += 1
    await db.flush()
    await db.refresh(group, attribute_names=["updated_at"])
    return ShotGroupUpdateResult(
        episode_version=episode.episode_version,
        changed_fields=tuple(changed_fields),
    )


__all__ = ["ShotGroupUpdateResult", "update_shot_group"]
