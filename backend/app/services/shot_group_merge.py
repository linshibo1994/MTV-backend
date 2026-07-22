"""镜头组合并服务。"""

from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.errors import BadRequestError, ConflictError, ForbiddenError, NotFoundError
from app.core.ids import new_id
from app.models.episode import Episode
from app.models.scene import Scene
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.storyboard_job import StoryboardJob
from app.models.subject import Subject
from app.models.video_generation_job import VideoGenerationJob
from app.services.storyboard import (
    _build_group_prompt,
    _classify_materials,
    _extract_audio_elements,
    _gen_shot_no,
)

_SHOT_LINE_PREFIX = re.compile(r"^【镜头\d+\s*:\s*[^】]+】")
_ACTIVE_STORYBOARD_STATUSES = ("pending", "running", "retry")
_ACTIVE_VIDEO_STATUSES = ("starting", "processing")


@dataclass(frozen=True)
class ShotGroupMergeResult:
    """合并后的关键事实，供路由审计使用。"""

    shot_group_id: str
    removed_shot_group_ids: tuple[str, ...]
    episode_version: int


def _duration(shot: Shot) -> int:
    value = int(shot.end_seconds) - int(shot.start_seconds)
    if value <= 0:
        raise BadRequestError("镜头时长必须大于 0 秒")
    return value


def _validate_group_shape(shots: list[Shot], *, label: str) -> int:
    if not 2 <= len(shots) <= 5:
        raise BadRequestError(f"{label}必须包含 2～5 个镜头")
    duration = sum(_duration(shot) for shot in shots)
    if not 3 <= duration <= 15:
        raise BadRequestError(f"{label}总时长必须为 3～15 秒")
    return duration


def _rewrite_shot_line(shot: Shot, display_no: int, start: int, end: int) -> str:
    prefix = f"【镜头{display_no}:{start}s～{end}s】"
    original = shot.shot_line or ""
    if _SHOT_LINE_PREFIX.match(original):
        return _SHOT_LINE_PREFIX.sub(prefix, original, count=1)
    return f"{prefix}{original or shot.content}"


def _ordered_unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _shot_payload(shot: Shot) -> dict:
    return {
        "dialogue_character": shot.dialogue_character,
        "sounds": list(shot.sounds or []),
    }


async def _refresh_group(
    db: AsyncSession,
    group: ShotGroup,
    shots: list[Shot],
    *,
    background: str,
    narrative_purpose: str,
    subjects: list[Subject],
    duration_by_shot: dict[str, int],
) -> None:
    """按当前镜头顺序重建一个发生结构变化的镜头组。"""

    cursor = 0
    referenced_names: list[str] = [background] if background else []
    for display_no, shot in enumerate(shots, start=1):
        duration = duration_by_shot[shot.shot_id]
        start, end = cursor, cursor + duration
        cursor = end
        shot.shot_group_id = group.shot_group_id
        shot.scene_no = group.scene_no
        shot.group_no = group.group_no
        shot.display_no = display_no
        shot.shot_no = _gen_shot_no(display_no)
        shot.start_seconds = start
        shot.end_seconds = end
        shot.shot_line = _rewrite_shot_line(shot, display_no, start, end)
        referenced_names.extend(str(name) for name in (shot.characters or []))
        referenced_names.extend(str(name) for name in (shot.props or []))
        referenced_names.extend(str(name) for name in (shot.sounds or []))

    referenced_names = _ordered_unique(referenced_names)
    audio_elements = await _extract_audio_elements(
        db,
        [_shot_payload(shot) for shot in shots],
        subjects,
    )
    group.prompt = _build_group_prompt(
        shot_lines=[shot.shot_line for shot in shots],
        referenced_names=referenced_names,
        narrative_purpose=narrative_purpose,
    )
    group.materials = _classify_materials(referenced_names, subjects)
    group.audio_elements = {
        "characters": [item for item in audio_elements if item.get("type") == "character"],
        "sounds": [item for item in audio_elements if item.get("type") == "sound"],
    }
    group.description_text = narrative_purpose
    group.duration_seconds = cursor
    group.video_duration_seconds = cursor
    group.reference_frames = {
        "first_frame": None,
        "last_frame": None,
        "reference_frame": None,
    }


async def _raise_for_missing_shots(
    db: AsyncSession,
    requested_ids: set[str],
    local_ids: set[str],
) -> None:
    missing_ids = requested_ids - local_ids
    if not missing_ids:
        return
    existing_result = await db.execute(
        select(Shot.shot_id).where(Shot.shot_id.in_(missing_ids)).limit(1)
    )
    if existing_result.scalar_one_or_none() is not None:
        raise ForbiddenError("镜头不属于当前剧本/分集")
    raise NotFoundError("镜头不存在")


async def merge_shots_into_group(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    shot_ids: list[str],
    expected_episode_version: int,
) -> ShotGroupMergeResult:
    """把同一场景中连续镜头合并为新的 ShotGroup。

    固定锁序为 Script -> Episode -> Scene -> ShotGroup -> Shot，与视频任务创建保持一致。
    服务仅 flush，事务提交由 API 路由负责。
    """

    locked_script_id = (
        await db.execute(
            select(Script.id)
            .where(Script.id == script_id)
            .with_for_update(read=True, key_share=True)
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
            .with_for_update()
        )
    ).scalar_one_or_none()
    if episode is None:
        raise NotFoundError("分集不存在")
    if episode.episode_version != expected_episode_version:
        raise ConflictError("episode_version 冲突")

    active_storyboard_id = (
        await db.execute(
            select(StoryboardJob.job_id)
            .where(
                StoryboardJob.episode_id == episode_id,
                StoryboardJob.status.in_(_ACTIVE_STORYBOARD_STATUSES),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if active_storyboard_id is not None:
        raise ConflictError("分镜生成任务运行中，暂不可合并镜头")

    scenes = list(
        (
            await db.execute(
                select(Scene)
                .where(Scene.episode_id == episode_id)
                .order_by(Scene.scene_no)
                .with_for_update()
            )
        ).scalars().all()
    )
    scene_by_no = {scene.scene_no: scene for scene in scenes}

    groups = list(
        (
            await db.execute(
                select(ShotGroup)
                .where(
                    ShotGroup.script_id == script_id,
                    ShotGroup.episode_id == episode_id,
                )
                .options(noload(ShotGroup.shots))
                .order_by(ShotGroup.group_index, ShotGroup.shot_group_id)
                .with_for_update()
            )
        ).scalars().all()
    )
    if not groups:
        raise ConflictError("当前分集尚无可合并的镜头组")

    all_shots = list(
        (
            await db.execute(
                select(Shot)
                .join(ShotGroup, ShotGroup.shot_group_id == Shot.shot_group_id)
                .where(
                    ShotGroup.script_id == script_id,
                    ShotGroup.episode_id == episode_id,
                )
                .order_by(
                    ShotGroup.group_index,
                    Shot.display_no,
                    Shot.shot_id,
                )
                .with_for_update(of=Shot)
            )
        ).scalars().all()
    )

    requested_ids = set(shot_ids)
    local_ids = {shot.shot_id for shot in all_shots}
    await _raise_for_missing_shots(db, requested_ids, local_ids)
    selected = [shot for shot in all_shots if shot.shot_id in requested_ids]
    scene_nos = {shot.scene_no for shot in selected}
    if len(scene_nos) != 1:
        raise BadRequestError("只能合并同一场景中的镜头")
    scene_no = next(iter(scene_nos))
    scene = scene_by_no.get(scene_no)
    if scene is None:
        raise ConflictError("镜头所属场景不存在")

    scene_shots = [shot for shot in all_shots if shot.scene_no == scene_no]
    selected_positions = [
        index for index, shot in enumerate(scene_shots) if shot.shot_id in requested_ids
    ]
    if selected_positions != list(
        range(selected_positions[0], selected_positions[0] + len(selected_positions))
    ):
        raise BadRequestError("只能合并叙事顺序连续的镜头")

    group_by_id = {group.shot_group_id: group for group in groups}
    source_group_ids = _ordered_unique([shot.shot_group_id for shot in selected])
    if len(source_group_ids) < 2:
        raise BadRequestError("所选镜头已属于同一镜头组，无需合并")
    source_groups = [group_by_id[group_id] for group_id in source_group_ids]

    target_duration = _validate_group_shape(selected, label="合并后的镜头组")
    shots_by_group: dict[str, list[Shot]] = {group.shot_group_id: [] for group in groups}
    for shot in all_shots:
        shots_by_group[shot.shot_group_id].append(shot)

    first_group = source_groups[0]
    last_group = source_groups[-1]
    first_shots = shots_by_group[first_group.shot_group_id]
    last_shots = shots_by_group[last_group.shot_group_id]
    first_selected_index = next(
        index for index, shot in enumerate(first_shots) if shot.shot_id in requested_ids
    )
    last_selected_index = max(
        index for index, shot in enumerate(last_shots) if shot.shot_id in requested_ids
    )
    prefix_shots = first_shots[:first_selected_index]
    suffix_shots = last_shots[last_selected_index + 1 :]
    if prefix_shots:
        _validate_group_shape(prefix_shots, label="合并后保留的前置镜头组")
    if suffix_shots:
        _validate_group_shape(suffix_shots, label="合并后保留的后置镜头组")

    source_video_job_id = (
        await db.execute(
            select(VideoGenerationJob.job_id)
            .where(VideoGenerationJob.shot_group_id.in_(source_group_ids))
            .limit(1)
        )
    ).scalar_one_or_none()
    if source_video_job_id is not None:
        raise ConflictError("所选镜头组已有视频任务，不能修改其分镜结构")
    active_video_job_id = (
        await db.execute(
            select(VideoGenerationJob.job_id)
            .where(
                VideoGenerationJob.episode_id == episode_id,
                VideoGenerationJob.status.in_(_ACTIVE_VIDEO_STATUSES),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if active_video_job_id is not None:
        raise ConflictError("当前分集有视频生成任务运行中，暂不可合并镜头")

    duration_by_shot = {shot.shot_id: _duration(shot) for shot in all_shots}
    subjects = list(
        (
            await db.execute(
                select(Subject).where(Subject.script_id == script_id).order_by(Subject.id)
            )
        ).scalars().all()
    )

    # 先释放源组内 display_no/shot_no 唯一值，避免搬迁和重排时瞬时冲突。
    source_shots = [shot for group_id in source_group_ids for shot in shots_by_group[group_id]]
    for index, shot in enumerate(source_shots, start=1):
        shot.display_no = 100_000 + index
        shot.shot_no = f"tmp-{new_id()}"
    await db.flush()

    target_group_id = new_id()
    target_group = ShotGroup(
        shot_group_id=target_group_id,
        script_id=script_id,
        episode_id=episode_id,
        scene_no=scene_no,
        group_index=max(group.group_index for group in groups) + 10_000,
        group_no=f"TMP{new_id()[:20]}",
        duration_seconds=target_duration,
        video_duration_seconds=target_duration,
    )
    db.add(target_group)

    narrative_parts = _ordered_unique(
        [group.description_text for group in source_groups if group.description_text]
    )
    target_narrative = "；".join(narrative_parts)
    await _refresh_group(
        db,
        target_group,
        selected,
        background=scene.background,
        narrative_purpose=target_narrative,
        subjects=subjects,
        duration_by_shot=duration_by_shot,
    )

    kept_source_ids: set[str] = set()
    if prefix_shots:
        kept_source_ids.add(first_group.shot_group_id)
        await _refresh_group(
            db,
            first_group,
            prefix_shots,
            background=scene.background,
            narrative_purpose=first_group.description_text,
            subjects=subjects,
            duration_by_shot=duration_by_shot,
        )
    if suffix_shots:
        kept_source_ids.add(last_group.shot_group_id)
        await _refresh_group(
            db,
            last_group,
            suffix_shots,
            background=scene.background,
            narrative_purpose=last_group.description_text,
            subjects=subjects,
            duration_by_shot=duration_by_shot,
        )

    removed_ids = tuple(
        group_id for group_id in source_group_ids if group_id not in kept_source_ids
    )
    await db.flush()
    if removed_ids:
        await db.execute(
            delete(ShotGroup)
            .where(ShotGroup.shot_group_id.in_(removed_ids))
            .execution_options(synchronize_session="fetch")
        )
        await db.flush()

    shots_by_group[target_group_id] = selected
    if prefix_shots:
        shots_by_group[first_group.shot_group_id] = prefix_shots
    if suffix_shots:
        shots_by_group[last_group.shot_group_id] = suffix_shots
    for removed_id in removed_ids:
        shots_by_group.pop(removed_id, None)

    source_id_set = set(source_group_ids)
    final_groups: list[ShotGroup] = []
    for group in groups:
        if group.shot_group_id == first_group.shot_group_id:
            if prefix_shots:
                final_groups.append(first_group)
            final_groups.append(target_group)
            continue
        if group.shot_group_id in source_id_set:
            if group.shot_group_id == last_group.shot_group_id and suffix_shots:
                final_groups.append(last_group)
            continue
        final_groups.append(group)

    # 两阶段重排组索引/组号，规避非延迟唯一约束的瞬时碰撞。
    temporary_base = len(groups) + 100_000
    for index, group in enumerate(final_groups, start=1):
        group.group_index = temporary_base + index
        group.group_no = f"TMP{index}-{new_id()[:16]}"
        for shot in shots_by_group[group.shot_group_id]:
            shot.group_no = group.group_no
    await db.flush()

    next_group_no_by_scene: dict[int, int] = {}
    for group_index, group in enumerate(final_groups, start=1):
        group.group_index = group_index
        next_no = next_group_no_by_scene.get(group.scene_no, 0) + 1
        next_group_no_by_scene[group.scene_no] = next_no
        group.group_no = f"G{next_no}"
        for shot in shots_by_group[group.shot_group_id]:
            shot.group_no = group.group_no

    groups_by_scene: dict[int, list[ShotGroup]] = {}
    for group in final_groups:
        groups_by_scene.setdefault(group.scene_no, []).append(group)
    succeeded_unit_ids = set(
        (
            await db.execute(
                select(VideoGenerationJob.unit_id).where(
                    VideoGenerationJob.episode_id == episode_id,
                    VideoGenerationJob.status == "succeeded",
                )
            )
        ).scalars().all()
    )
    for current_scene in scenes:
        scene_groups = groups_by_scene.get(current_scene.scene_no, [])
        current_scene.shot_group_count = len(scene_groups)
        current_scene.pending_shot_count = sum(
            len(shots_by_group[group.shot_group_id])
            for group in scene_groups
            if group.unit_id not in succeeded_unit_ids
        )

    episode.episode_version += 1
    await db.flush()
    return ShotGroupMergeResult(
        shot_group_id=target_group_id,
        removed_shot_group_ids=removed_ids,
        episode_version=episode.episode_version,
    )


__all__ = ["ShotGroupMergeResult", "merge_shots_into_group"]
