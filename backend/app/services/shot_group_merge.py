"""将一集全部小分镜自动组合为正式视频镜头组。"""

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
from app.models.shot_group import (
    DRAFT_SHOT_GROUP_UNIT_PREFIX,
    FINAL_SHOT_GROUP_UNIT_PREFIX,
    ShotGroup,
)
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
_MIN_GROUP_SECONDS = 3
_MAX_GROUP_SECONDS = 15
_PREFERRED_GROUP_SECONDS = 6


@dataclass(frozen=True)
class ShotGroupMergeResult:
    """整集合并后的关键事实，供路由审计使用。"""

    shot_group_ids: tuple[str, ...]
    removed_shot_group_ids: tuple[str, ...]
    episode_version: int
    already_grouped: bool = False


def _duration(shot: Shot) -> int:
    value = int(shot.end_seconds) - int(shot.start_seconds)
    if value <= 0:
        raise BadRequestError("镜头时长必须大于 0 秒")
    if value > _MAX_GROUP_SECONDS:
        raise BadRequestError("单个镜头时长不能超过 15 秒")
    return value


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


def _is_final_group(group: ShotGroup) -> bool:
    return group.unit_id == f"{FINAL_SHOT_GROUP_UNIT_PREFIX}{group.shot_group_id}"


def _is_draft_group(group: ShotGroup) -> bool:
    return group.unit_id == f"{DRAFT_SHOT_GROUP_UNIT_PREFIX}{group.shot_group_id}"


def _partition_scene_shots(
    shots: list[Shot],
    *,
    duration_by_shot: dict[str, int],
    preferred_boundaries: set[int],
) -> list[list[Shot]]:
    """把同一场景的连续镜头切成 3～15 秒的组。

    Provider 在分镜生成阶段给出的 draft 边界代表 LLM 对叙事连续性的建议；
    动态规划优先保留这些边界，再选择更接近 6 秒的合法分区。请求数组
    顺序不参与分区，避免客户端重排叙事。
    """

    count = len(shots)
    # dp[i] = (代价, 从 i 开始的分组下标区间)。代价按叙事边界破坏数、
    # 时长偏差、组数、切点顺序依次比较，确保结果稳定且尾组始终合法。
    dp: dict[int, tuple[tuple[int, int, int, tuple[int, ...]], list[tuple[int, int]]]] = {
        count: ((0, 0, 0, ()), [])
    }
    for start in range(count - 1, -1, -1):
        total = 0
        best: tuple[tuple[int, int, int, tuple[int, ...]], list[tuple[int, int]]] | None = None
        for end in range(start + 1, count + 1):
            total += duration_by_shot[shots[end - 1].shot_id]
            if total > _MAX_GROUP_SECONDS:
                break
            if total < _MIN_GROUP_SECONDS or end not in dp:
                continue

            skipped_boundaries = sum(1 for boundary in preferred_boundaries if start < boundary < end)
            added_boundary = int(end < count and end not in preferred_boundaries)
            tail_cost, tail_groups = dp[end]
            cut_points = (end,) + tail_cost[3]
            cost = (
                skipped_boundaries + added_boundary + tail_cost[0],
                abs(total - _PREFERRED_GROUP_SECONDS) + tail_cost[1],
                1 + tail_cost[2],
                cut_points,
            )
            candidate = (cost, [(start, end), *tail_groups])
            if best is None or candidate[0] < best[0]:
                best = candidate
        if best is not None:
            dp[start] = best

    if 0 not in dp:
        raise BadRequestError("当前场景的分镜时长无法自动组合为 3～15 秒镜头组")
    return [shots[start:end] for start, end in dp[0][1]]


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
    """按组内叙事顺序重建时间轴、Prompt、素材与音频。"""

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


async def _raise_for_unknown_shots(
    db: AsyncSession,
    requested_ids: set[str],
    local_ids: set[str],
) -> None:
    unknown_ids = requested_ids - local_ids
    if not unknown_ids:
        return
    existing_result = await db.execute(
        select(Shot.shot_id).where(Shot.shot_id.in_(unknown_ids)).limit(1)
    )
    if existing_result.scalar_one_or_none() is not None:
        raise ForbiddenError("镜头不属于当前剧本/分集")
    raise NotFoundError("镜头不存在")


async def merge_shots_into_groups(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    shot_ids: list[str],
    expected_episode_version: int,
) -> ShotGroupMergeResult:
    """接收当前分集全部小分镜，自动生成多个正式视频镜头组。"""

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
        raise ConflictError("分镜生成任务运行中，暂不可合并镜头组")

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

    source_groups = list(
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
    if not source_groups:
        raise ConflictError("当前分集尚未生成分镜")

    all_shots = list(
        (
            await db.execute(
                select(Shot)
                .join(ShotGroup, ShotGroup.shot_group_id == Shot.shot_group_id)
                .where(
                    ShotGroup.script_id == script_id,
                    ShotGroup.episode_id == episode_id,
                )
                .order_by(ShotGroup.group_index, Shot.display_no, Shot.shot_id)
                .with_for_update(of=Shot)
            )
        ).scalars().all()
    )
    if not all_shots:
        raise ConflictError("当前分集没有可合并的小分镜")

    if len(shot_ids) != len(set(shot_ids)):
        raise BadRequestError("shot_ids 不得重复")
    requested_ids = set(shot_ids)
    local_ids = {shot.shot_id for shot in all_shots}
    await _raise_for_unknown_shots(db, requested_ids, local_ids)
    omitted_count = len(local_ids - requested_ids)
    if omitted_count:
        raise BadRequestError(f"必须提交当前分集全部分镜 ID，缺少 {omitted_count} 个")

    final_groups = [group for group in source_groups if _is_final_group(group)]
    draft_groups = [group for group in source_groups if _is_draft_group(group)]
    if len(final_groups) == len(source_groups):
        # 网络重试或重复点击：全量 ID 一致时直接返回当前结果，不重复改组和涨版本。
        return ShotGroupMergeResult(
            shot_group_ids=tuple(group.shot_group_id for group in final_groups),
            removed_shot_group_ids=(),
            episode_version=episode.episode_version,
            already_grouped=True,
        )
    if len(draft_groups) != len(source_groups):
        raise ConflictError("当前分集镜头组状态不一致，请重新生成分镜")

    source_group_ids = [group.shot_group_id for group in source_groups]
    source_video_job_id = (
        await db.execute(
            select(VideoGenerationJob.job_id)
            .where(VideoGenerationJob.shot_group_id.in_(source_group_ids))
            .limit(1)
        )
    ).scalar_one_or_none()
    if source_video_job_id is not None:
        raise ConflictError("分镜已关联视频生成任务，不能重新组合")

    duration_by_shot = {shot.shot_id: _duration(shot) for shot in all_shots}
    shots_by_source_group: dict[str, list[Shot]] = {
        group.shot_group_id: [] for group in source_groups
    }
    shots_by_scene: dict[int, list[Shot]] = {}
    for shot in all_shots:
        if shot.scene_no not in scene_by_no:
            raise ConflictError("镜头所属场景不存在")
        shots_by_source_group[shot.shot_group_id].append(shot)
        shots_by_scene.setdefault(shot.scene_no, []).append(shot)

    final_shot_buckets: list[tuple[Scene, list[Shot]]] = []
    for scene in scenes:
        scene_shots = shots_by_scene.get(scene.scene_no, [])
        if not scene_shots:
            continue
        preferred_boundaries: set[int] = set()
        cursor = 0
        for source_group in source_groups:
            if source_group.scene_no != scene.scene_no:
                continue
            cursor += len(shots_by_source_group[source_group.shot_group_id])
            preferred_boundaries.add(cursor)
        for bucket in _partition_scene_shots(
            scene_shots,
            duration_by_shot=duration_by_shot,
            preferred_boundaries=preferred_boundaries,
        ):
            final_shot_buckets.append((scene, bucket))

    if sum(len(bucket) for _, bucket in final_shot_buckets) != len(all_shots):
        raise ConflictError("自动组合未覆盖全部分镜")

    subjects = list(
        (
            await db.execute(
                select(Subject).where(Subject.script_id == script_id).order_by(Subject.id)
            )
        ).scalars().all()
    )
    source_group_by_id = {group.shot_group_id: group for group in source_groups}

    # 先释放源组内唯一编号，避免批量搬迁时命中非延迟唯一约束。
    for index, shot in enumerate(all_shots, start=1):
        shot.display_no = 100_000 + index
        shot.shot_no = f"tmp-{new_id()}"
    await db.flush()

    created_groups: list[ShotGroup] = []
    temporary_base = max(group.group_index for group in source_groups) + 10_000
    for index, (scene, bucket) in enumerate(final_shot_buckets, start=1):
        shot_group_id = new_id()
        group = ShotGroup(
            shot_group_id=shot_group_id,
            script_id=script_id,
            episode_id=episode_id,
            scene_no=scene.scene_no,
            group_index=temporary_base + index,
            group_no=f"TMP{index}-{new_id()[:12]}",
            unit_id=f"{FINAL_SHOT_GROUP_UNIT_PREFIX}{shot_group_id}",
            unit_runtime_key=f"{episode_id}:{scene.scene_no}:group:{shot_group_id}",
        )
        db.add(group)
        narrative_parts = _ordered_unique(
            [
                source_group_by_id[shot.shot_group_id].description_text
                for shot in bucket
                if source_group_by_id[shot.shot_group_id].description_text
            ]
        )
        await _refresh_group(
            db,
            group,
            bucket,
            background=scene.background,
            narrative_purpose="；".join(narrative_parts),
            subjects=subjects,
            duration_by_shot=duration_by_shot,
        )
        created_groups.append(group)

    await db.flush()
    await db.execute(
        delete(ShotGroup)
        .where(ShotGroup.shot_group_id.in_(source_group_ids))
        .execution_options(synchronize_session="fetch")
    )
    await db.flush()

    next_group_no_by_scene: dict[int, int] = {}
    shots_by_final_group = {
        group.shot_group_id: bucket
        for group, (_, bucket) in zip(created_groups, final_shot_buckets, strict=True)
    }
    for group_index, group in enumerate(created_groups, start=1):
        group.group_index = group_index
        next_no = next_group_no_by_scene.get(group.scene_no, 0) + 1
        next_group_no_by_scene[group.scene_no] = next_no
        group.group_no = f"G{next_no}"
        for shot in shots_by_final_group[group.shot_group_id]:
            shot.group_no = group.group_no

    groups_by_scene: dict[int, list[ShotGroup]] = {}
    for group in created_groups:
        groups_by_scene.setdefault(group.scene_no, []).append(group)
    for scene in scenes:
        scene_groups = groups_by_scene.get(scene.scene_no, [])
        scene.shot_group_count = len(scene_groups)
        scene.pending_shot_count = sum(
            len(shots_by_final_group[group.shot_group_id]) for group in scene_groups
        )

    episode.episode_version += 1
    await db.flush()
    return ShotGroupMergeResult(
        shot_group_ids=tuple(group.shot_group_id for group in created_groups),
        removed_shot_group_ids=tuple(source_group_ids),
        episode_version=episode.episode_version,
    )


# 兼容内部旧导入名；语义已经变为整集合并并产生多个组。
merge_shots_into_group = merge_shots_into_groups

__all__ = [
    "ShotGroupMergeResult",
    "merge_shots_into_group",
    "merge_shots_into_groups",
]
