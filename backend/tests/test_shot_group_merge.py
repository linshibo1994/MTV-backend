"""整集小分镜自动组合的最小验收。"""

from __future__ import annotations

import os
import secrets
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


def _structured_shot(
    display_no: int,
    start: int,
    end: int,
    *,
    start_boundary: dict | None = None,
    end_boundary: dict | None = None,
    source_block_ref: str | None = None,
) -> dict:
    return {
        "schema_version": "storyboard.shot-structure.v1",
        "display_no": display_no,
        "start_seconds": start,
        "end_seconds": end,
        "visual": {
            "shot_size": "中景",
            "composition": ["三分法"],
            "camera_angle": "平视",
            "camera_movement": "固定",
            "action": "角色停下并回望",
            "frame_description": "角色位于画面左侧并回望",
            "lighting": "冷色侧光",
            "emotion": "迟疑",
        },
        "narrative_purpose": "保持动作连续",
        "character_refs": ["C01"],
        "prop_refs": [],
        "sound_refs": [],
        "sound_design": "无",
        "dialogue": None,
        "source_block_refs": [source_block_ref or f"B{display_no:02d}"],
        "character_positions": [
            {"ref": "C01", "position": "左中", "facing": "面朝右"}
        ],
        "subject_ids_by_ref": {"C01": "subject-c1"},
        "background_ref": "BG01",
        **({"start_boundary": start_boundary} if start_boundary else {}),
        **({"end_boundary": end_boundary} if end_boundary else {}),
    }


def test_partition_preserves_llm_narrative_boundaries() -> None:
    from app.services.shot_group_merge import _partition_scene_shots

    shots = [SimpleNamespace(shot_id=f"shot-{index}") for index in range(1, 5)]
    durations = {"shot-1": 3, "shot-2": 2, "shot-3": 2, "shot-4": 2}

    groups = _partition_scene_shots(
        shots,
        duration_by_shot=durations,
        preferred_boundaries={2, 4},
    )

    assert [[shot.shot_id for shot in group] for group in groups] == [
        ["shot-1", "shot-2"],
        ["shot-3", "shot-4"],
    ]
    assert [sum(durations[shot.shot_id] for shot in group) for group in groups] == [5, 4]


@pytest.mark.parametrize(
    ("legacy_line", "expected"),
    [
        (
            "【镜头9:18s～21s】场景是开放办公区。，冷白屏光，老姜抬手揉按太阳穴。，"
            "疲惫，老姜:，构图是【中央构图】，声音设计是【无】，台词类型是【none】",
            "【镜头2:3s～6s】场景是开放办公区，冷白屏光，老姜抬手揉按太阳穴，"
            "疲惫，构图是【中央构图】",
        ),
        (
            "【镜头9:18s～21s】场景是开放办公区，突发不安，环境与场景:，，"
            "楼内空调低鸣。】，开放办公区灯光持续亮着。，构图是【三分法】，"
            "声音设计是【none】，台词类型是【】",
            "【镜头2:3s～6s】场景是开放办公区，突发不安，楼内空调低鸣，"
            "开放办公区灯光持续亮着，构图是【三分法】",
        ),
        (
            "【镜头9:18s～21s】场景是开放办公区，老姜:，构图是【三分法】，"
            "声音设计是【】，台词类型是【 】",
            "【镜头2:3s～6s】场景是开放办公区，构图是【三分法】",
        ),
    ],
)
def test_rewrite_shot_line_removes_absent_fields_and_normalizes_punctuation(
    legacy_line: str,
    expected: str,
) -> None:
    from app.services.shot_group_merge import _rewrite_shot_line

    shot = SimpleNamespace(shot_line=legacy_line, content="兜底内容")

    assert _rewrite_shot_line(shot, display_no=2, start=3, end=6) == expected


@pytest.mark.parametrize("dialogue_kind", ["dialogue", "voice_over", "inner_monologue"])
def test_rewrite_shot_line_keeps_actual_sound_and_dialogue_but_removes_legacy_dialogue_kind(
    dialogue_kind: str,
) -> None:
    from app.services.shot_group_merge import _rewrite_shot_line

    shot = SimpleNamespace(
        shot_line=(
            "【镜头9:18s～21s】老姜端起茶杯，老姜:“系统性变革到了深水区。”，"
            "构图是【留白】，声音设计是【茶杯轻碰桌面的瓷声，办公室底噪持续】，"
            f"台词类型是【{dialogue_kind}】"
        ),
        content="兜底内容",
    )

    rewritten = _rewrite_shot_line(shot, display_no=2, start=3, end=6)

    assert rewritten == (
        "【镜头2:3s～6s】老姜端起茶杯，老姜:“系统性变革到了深水区。”，"
        "构图是【留白】，声音设计是【茶杯轻碰桌面的瓷声，办公室底噪持续】"
    )
    assert "台词类型是" not in rewritten
    assert dialogue_kind not in rewritten


@pytest.mark.parametrize(
    "dialogue",
    [
        "先听，声音设计是【无】不是这次的问题。",
        "我说，台词类型是【voice_over】不能删。",
    ],
)
def test_rewrite_shot_line_does_not_treat_dialogue_text_as_optional_metadata(
    dialogue: str,
) -> None:
    """历史清理只能处理镜头尾部字段，不能改写角色实际说出的内容。"""
    from app.services.shot_group_merge import _rewrite_shot_line

    shot = SimpleNamespace(
        shot_line=(
            f"【镜头9:18s～21s】老姜端起茶杯，老姜：{dialogue}，"
            "构图是【留白】，声音设计是【无】，台词类型是【voice_over】"
        ),
        content="兜底内容",
    )

    rewritten = _rewrite_shot_line(shot, display_no=2, start=3, end=6)

    assert rewritten == (
        f"【镜头2:3s～6s】老姜端起茶杯，老姜：{dialogue}，构图是【留白】"
    )


def test_rewrite_shot_line_normalizes_absent_dialogue_without_role_segment() -> None:
    """无“角色：”段的旧模板行也要清除字段边界的异常标点。"""
    from app.services.shot_group_merge import _rewrite_shot_line

    shot = SimpleNamespace(
        shot_line=(
            "【镜头9:18s～21s】场景是开放办公区。，冷白屏光，老姜抬手揉按太阳穴。，"
            "疲惫，构图是【中央构图】，声音设计是【无】，台词类型是【none】"
        ),
        content="兜底内容",
    )

    assert _rewrite_shot_line(shot, display_no=2, start=3, end=6) == (
        "【镜头2:3s～6s】场景是开放办公区，冷白屏光，老姜抬手揉按太阳穴，"
        "疲惫，构图是【中央构图】"
    )


def test_rewrite_shot_line_removes_no_composition_metadata_after_real_dialogue() -> None:
    """历史行没有构图字段时，也要仅清理真实台词后的连续元数据尾段。"""
    from app.services.shot_group_merge import _rewrite_shot_line

    shot = SimpleNamespace(
        shot_line=(
            "【镜头9:18s～21s】场景是开放办公区，近景·平视，固定，冷白屏光，"
            "老姜端起茶杯，疲惫，老姜：别走\n"
            "声音设计是【无】\n"
            "台词类型是【voice_over】"
        ),
        content="兜底内容",
        structured_data={
            "schema_version": "storyboard.shot-structure.v1",
            "dialogue": {"speaker_ref": "C01", "text": "别走", "kind": "voice_over"},
        },
    )

    assert _rewrite_shot_line(shot, display_no=2, start=3, end=6) == (
        "【镜头2:3s～6s】场景是开放办公区，近景·平视，固定，冷白屏光，"
        "老姜端起茶杯，疲惫，老姜：别走"
    )


def test_rewrite_shot_line_preserves_structured_multiline_dialogue_before_metadata_tail() -> None:
    """结构快照中的多行真实台词必须完整保留，哪怕末行与旧字段同形。"""
    from app.services.shot_group_merge import _rewrite_shot_line

    dialogue = "请照读以下两行\n声音设计是【无】\n台词类型是【voice_over】"
    shot = SimpleNamespace(
        shot_line=(
            "【镜头9:18s～21s】场景是开放办公区，近景·平视，固定，冷白屏光，"
            f"老姜端起茶杯，疲惫，老姜：{dialogue}\n"
            "声音设计是【无】\n"
            "台词类型是【voice_over】"
        ),
        content="兜底内容",
        structured_data={
            "schema_version": "storyboard.shot-structure.v1",
            "dialogue": {"speaker_ref": "C01", "text": dialogue, "kind": "voice_over"},
        },
    )

    assert _rewrite_shot_line(shot, display_no=2, start=3, end=6) == (
        "【镜头2:3s～6s】场景是开放办公区，近景·平视，固定，冷白屏光，"
        f"老姜端起茶杯，疲惫，老姜：{dialogue}"
    )


@pytest.mark.parametrize("punctuation", ["。", "！", "？"])
def test_rewrite_shot_line_removes_metadata_tail_with_trailing_sentence_punctuation(
    punctuation: str,
) -> None:
    """旧字段后的常见句末标点不能阻止内部字段被移除。"""
    from app.services.shot_group_merge import _rewrite_shot_line

    shot = SimpleNamespace(
        shot_line=(
            "【镜头9:18s～21s】场景是开放办公区，近景·平视，固定，冷白屏光，"
            "老姜端起茶杯，疲惫，老姜：别走\n"
            "声音设计是【无】\n"
            f"台词类型是【voice_over】{punctuation}"
        ),
        content="兜底内容",
        structured_data={
            "schema_version": "storyboard.shot-structure.v1",
            "dialogue": {"speaker_ref": "C01", "text": "别走", "kind": "voice_over"},
        },
    )

    assert _rewrite_shot_line(shot, display_no=2, start=3, end=6) == (
        "【镜头2:3s～6s】场景是开放办公区，近景·平视，固定，冷白屏光，"
        "老姜端起茶杯，疲惫，老姜：别走"
    )


def test_rewrite_shot_line_cleans_unstructured_legacy_metadata_tail() -> None:
    """无结构快照且明确没有台词时，旧字段不能继续进入组合 Prompt。"""
    from app.services.shot_group_merge import _rewrite_shot_line

    shot = SimpleNamespace(
        shot_line=(
            "【镜头9:18s～21s】场景是开放办公区，近景·平视，固定，冷白屏光，"
            "老姜端起茶杯，疲惫\n"
            "声音设计是【无】\n"
            "台词类型是【voice_over】"
        ),
        content="兜底内容",
    )

    assert _rewrite_shot_line(shot, display_no=2, start=3, end=6) == (
        "【镜头2:3s～6s】场景是开放办公区，近景·平视，固定，冷白屏光，"
        "老姜端起茶杯，疲惫"
    )


def test_rewrite_shot_line_preserves_unstructured_multiline_dialogue_that_looks_like_metadata() -> None:
    """没有结构快照时，不能推断并删改与字段同形的真实多行台词。"""
    from app.services.shot_group_merge import _rewrite_shot_line

    dialogue = "请原样读\n声音设计是【无】\n台词类型是【voice_over】"
    shot = SimpleNamespace(
        shot_line=(
            "【镜头9:18s～21s】场景是开放办公区，近景·平视，固定，冷白屏光，"
            f"老姜端起茶杯，疲惫，老姜：{dialogue}"
        ),
        content="兜底内容",
    )

    assert _rewrite_shot_line(shot, display_no=2, start=3, end=6) == (
        "【镜头2:3s～6s】场景是开放办公区，近景·平视，固定，冷白屏光，"
        f"老姜端起茶杯，疲惫，老姜：{dialogue}"
    )


def test_rewrite_shot_line_locates_short_structured_dialogue_before_metadata_tail() -> None:
    """短台词不能误匹配到后续声音字段中相同的文本。"""
    from app.services.shot_group_merge import _rewrite_shot_line

    shot = SimpleNamespace(
        shot_line=(
            "【镜头9:18s～21s】场景是开放办公区，近景·平视，固定，冷白屏光，"
            "老姜端起茶杯，疲惫，老姜：无\n"
            "声音设计是【无】\n"
            "台词类型是【voice_over】"
        ),
        content="兜底内容",
        structured_data={
            "schema_version": "storyboard.shot-structure.v1",
            "dialogue": {"speaker_ref": "C01", "text": "无", "kind": "voice_over"},
        },
    )

    assert _rewrite_shot_line(shot, display_no=2, start=3, end=6) == (
        "【镜头2:3s～6s】场景是开放办公区，近景·平视，固定，冷白屏光，"
        "老姜端起茶杯，疲惫，老姜：无"
    )


async def test_refresh_group_retimes_structure_and_rebuilds_quality() -> None:
    from app.services.shot_group_merge import _refresh_group

    start_boundary = {
        "positions": [{"ref": "C01", "position": "左中", "facing": "面朝右"}],
        "visible_state": ["角色刚停下"],
    }
    end_boundary = {
        "positions": [{"ref": "C01", "position": "正中", "facing": "面朝右"}],
        "visible_state": ["角色完成回望"],
    }
    shots = [
        SimpleNamespace(
            shot_id="shot-1",
            shot_group_id="draft-1",
            scene_no=1,
            group_no="G1",
            display_no=1,
            shot_no="old-1",
            start_seconds=0,
            end_seconds=2,
            shot_line="【镜头1:0s～2s】镜头一",
            content="镜头一",
            characters=["阿明"],
            props=[],
            sounds=[],
            dialogue_character="",
            structured_data=_structured_shot(
                1,
                0,
                2,
                start_boundary=start_boundary,
            ),
        ),
        SimpleNamespace(
            shot_id="shot-2",
            shot_group_id="draft-1",
            scene_no=1,
            group_no="G1",
            display_no=2,
            shot_no="old-2",
            start_seconds=2,
            end_seconds=5,
            shot_line="【镜头2:2s～5s】镜头二",
            content="镜头二",
            characters=["阿明"],
            props=[],
            sounds=[],
            dialogue_character="",
            structured_data=_structured_shot(
                2,
                2,
                5,
                end_boundary=end_boundary,
            ),
        ),
    ]
    group = SimpleNamespace(
        shot_group_id="final-1",
        scene_no=1,
        group_no="G1",
    )
    subjects = [
        SimpleNamespace(
            id="subject-c1",
            name="阿明",
            type="character",
            image_entry_id=None,
        )
    ]
    empty_result = SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: []),
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=empty_result))

    await _refresh_group(
        db,
        group,
        shots,
        background="办公室",
        narrative_purpose="保持动作连续",
        subjects=subjects,
        duration_by_shot={"shot-1": 2, "shot-2": 3},
    )

    assert shots[0].structured_data["start_seconds"] == 0
    assert shots[1].structured_data["start_seconds"] == 2
    assert group.quality_summary["director_commands"][0]["frame_role"] == "first"
    assert group.quality_summary["director_commands"][1]["frame_role"] == "last"
    assert group.reference_frames["boundary_projections"]["start"][
        "prompt_fragment"
    ].startswith("【入画状态】")
    assert "【镜头1:0s～2s】中景·平视，镜头保持固定。" in group.prompt
    assert "构图是【" not in group.prompt
    assert "【导演指令】" not in group.prompt
    assert group.materials["character"] == [
        {"ref": "C01", "subject_id": "subject-c1", "name": "阿明"}
    ]


async def test_refresh_group_preserves_storyboard_provenance_anchor() -> None:
    from app.services.shot_group_merge import _refresh_group

    shot = SimpleNamespace(
        shot_id="shot-1",
        shot_group_id="draft-1",
        scene_no=1,
        group_no="G1",
        display_no=1,
        shot_no="old-1",
        start_seconds=0,
        end_seconds=3,
        shot_line="【镜头1:0s～3s】镜头一",
        content="镜头一",
        characters=["阿明"],
        props=[],
        sounds=[],
        dialogue_character="",
        structured_data=_structured_shot(1, 0, 3),
    )
    group = SimpleNamespace(shot_group_id="final-1", scene_no=1, group_no="G1")
    subject = SimpleNamespace(
        id="subject-c1",
        name="阿明",
        type="character",
        image_entry_id="asset-c1",
    )
    empty_result = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: []))
    db = SimpleNamespace(execute=AsyncMock(return_value=empty_result))
    prompt_context = {
        "schema": "shot_group.prompt-context.v1",
        "storyboard_provenance_sha256": "a" * 64,
    }

    await _refresh_group(
        db,
        group,
        [shot],
        background="办公室",
        narrative_purpose="建立空间",
        subjects=[subject],
        duration_by_shot={"shot-1": 3},
        prompt_context=prompt_context,
        source_storyboard_job_id="job-1",
    )

    assert group.prompt_context == prompt_context
    assert group.source_storyboard_job_id == "job-1"
    assert group.materials["character"][0]["ref"] == "C01"


async def test_refresh_group_does_not_self_validate_local_coverage_as_expected() -> None:
    from app.services.shot_group_merge import _refresh_group

    shot = SimpleNamespace(
        shot_id="shot-local-coverage",
        shot_group_id="draft-1",
        scene_no=1,
        group_no="G1",
        display_no=1,
        shot_no="old-1",
        start_seconds=0,
        end_seconds=3,
        shot_line="【镜头1:0s～3s】镜头一",
        content="镜头一",
        characters=["阿明"],
        props=[],
        sounds=[],
        dialogue_character="",
        structured_data=_structured_shot(1, 0, 3, source_block_ref="B99"),
    )
    group = SimpleNamespace(shot_group_id="final-1", scene_no=1, group_no="G1")
    subject = SimpleNamespace(
        id="subject-c1",
        name="阿明",
        type="character",
        image_entry_id=None,
    )
    empty_result = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: []))
    db = SimpleNamespace(execute=AsyncMock(return_value=empty_result))

    await _refresh_group(
        db,
        group,
        [shot],
        background="办公室",
        narrative_purpose="局部重算",
        subjects=[subject],
        duration_by_shot={"shot-local-coverage": 3},
    )

    assert group.quality_summary["coverage_block_refs"] == ["B99"]
    warning_codes = {
        item["code"] for item in group.quality_summary["warnings"]
    }
    assert "coverage_missing_block" not in warning_codes
    assert "coverage_unknown_block" not in warning_codes


async def test_refresh_group_keeps_incomplete_legacy_structure_compatible() -> None:
    from app.services.shot_group_merge import _refresh_group

    shot = SimpleNamespace(
        shot_id="legacy-shot",
        shot_group_id="draft-1",
        scene_no=1,
        group_no="G1",
        display_no=1,
        shot_no="old-1",
        start_seconds=0,
        end_seconds=3,
        shot_line="【镜头1:0s～3s】旧镜头",
        content="旧镜头",
        characters=[],
        props=[],
        sounds=[],
        dialogue_character="",
        structured_data={
            "visual": {
                "shot_size": "中景",
                "composition": ["单人"],
                "camera_angle": "平视",
                "camera_movement": "固定",
                "action": "角色走入画面",
            }
        },
    )
    group = SimpleNamespace(shot_group_id="final-1", scene_no=1, group_no="G1")
    db = SimpleNamespace(execute=AsyncMock())

    await _refresh_group(
        db,
        group,
        [shot],
        background="办公室",
        narrative_purpose="兼容旧镜头",
        subjects=[],
        duration_by_shot={"legacy-shot": 3},
    )

    assert "【镜头1:0s～3s】旧镜头" in group.prompt
    assert "【叙事目的】兼容旧镜头" in group.prompt
    assert group.quality_summary == {}
    assert "boundary_projections" not in group.reference_frames


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 镜头组合测试",
)
def test_postgres_merge_all_storyboard_shots_into_video_units() -> None:
    import asyncio

    from sqlalchemy import delete, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.errors import BadRequestError, ConflictError
    from app.core.ids import new_id
    from app.crud.shot import get_shot_hierarchy, get_video_workbench
    from app.models.episode import Episode
    from app.models.scene import Scene
    from app.models.script import Script
    from app.models.shot import Shot
    from app.models.shot_group import ShotGroup
    from app.models.subject import Subject
    from app.models.video_generation_job import VideoGenerationJob
    from app.services.shot_group_merge import merge_shots_into_groups
    from app.services.video_generation import create_video_job

    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(5)
        script_id = f"script-merge-{suffix}"
        episode_id = f"episode-merge-{suffix}"
        group_ids = [f"draft-{index}-{suffix}" for index in range(1, 4)]
        shot_ids = [f"shot-{index}-{suffix}" for index in range(1, 7)]
        group_specs = (
            (1, group_ids[0], (3, 2), "建立办公室冲突"),
            (1, group_ids[1], (2, 2), "推进人物行动"),
            (2, group_ids[2], (3, 3), "切换到街道"),
        )

        try:
            async with session_factory() as db:
                db.add(
                    Script(
                        id=script_id,
                        title="整集镜头组合测试",
                        scope="personal",
                        owner_user_id="user-merge",
                        created_by_user_id="user-merge",
                    )
                )
                await db.flush()
                db.add(
                    Episode(
                        episode_id=episode_id,
                        script_id=script_id,
                        episode_no=1,
                        title="第一集",
                        episode_version=1,
                    )
                )
                subjects = [
                    Subject(id=new_id(), script_id=script_id, type="background", name="办公室"),
                    Subject(id=new_id(), script_id=script_id, type="background", name="街道"),
                    Subject(id=new_id(), script_id=script_id, type="character", name="老姜"),
                    Subject(id=new_id(), script_id=script_id, type="prop", name="手机"),
                    Subject(id=new_id(), script_id=script_id, type="sound", name="环境音乐"),
                ]
                db.add_all(subjects)
                db.add_all(
                    [
                        Scene(
                            id=new_id(),
                            episode_id=episode_id,
                            scene_no=scene_no,
                            scene_key=f"bg:{background}",
                            background=background,
                            label=f"场 {scene_no} · {background}",
                            shot_group_count=0,
                            pending_shot_count=4 if scene_no == 1 else 2,
                        )
                        for scene_no, background in ((1, "办公室"), (2, "街道"))
                    ]
                )
                await db.flush()

                shot_index = 0
                for group_index, (scene_no, group_id, durations, narrative) in enumerate(
                    group_specs, start=1
                ):
                    background = "办公室" if scene_no == 1 else "街道"
                    db.add(
                        ShotGroup(
                            shot_group_id=group_id,
                            script_id=script_id,
                            episode_id=episode_id,
                            scene_no=scene_no,
                            group_index=group_index,
                            group_no=f"G{group_index}",
                            unit_id=f"draft:{group_id}",
                            unit_runtime_key=f"{episode_id}:{scene_no}:draft:{group_id}",
                            duration_seconds=sum(durations),
                            video_duration_seconds=sum(durations),
                            description_text=narrative,
                        )
                    )
                    await db.flush()
                    cursor = 0
                    for display_no, duration in enumerate(durations, start=1):
                        shot_id = shot_ids[shot_index]
                        shot_index += 1
                        db.add(
                            Shot(
                                shot_id=shot_id,
                                scene_no=scene_no,
                                shot_group_id=group_id,
                                group_no=f"G{group_index}",
                                shot_no=f"old-{display_no}",
                                display_no=display_no,
                                content=f"镜头{shot_index}",
                                description=f"镜头{shot_index}描述",
                                shot_line=(
                                    f"【镜头{display_no}:{cursor}s～{cursor + duration}s】"
                                    f"场景是{background}，老姜查看手机"
                                ),
                                start_seconds=cursor,
                                end_seconds=cursor + duration,
                                characters=["老姜"],
                                props=["手机"],
                                sounds=["环境音乐"],
                                dialogue_character="老姜" if display_no == 1 else "",
                            )
                        )
                        cursor += duration
                await db.commit()

            async with session_factory() as db:
                hierarchy = await get_shot_hierarchy(db, script_id, episode_id)
                assert [len(scene.groups) for scene in hierarchy.scenes] == [2, 1]
                assert [
                    sum(len(group.shots) for group in scene.groups) for scene in hierarchy.scenes
                ] == [4, 2]
                assert [scene.ungrouped_shots for scene in hierarchy.scenes] == [[], []]
                before = await get_video_workbench(db, script_id, episode_id)
                assert [scene.shot_groups for scene in before.scenes] == [[], []]
                assert [len(scene.pending_shots) for scene in before.scenes] == [4, 2]

                with pytest.raises(ConflictError, match="先完成镜头组合并"):
                    await create_video_job(
                        db,
                        script_id=script_id,
                        episode_id=episode_id,
                        shot_group_id=group_ids[0],
                        user_id="user-merge",
                        idempotency_key=f"draft-block-{suffix}",
                    )
                await db.rollback()

                with pytest.raises(BadRequestError, match="全部分镜 ID"):
                    await merge_shots_into_groups(
                        db,
                        script_id=script_id,
                        episode_id=episode_id,
                        shot_ids=shot_ids[:-1],
                        expected_episode_version=1,
                    )
                await db.rollback()

            async with session_factory() as db:
                result = await merge_shots_into_groups(
                    db,
                    script_id=script_id,
                    episode_id=episode_id,
                    shot_ids=list(reversed(shot_ids)),
                    expected_episode_version=1,
                )
                await db.commit()

            async with session_factory() as db:
                episode = await db.get(Episode, episode_id)
                assert episode is not None and episode.episode_version == 2
                assert len(result.shot_group_ids) == 3
                assert result.removed_shot_group_ids == tuple(group_ids)

                groups = list(
                    (
                        await db.execute(
                            select(ShotGroup)
                            .where(ShotGroup.episode_id == episode_id)
                            .order_by(ShotGroup.group_index)
                        )
                    ).scalars().all()
                )
                assert [group.duration_seconds for group in groups] == [5, 4, 6]
                assert [group.scene_no for group in groups] == [1, 1, 2]
                assert [group.group_no for group in groups] == ["G1", "G2", "G1"]
                assert all(group.unit_id == f"group:{group.shot_group_id}" for group in groups)
                assert all(3 <= group.duration_seconds <= 15 for group in groups)

                ordered_ids: list[str] = []
                for group in groups:
                    merged_shots = list(
                        (
                            await db.execute(
                                select(Shot)
                                .where(Shot.shot_group_id == group.shot_group_id)
                                .order_by(Shot.display_no)
                            )
                        ).scalars().all()
                    )
                    ordered_ids.extend(shot.shot_id for shot in merged_shots)
                    assert merged_shots[0].start_seconds == 0
                    assert merged_shots[-1].end_seconds == group.duration_seconds
                    assert "老姜" in group.prompt and "手机" in group.prompt
                    assert [item["name"] for item in group.materials["character"]] == ["老姜"]
                    assert [item["name"] for item in group.materials["prop"]] == ["手机"]
                    assert group.audio_elements["sounds"][0]["name"] == "环境音乐"
                assert ordered_ids == shot_ids

                workbench = await get_video_workbench(db, script_id, episode_id)
                assert [len(scene.shot_groups) for scene in workbench.scenes] == [2, 1]
                assert [scene.pending_shots for scene in workbench.scenes] == [[], []]
                job_count = (
                    await db.execute(
                        select(VideoGenerationJob.job_id).where(
                            VideoGenerationJob.episode_id == episode_id
                        )
                    )
                ).scalars().all()
                assert job_count == []
        finally:
            async with session_factory() as db:
                await db.execute(
                    delete(VideoGenerationJob).where(VideoGenerationJob.episode_id == episode_id)
                )
                await db.execute(delete(Shot).where(Shot.shot_id.in_(shot_ids)))
                await db.execute(delete(ShotGroup).where(ShotGroup.episode_id == episode_id))
                await db.execute(delete(Scene).where(Scene.episode_id == episode_id))
                await db.execute(delete(Subject).where(Subject.script_id == script_id))
                await db.execute(delete(Episode).where(Episode.episode_id == episode_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())
