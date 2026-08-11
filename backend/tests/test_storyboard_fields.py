"""分镜展示字段的历史文本解析与查询投影测试。"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

_DESCRIPTION = (
    "在【办公室】中采用【平视】拍摄视角，"
    "画面主体【阿明】的关键动作是【推门进入】，"
    "整体表现的画面是【阿明走进办公室】，"
    "表达了【紧张】，"
    "整体画面的光影效果是【冷色顶光】，"
    "整个镜头的叙事目的是【交代冲突】。"
)
_SHOT_LINE = (
    "【镜头1:0s～3s】场景是办公室，全景·平视，固定，"
    "冷色顶光，阿明推门进入，紧张，阿明:我们谈谈"
)


def _shot(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "shot_id": "shot-1",
        "scene_no": 1,
        "shot_group_id": "group-1",
        "group_no": "G1",
        "shot_no": "2x2x2x2x2x",
        "display_no": 1,
        "content": "阿明推门进入办公室。",
        "description": _DESCRIPTION,
        "shot_line": _SHOT_LINE,
        "start_seconds": 0,
        "end_seconds": 3,
        "characters": ["阿明"],
        "props": ["钥匙"],
        "sounds": ["门响"],
        "dialogue_character": "阿明",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_parse_storyboard_fields_reads_v1_fixed_templates() -> None:
    from app.core.storyboard_fields import parse_storyboard_fields

    fields = parse_storyboard_fields(
        _DESCRIPTION,
        _SHOT_LINE,
        display_no=1,
        start_seconds=0,
        end_seconds=3,
    )

    assert fields.model_dump() == {
        "scene": "办公室",
        "background": "办公室",
        "shot_size": "全景",
        "shot_scale": "全景",
        "camera_angle": "平视",
        "camera": "平视",
        "camera_movement": "固定",
        "action": "推门进入",
        "frame_description": "阿明走进办公室",
        "lighting": "冷色顶光",
        "emotion": "紧张",
        "narrative_purpose": "交代冲突",
        "dialogue_text": "我们谈谈",
        "dialogue_kind": None,
        "composition": None,
        "sound_design": None,
    }


def test_parse_storyboard_fields_reads_v2_composition_and_sound_design() -> None:
    from app.core.storyboard_fields import parse_storyboard_fields

    fields = parse_storyboard_fields(
        _DESCRIPTION,
        f"{_SHOT_LINE}，构图是【三分法、前景遮挡】，声音设计是【环境音/动作音描述】",
        display_no=1,
        start_seconds=0,
        end_seconds=3,
    )

    assert fields.dialogue_text == "我们谈谈"
    assert fields.composition == "三分法、前景遮挡"
    assert fields.sound_design == "环境音/动作音描述"


def test_parse_storyboard_fields_preserves_multiline_dialogue_and_rejects_unknown_kind() -> None:
    from app.core.storyboard_fields import parse_storyboard_fields

    multiline = parse_storyboard_fields(
        _DESCRIPTION,
        (
            f"{_SHOT_LINE}\n第二句，构图是【三分法】，"
            "声音设计是【环境音】，台词类型是【voice_over】"
        ),
        display_no=1,
        start_seconds=0,
        end_seconds=3,
    )
    assert multiline.dialogue_text == "我们谈谈\n第二句"
    assert multiline.dialogue_kind == "voice_over"
    assert multiline.composition == "三分法"
    assert multiline.sound_design == "环境音"

    unknown = parse_storyboard_fields(
        _DESCRIPTION,
        f"{_SHOT_LINE}，台词类型是【未知类型】",
        display_no=1,
        start_seconds=0,
        end_seconds=3,
    )
    assert unknown.dialogue_text == "我们谈谈"
    assert unknown.dialogue_kind is None


def test_parse_storyboard_fields_never_guesses_invalid_text_or_mismatched_timing() -> None:
    from app.core.storyboard_fields import parse_storyboard_fields

    invalid = parse_storyboard_fields(
        "自由描述，不符合固定模板",
        "任意镜头文本",
        display_no=1,
        start_seconds=0,
        end_seconds=3,
    )
    assert all(value is None for value in invalid.model_dump().values())

    timing_mismatch = parse_storyboard_fields(
        _DESCRIPTION,
        _SHOT_LINE,
        display_no=1,
        start_seconds=1,
        end_seconds=3,
    )
    assert timing_mismatch.background == "办公室"
    assert timing_mismatch.action == "推门进入"
    assert timing_mismatch.shot_scale is None
    assert timing_mismatch.camera_movement is None
    assert timing_mismatch.dialogue_text is None


def test_shot_out_projects_all_reliably_parsed_fields() -> None:
    from app.crud.shot import _shot_to_out

    result = _shot_to_out(
        _shot(
            shot_line=(
                f"{_SHOT_LINE}，构图是【三分法、前景遮挡】，"
                "声音设计是【环境音/动作音描述】"
            )
        )
    )

    assert result.background == "办公室"
    assert result.scene == "办公室"
    assert result.shot_size == result.shot_scale == "全景"
    assert result.camera_angle == result.camera == "平视"
    assert result.camera_movement == "固定"
    assert result.action == "推门进入"
    assert result.frame_description == "阿明走进办公室"
    assert result.lighting == "冷色顶光"
    assert result.emotion == "紧张"
    assert result.narrative_purpose == "交代冲突"
    assert result.dialogue_text == "我们谈谈"
    assert result.composition == "三分法、前景遮挡"
    assert result.sound_design == "环境音/动作音描述"


def test_shot_hierarchy_projects_scene_and_draft_group_fields() -> None:
    from app.crud.shot import get_shot_hierarchy

    shot = _shot()
    scene = SimpleNamespace(
        scene_key="bg:办公室",
        scene_no=1,
        background="办公室",
        label="场 1 · 办公室",
    )
    group = SimpleNamespace(
        shot_group_id="group-1",
        scene_no=1,
        group_no="G1",
        prompt="分镜提示词",
        duration_seconds=3,
        description_text="交代冲突",
        materials={"character": [{"name": "阿明"}], "background": [{"name": "办公室"}]},
        audio_elements={"characters": [{"name": "阿明"}], "sounds": [{"name": "门响"}]},
        reference_frames={},
        shots=[shot],
        group_index=1,
    )

    class _Result:
        def __init__(self, scalar: object | None = None, values: list[object] | None = None) -> None:
            self._scalar = scalar
            self._values = values or []

        def scalar_one_or_none(self) -> object | None:
            return self._scalar

        def scalars(self) -> _Result:
            return self

        def unique(self) -> _Result:
            return self

        def all(self) -> list[object]:
            return self._values

    class _DB:
        def __init__(self) -> None:
            self._results = [
                _Result(scalar=SimpleNamespace(episode_version=1)),
                _Result(values=[scene]),
                _Result(values=[group]),
                _Result(values=[shot]),
            ]

        async def execute(self, _statement: object) -> _Result:
            return self._results.pop(0)

    async def scenario() -> None:
        result = await get_shot_hierarchy(_DB(), "script-1", "episode-1")
        result_scene = result.scenes[0]
        result_group = result_scene.groups[0]
        assert (result_scene.scene_key, result_scene.background, result_scene.label) == (
            "bg:办公室",
            "办公室",
            "场 1 · 办公室",
        )
        assert result_group.duration_seconds == 3
        assert result_group.narrative_purpose == "交代冲突"
        assert [item.name for item in result_group.materials.background] == ["办公室"]
        assert result_group.audio_elements == [
            {"type": "character", "name": "阿明"},
            {"type": "sound", "name": "门响"},
        ]

    asyncio.run(scenario())


def test_breakdown_endpoint_projects_parsed_shot_and_group_fields(monkeypatch) -> None:
    from app.api.v1 import scripts as scripts_api

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_no=1,
        title="第一集",
        script_text="正文",
        script_file_storage_key=None,
        schema_version=1,
        episode_version=1,
        client_save_seq=0,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    group = SimpleNamespace(
        shot_group_id="group-1",
        scene_no=1,
        group_no="G1",
        prompt="分镜提示词",
        duration_seconds=3,
        description_text="交代冲突",
    )
    monkeypatch.setattr(scripts_api.script_crud, "get_script", AsyncMock(return_value=object()))
    monkeypatch.setattr(scripts_api, "check_script_access", AsyncMock())
    monkeypatch.setattr(
        scripts_api.script_crud,
        "get_breakdown",
        AsyncMock(
            return_value={
                "episode": episode,
                "subjects": [],
                "shots": [
                    _shot(
                        shot_line=(
                            f"{_SHOT_LINE}，构图是【三分法】，"
                            "声音设计是【门响后淡出】"
                        )
                    )
                ],
                "shot_groups": [group],
            }
        ),
    )
    monkeypatch.setattr(
        scripts_api,
        "get_subject_catalog_state",
        AsyncMock(return_value={"state": "current", "is_current": True}),
    )

    async def scenario() -> None:
        result = await scripts_api.get_breakdown(
            "script-1", "episode-1", db=object(), user=SimpleNamespace()
        )
        assert result.shots[0].shot_scale == "全景"
        assert result.shots[0].dialogue_text == "我们谈谈"
        assert result.shots[0].composition == "三分法"
        assert result.shots[0].sound_design == "门响后淡出"
        assert result.shot_groups[0].duration_seconds == 3
        assert result.shot_groups[0].narrative_purpose == "交代冲突"

    asyncio.run(scenario())
