"""T7 分镜 remediation 行为测试。

仅使用 fake / SQLite 级别对象，不连接真实 DB、Redis、AI 或网络。
"""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError

from app.core.errors import ConflictError, ForbiddenError, InsufficientCreditsError
from app.models.scene import Scene
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.storyboard_job import StoryboardJob


def _constraint_columns(model) -> list[set[str]]:
    return [
        {column.name for column in constraint.columns}
        for constraint in model.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    ]


def _valid_storyboard_payload() -> dict:
    return {
        "scenes": [
            {
                "scene_no": 1,
                "background": "办公室",
                "shot_groups": [
                    {
                        "group_no": "G1",
                        "narrative_purpose": "交代冲突",
                        "duration_seconds": 6,
                        "shots": [
                            {
                                "display_no": 1,
                                "content": "阿明推门进入办公室。",
                                "description": "在【办公室】中采用【平视】拍摄视角，画面主体【阿明】的关键动作是【推门进入】，整体表现的画面是【阿明走进办公室】，表达了【紧张】，整体画面的光影效果是【冷色顶光】，整个镜头的叙事目的是【交代冲突】。",
                                "shot_line": "【镜头1:0s～3s】场景是办公室，全景·平视，固定，冷色顶光，阿明推门进入，紧张，阿明:我们谈谈",
                                "start_seconds": 0,
                                "end_seconds": 3,
                                "characters": ["阿明"],
                                "dialogue_character": "阿明",
                                "props": ["钥匙"],
                                "sounds": ["门响"],
                            },
                            {
                                "display_no": 2,
                                "content": "小林抬头看向阿明。",
                                "description": "在【办公室】中采用【平视】拍摄视角，画面主体【小林】的关键动作是【抬头看向阿明】，整体表现的画面是【小林停下工作】，表达了【迟疑】，整体画面的光影效果是【冷色顶光】，整个镜头的叙事目的是【交代冲突】。",
                                "shot_line": "【镜头2:3s～6s】场景是办公室，中景·平视，固定，冷色顶光，小林抬头，迟疑，小林:",
                                "start_seconds": 3,
                                "end_seconds": 6,
                                "characters": ["小林"],
                                "dialogue_character": "",
                                "props": [],
                                "sounds": [],
                            },
                        ],
                    }
                ],
            }
        ]
    }


def _valid_storyboard_v2_payload() -> dict:
    return {
        "schema_version": "storyboard.v2",
        "scenes": [
            {
                "scene_no": 1,
                "background_ref": "BG01",
                "shot_groups": [
                    {
                        "group_no": "G1",
                        "narrative_purpose": "建立空间并交代冲突",
                        "duration_seconds": 6,
                        "shots": [
                            {
                                "display_no": 1,
                                "start_seconds": 0,
                                "end_seconds": 3,
                                "visual": {
                                    "shot_size": "全景",
                                    "composition": ["三分法", "纵深构图"],
                                    "camera_angle": "平视",
                                    "camera_movement": "推镜",
                                    "action": "阿明推门进入",
                                    "frame_description": "前景门框形成框景，阿明进入办公室",
                                    "lighting": "冷色顶光",
                                    "emotion": "紧张",
                                },
                                "narrative_purpose": "交代阿明进入并触发冲突",
                                "character_refs": ["C01"],
                                "prop_refs": ["P01"],
                                "sound_refs": ["S01"],
                                "sound_design": "门轴轻响，室内环境声短暂停顿",
                                "dialogue": {
                                    "speaker_ref": "C01",
                                    "text": "我们谈谈",
                                    "kind": "dialogue",
                                },
                            },
                            {
                                "display_no": 2,
                                "start_seconds": 3,
                                "end_seconds": 6,
                                "visual": {
                                    "shot_size": "近景",
                                    "composition": ["单人", "三分法"],
                                    "camera_angle": "平视",
                                    "camera_movement": "固定",
                                    "action": "小林停下工作并抬头",
                                    "frame_description": "小林越过前景文件看向门口",
                                    "lighting": "冷色顶光勾勒面部",
                                    "emotion": "迟疑",
                                },
                                "narrative_purpose": "用反应镜承接前一镜",
                                "character_refs": ["C02"],
                                "prop_refs": [],
                                "sound_refs": [],
                                "sound_design": "无",
                                "dialogue": None,
                            },
                        ],
                    }
                ],
            }
        ],
    }


def _subject_catalog() -> dict[str, str]:
    return {
        "办公室": "background",
        "阿明": "character",
        "小林": "character",
        "钥匙": "prop",
        "门响": "sound",
    }


def _subject_references() -> dict[str, SimpleNamespace]:
    return {
        "BG01": SimpleNamespace(name="办公室", subject_type="background"),
        "C01": SimpleNamespace(name="阿明", subject_type="character"),
        "C02": SimpleNamespace(name="小林", subject_type="character"),
        "P01": SimpleNamespace(name="钥匙", subject_type="prop"),
        "S01": SimpleNamespace(name="门响", subject_type="sound"),
    }


def test_storyboard_models_declare_required_state_and_uniqueness() -> None:
    assert {
        "episode_version",
        "input_version",
        "attempt",
        "idempotency_key",
        "lease_owner",
        "lease_token",
        "lease_until",
        "heartbeat_at",
        "started_at",
        "finished_at",
        "error_type",
        "retryable",
        "canceled_at",
        "reservation_id",
        "input_client_save_seq",
    }.issubset(StoryboardJob.__table__.columns.keys())

    assert {"episode_id", "attempt"} in _constraint_columns(StoryboardJob)
    assert {"episode_id", "idempotency_key"} in _constraint_columns(StoryboardJob)
    assert {"episode_id", "scene_no"} in _constraint_columns(Scene)
    assert {"episode_id", "group_index"} in _constraint_columns(ShotGroup)
    assert {"shot_group_id", "display_no"} in _constraint_columns(Shot)
    assert {"shot_group_id", "shot_no"} in _constraint_columns(Shot)


def test_storyboard_contract_accepts_valid_payload_and_rejects_extra_fields() -> None:
    from app.schemas.storyboard_contracts import StoryboardContract, validate_storyboard_contract

    contract = validate_storyboard_contract(_valid_storyboard_payload(), _subject_catalog())
    assert isinstance(contract, StoryboardContract)

    invalid = _valid_storyboard_payload()
    invalid["unexpected"] = True
    with pytest.raises(ValueError):
        validate_storyboard_contract(invalid, _subject_catalog())


def test_storyboard_v2_contract_renders_legacy_fields_deterministically() -> None:
    from app.core.storyboard_fields import parse_storyboard_fields
    from app.services import storyboard as storyboard_service

    contract = storyboard_service._validate_provider_payload(
        _valid_storyboard_v2_payload(),
        subject_catalog=_subject_catalog(),
        subject_reference_names={
            ref: item.name for ref, item in _subject_references().items()
        },
        subject_reference_types={
            ref: item.subject_type for ref, item in _subject_references().items()
        },
    )

    shot = contract.scenes[0].shot_groups[0].shots[0]
    assert shot.content == "前景门框形成框景，阿明进入办公室"
    assert shot.characters == ["阿明"]
    assert shot.props == ["钥匙"]
    assert shot.sounds == ["门响"]
    assert shot.dialogue_character == "阿明"
    assert "全景·平视，推镜" in shot.shot_line
    assert "构图是【三分法、纵深构图】" in shot.shot_line
    assert "声音设计是【门轴轻响，室内环境声短暂停顿】" in shot.shot_line
    assert "整个镜头的叙事目的是【交代阿明进入并触发冲突】" in shot.description
    fields = parse_storyboard_fields(
        shot.description,
        shot.shot_line,
        display_no=shot.display_no,
        start_seconds=shot.start_seconds,
        end_seconds=shot.end_seconds,
    )
    assert fields.scene == "办公室"
    assert fields.shot_size == "全景"
    assert fields.camera_movement == "推镜"
    assert fields.action == "阿明推门进入"
    assert fields.narrative_purpose == "交代阿明进入并触发冲突"
    assert fields.composition == "三分法、纵深构图"
    assert fields.sound_design == "门轴轻响，室内环境声短暂停顿"


def test_storyboard_v2_contract_accepts_single_shot_video_unit() -> None:
    """定场、反应和转场允许作为单镜头 3 秒视频单元落库。"""
    from app.services import storyboard as storyboard_service

    payload = _valid_storyboard_v2_payload()
    group = payload["scenes"][0]["shot_groups"][0]
    group["duration_seconds"] = 3
    group["shots"] = [group["shots"][0]]
    group["shots"][0]["end_seconds"] = 3

    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names={
            ref: item.name for ref, item in _subject_references().items()
        },
        subject_reference_types={
            ref: item.subject_type for ref, item in _subject_references().items()
        },
    )

    assert len(contract.scenes[0].shot_groups[0].shots) == 1
    assert contract.scenes[0].shot_groups[0].duration_seconds == 3


def test_storyboard_v2_continuity_diagnostic_has_safe_rule_code_and_path() -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_v2_contract

    payload = _valid_storyboard_v2_payload()
    payload["scenes"][0]["shot_groups"][0]["shots"][1]["start_seconds"] = 4

    with pytest.raises(ValueError) as exc_info:
        validate_storyboard_v2_contract(
            payload,
            {
                ref: item.subject_type for ref, item in _subject_references().items()
            },
        )

    assert getattr(exc_info.value, "diagnostic") == {
        "version": 1,
        "category": "semantic_validation",
        "schema": "StoryboardV2Contract",
        "validation": {
            "error_count": 1,
            "codes": ["shot_time_not_continuous"],
            "paths": ["scenes[].shot_groups[].shots[].start_seconds"],
        },
    }


def test_storyboard_v2_voice_over_without_speaker_preserves_kind_and_multiline_text() -> None:
    from app.core.storyboard_fields import parse_storyboard_fields
    from app.services import storyboard as storyboard_service

    payload = _valid_storyboard_v2_payload()
    payload["scenes"][0]["shot_groups"][0]["shots"][0]["dialogue"] = {
        "speaker_ref": None,
        "text": "第一句旁白\n第二句旁白",
        "kind": "voice_over",
    }
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names={
            ref: item.name for ref, item in _subject_references().items()
        },
        subject_reference_types={
            ref: item.subject_type for ref, item in _subject_references().items()
        },
    )

    shot = contract.scenes[0].shot_groups[0].shots[0]
    assert shot.dialogue_character == ""
    fields = parse_storyboard_fields(
        shot.description,
        shot.shot_line,
        display_no=shot.display_no,
        start_seconds=shot.start_seconds,
        end_seconds=shot.end_seconds,
    )
    assert fields.dialogue_text == "第一句旁白\n第二句旁白"
    assert fields.dialogue_kind == "voice_over"


def test_storyboard_v2_dialogue_requires_visible_character_speaker() -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_v2_contract

    payload = _valid_storyboard_v2_payload()
    payload["scenes"][0]["shot_groups"][0]["shots"][0]["dialogue"][
        "speaker_ref"
    ] = None

    with pytest.raises(ValueError, match="speaker_ref"):
        validate_storyboard_v2_contract(
            payload,
            {
                ref: item.subject_type for ref, item in _subject_references().items()
            },
        )


def test_storyboard_v2_cross_type_same_name_keeps_reference_identity() -> None:
    from app.services import storyboard as storyboard_service

    references = _subject_references()
    reference_names = {ref: item.name for ref, item in references.items()}
    reference_names["C01"] = "同名主体"
    reference_names["P01"] = "同名主体"
    contract = storyboard_service._validate_provider_payload(
        _valid_storyboard_v2_payload(),
        subject_catalog={"办公室": "background", "同名主体": "ambiguous", "小林": "character", "门响": "sound"},
        subject_reference_names=reference_names,
        subject_reference_types={
            ref: item.subject_type for ref, item in references.items()
        },
    )

    shot = contract.scenes[0].shot_groups[0].shots[0]
    assert shot.characters == ["同名主体"]
    assert shot.props == ["同名主体"]


def test_storyboard_v2_contract_rejects_wrong_subject_reference_type() -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_v2_contract

    payload = _valid_storyboard_v2_payload()
    payload["scenes"][0]["shot_groups"][0]["shots"][0]["prop_refs"] = ["S01"]
    reference_types = {
        ref: item.subject_type for ref, item in _subject_references().items()
    }
    with pytest.raises(ValueError, match="prop_refs"):
        validate_storyboard_v2_contract(payload, reference_types)


def test_storyboard_v2_contract_accepts_four_digit_subject_reference() -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_v2_contract

    payload = _valid_storyboard_v2_payload()
    first_shot = payload["scenes"][0]["shot_groups"][0]["shots"][0]
    first_shot["character_refs"] = ["C1000"]
    first_shot["dialogue"]["speaker_ref"] = "C1000"
    reference_types = {
        ref: item.subject_type for ref, item in _subject_references().items()
    }
    reference_types["C1000"] = "character"

    contract = validate_storyboard_v2_contract(payload, reference_types)

    assert contract.scenes[0].shot_groups[0].shots[0].character_refs == ["C1000"]


def test_storyboard_contract_rejects_empty_scene_groups_and_empty_global_product() -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_contract

    scene_without_groups = _valid_storyboard_payload()
    scene_without_groups["scenes"][0]["shot_groups"] = []
    with pytest.raises(ValueError, match="shot_group"):
        validate_storyboard_contract(scene_without_groups, _subject_catalog())

    empty_product = {"scenes": []}
    with pytest.raises(ValueError, match="scenes"):
        validate_storyboard_contract(empty_product, _subject_catalog())


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda p: p["scenes"][0].update(scene_no=2), "scene_no"),
        (lambda p: p["scenes"][0]["shot_groups"][0].update(group_no="G2"), "group_no"),
        (lambda p: p["scenes"][0]["shot_groups"][0].update(duration_seconds=16), "duration"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][1].update(display_no=3), "display_no"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][1].update(start_seconds=4), "continuous"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(start_seconds=0.5), "start_seconds"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(end_seconds=0), "greater"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(characters=["外部角色"]), "subject"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(description="自由发挥"), "description"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(shot_line="镜头1 0-3"), "shot_line"),
    ],
)
def test_storyboard_contract_rejects_strict_scene_group_shot_rules(mutate, message) -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_contract

    payload = _valid_storyboard_payload()
    mutate(payload)
    with pytest.raises(ValueError, match=message):
        validate_storyboard_contract(payload, _subject_catalog())


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda p: p["scenes"][0].update(background="阿明"), "background"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(characters=["门响"]), "character"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(dialogue_character="门响"), "character"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(props=["办公室"]), "prop"),
        (lambda p: p["scenes"][0]["shot_groups"][0]["shots"][0].update(sounds=["钥匙"]), "sound"),
    ],
)
def test_storyboard_contract_rejects_subject_type_mismatch(mutate, message) -> None:
    from app.schemas.storyboard_contracts import validate_storyboard_contract

    payload = _valid_storyboard_payload()
    mutate(payload)
    with pytest.raises(ValueError, match=message):
        validate_storyboard_contract(payload, _subject_catalog())


def test_prompt_contains_all_nine_constraints() -> None:
    from app.services.storyboard import PROMPT_CONSTRAINTS, _build_group_prompt

    prompt = _build_group_prompt(
        ["【镜头1:0s～3s】场景是办公室，全景·平视，固定，冷色顶光，阿明进入，紧张，阿明:我们谈谈"],
        ["办公室", "阿明"],
        "交代冲突",
    )
    assert len(PROMPT_CONSTRAINTS) == 9
    for index, constraint in enumerate(PROMPT_CONSTRAINTS, start=1):
        assert f"{index}. {constraint}" in prompt


def test_group_prompt_uses_compressed_director_commands_without_full_constraint_lock() -> None:
    from app.services.storyboard import PROMPT_CONSTRAINTS, _build_group_prompt

    prompt = _build_group_prompt(
        ["【镜头1:0s～3s】画面"],
        ["阿明"],
        "建立冲突",
        director_commands=["【导演指令】镜头职责=single"],
    )

    assert "【导演指令】镜头职责=single" in prompt
    assert "【约束】" not in prompt
    assert all(constraint not in prompt for constraint in PROMPT_CONSTRAINTS)


def test_group_prompt_template_switches_only_render_variables_for_v2_summary() -> None:
    from app.services.prompt_template import fallback_snapshot
    from app.services.storyboard import CONSTRAINT_LOCK, _build_group_prompt

    template = fallback_snapshot(
        "storyboard.group",
        "{{ shot_lines }}|{{ director_commands }}|{{ constraint_lock }}",
    )

    structured = _build_group_prompt(
        ["镜头行"],
        ["阿明"],
        "建立冲突",
        director_commands=["  【导演指令】镜头职责=single  "],
        group_template=template,
    )
    legacy = _build_group_prompt(
        ["镜头行"],
        ["阿明"],
        "建立冲突",
        group_template=template,
    )

    assert structured == "镜头行|【导演指令】镜头职责=single\n|"
    assert CONSTRAINT_LOCK not in structured
    assert legacy == f"镜头行||{CONSTRAINT_LOCK}"
    assert template.source == "fallback"
    assert template.version == 0


async def test_selected_scene_audition_block_only_includes_selected_current_plan() -> None:
    from app.services.storyboard import _selected_scene_audition_block

    selected = SimpleNamespace(
        scene_no=2,
        selected_plan_id="plan-b",
        plans=[
            {
                "plan_id": "plan-a",
                "name": "未选方案",
                "audience_knowledge_timing": "旧一",
                "alignment_target": "旧二",
                "performance_space": "旧三",
                "strongest_image": "旧四",
                "landing": "旧五",
                "tradeoff": "旧六",
            },
            {
                "plan_id": "plan-b",
                "name": "确认方案",
                "audience_knowledge_timing": "答案一",
                "alignment_target": "答案二",
                "performance_space": "答案三",
                "strongest_image": "答案四",
                "landing": "答案五",
                "tradeoff": "答案六",
            },
        ],
    )
    result = SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: [selected]),
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=result))

    block = await _selected_scene_audition_block(
        db,
        script_id="script-1",
        episode_id="episode-1",
        input_version=7,
    )

    assert "确认方案" in block
    assert all(f"答案{suffix}" in block for suffix in "一二三四五六")
    assert "未选方案" not in block
    statement = db.execute.await_args.args[0]
    assert "scene_auditions.input_version" in str(statement)
    assert "scene_auditions.status" in str(statement)


async def test_storyboard_prompt_bundle_falls_back_per_invalid_runtime_template() -> None:
    from app.services import storyboard as storyboard_service
    from app.services.prompt_template import PromptTemplateSnapshot, fallback_snapshot

    broken = PromptTemplateSnapshot(
        key="storyboard.description",
        template_id="template-broken",
        version=9,
        content_sha256="a" * 64,
        content="{{ missing_variable }}",
        source="database",
    )
    fallbacks = {
        "storyboard.system": storyboard_service._SYSTEM_PROMPT,
        "storyboard.user": storyboard_service._USER_PROMPT_TEMPLATE_FALLBACK,
        "storyboard.description": storyboard_service._DESCRIPTION_TEMPLATE_FALLBACK,
        "storyboard.shot": storyboard_service._SHOT_TEMPLATE_FALLBACK,
        "storyboard.group": storyboard_service._GROUP_TEMPLATE_FALLBACK,
        "storyboard.constraint": storyboard_service.CONSTRAINT_LOCK,
    }
    snapshots = {
        key: fallback_snapshot(key, content)
        for key, content in fallbacks.items()
    }
    snapshots["storyboard.description"] = broken
    db = SimpleNamespace()
    with patch.object(
        storyboard_service,
        "resolve_prompt_bundle",
        AsyncMock(return_value=snapshots),
    ):
        context = await storyboard_service._resolve_storyboard_prompt_context(
            db,
            default_user_prompt="用户正文",
        )

    assert context.user_prompt == "用户正文"
    assert context.templates["storyboard.description"].source == "fallback"
    assert context.templates["storyboard.description"].version == 0


def test_storyboard_prompt_uses_short_subject_refs_without_server_ids() -> None:
    from app.services import storyboard as storyboard_service

    subjects = [
        SimpleNamespace(
            id="private-subject-uuid",
            name="办公室",
            type="background",
            episode_nos=[1],
            description="现代办公室",
            image_entry_id="private-asset-uuid",
            is_active=True,
        ),
        SimpleNamespace(
            id="private-character-uuid",
            name="阿明",
            type="character",
            episode_nos=[1],
            description="项目负责人",
            image_entry_id=None,
            is_active=True,
        ),
    ]
    prompt = storyboard_service._build_user_prompt(
        SimpleNamespace(episode_no=1, title="第一集", script_text="阿明进入办公室。"),
        subjects,
    )

    assert "BG01 | background | 办公室" in prompt
    assert "C01 | character | 阿明" in prompt
    assert "private-subject-uuid" not in prompt
    assert "private-asset-uuid" not in prompt


async def test_storyboard_llm_call_uses_shared_provider_factory(monkeypatch) -> None:
    from app.services import storyboard as storyboard_service

    class Provider:
        async def complete_json(self, system: str, user: str) -> dict:
            assert system == "system"
            assert user == "user"
            return {"scenes": []}

    monkeypatch.setattr("app.providers.get_llm_provider", lambda: Provider())
    assert await storyboard_service._call_llm("system", "user") == {"scenes": []}


async def test_storyboard_llm_call_passes_v2_schema_to_capable_provider(monkeypatch) -> None:
    from app.services import storyboard as storyboard_service

    class Provider:
        async def complete_json(
            self,
            system: str,
            user: str,
            *,
            response_schema: dict | None = None,
        ) -> dict:
            assert system == "system"
            assert user == "user"
            assert response_schema == storyboard_service._PROVIDER_RESPONSE_SCHEMA
            return {"scenes": []}

    monkeypatch.setattr("app.providers.get_llm_provider", lambda: Provider())
    assert await storyboard_service._call_llm("system", "user") == {"scenes": []}


async def test_storyboard_repairs_invalid_contract_once_without_resending_script(monkeypatch) -> None:
    """非整数时码属类型错误、不可确定性归一化，应交给一次 LLM 定向修复，且不重发剧本。"""
    from app.services import storyboard as storyboard_service

    invalid_timing = _valid_storyboard_v2_payload()
    # start_seconds 给非整数字符串：归一化只处理整数时码，类型错误会穿透到严格校验
    invalid_timing["scenes"][0]["shot_groups"][0]["shots"][1]["start_seconds"] = "3"
    call_llm = AsyncMock(
        side_effect=[
            invalid_timing,
            _valid_storyboard_v2_payload(),
        ]
    )
    monkeypatch.setattr(storyboard_service, "_call_llm", call_llm)

    contract = await storyboard_service._call_llm_with_contract(
        "system",
        "SCRIPT_PRIVATE_SENTINEL",
        _subject_catalog(),
        subject_references=_subject_references(),
    )

    assert contract.scenes[0].shot_groups[0].shots[1].display_no == 2
    assert call_llm.await_count == 2
    repair_call = call_llm.await_args_list[1]
    assert repair_call.args[0] == storyboard_service._REPAIR_SYSTEM_PROMPT
    assert "SCRIPT_PRIVATE_SENTINEL" not in repair_call.args[1]
    assert "scenes[].shot_groups[].shots[].start_seconds" in repair_call.args[1]



async def test_storyboard_normalizes_timecode_drift_without_llm_repair(
    monkeypatch,
) -> None:
    """时码连续性是可派生元数据，漂移应由服务端归一化，不交给 LLM 重试。"""
    from app.services import storyboard as storyboard_service

    invalid_timing = _valid_storyboard_v2_payload()
    # 相邻镜头首尾未对齐（shot2 起始 4s 而非 3s），单镜时长仍为 3s，属算术漂移
    invalid_timing["scenes"][0]["shot_groups"][0]["shots"][1]["start_seconds"] = 4
    invalid_timing["scenes"][0]["shot_groups"][0]["shots"][1]["end_seconds"] = 7
    call_llm = AsyncMock(side_effect=[invalid_timing])
    monkeypatch.setattr(storyboard_service, "_call_llm", call_llm)

    contract = await storyboard_service._call_llm_with_contract(
        "system",
        "SCRIPT_PRIVATE_SENTINEL",
        _subject_catalog(),
        subject_references=_subject_references(),
    )

    group = contract.scenes[0].shot_groups[0]
    assert group.shots[0].start_seconds == 0
    assert group.shots[0].end_seconds == 3
    assert group.shots[1].start_seconds == 3
    assert group.shots[1].end_seconds == 6
    assert group.duration_seconds == 6
    # 时码漂移已在服务端归一化，无需触发 LLM 修复，原剧本不入修复请求
    assert call_llm.await_count == 1


def test_normalize_provider_continuity_renumbers_and_scales_timecodes() -> None:
    """编号与整数时码属于可派生元数据，归一化应按数组顺序重建并校正总时长。"""
    from app.services import storyboard as storyboard_service

    payload = {
        "schema_version": "storyboard.v2",
        "scenes": [
            {
                "scene_no": 3,  # 非连续编号
                "background_ref": "BG01",
                "shot_groups": [
                    {
                        "group_no": "G2",  # 非连续编号
                        "narrative_purpose": "建立空间",
                        "duration_seconds": 8,  # 与镜头时长不一致
                        "shots": [
                            {
                                "display_no": 5,  # 非连续编号
                                "start_seconds": 0,
                                "end_seconds": 3,
                                "visual": {
                                    "shot_size": "全景",
                                    "composition": ["三分法"],
                                    "camera_angle": "平视",
                                    "camera_movement": "推镜",
                                    "action": "角色进入",
                                    "frame_description": "门框前景，角色进入",
                                    "lighting": "冷色顶光",
                                    "emotion": "紧张",
                                },
                                "narrative_purpose": "交代进入",
                                "character_refs": ["C01"],
                                "prop_refs": [],
                                "sound_refs": [],
                                "sound_design": "门轴轻响",
                                "dialogue": None,
                            },
                            {
                                "display_no": 9,  # 非连续编号
                                "start_seconds": 5,  # 与上一镜 3 未对齐（漂移）
                                "end_seconds": 6,
                                "visual": {
                                    "shot_size": "近景",
                                    "composition": ["单人"],
                                    "camera_angle": "平视",
                                    "camera_movement": "固定",
                                    "action": "抬头回应",
                                    "frame_description": "角色抬头",
                                    "lighting": "冷色顶光",
                                    "emotion": "迟疑",
                                },
                                "narrative_purpose": "反应镜",
                                "character_refs": ["C02"],
                                "prop_refs": [],
                                "sound_refs": [],
                                "sound_design": "无",
                                "dialogue": None,
                            },
                        ],
                    }
                ],
            }
        ],
    }
    reference_types = {
        ref: item.subject_type for ref, item in _subject_references().items()
    }
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names={
            ref: item.name for ref, item in _subject_references().items()
        },
        subject_reference_types=reference_types,
    )
    scene = contract.scenes[0]
    group = scene.shot_groups[0]
    # 编号按数组顺序重建
    assert scene.scene_no == 1
    assert group.group_no == "G1"
    assert [shot.display_no for shot in group.shots] == [1, 2]
    # 组时长声明 8s，单镜时长 [3,1]=4s，等比例缩放后总和精确为 8
    assert group.duration_seconds == 8
    shots = group.shots
    assert shots[0].start_seconds == 0
    assert shots[1].start_seconds == shots[0].end_seconds  # 首尾相接
    assert shots[1].end_seconds == 8  # 末镜对齐组时长
    assert all(
        shot.end_seconds > shot.start_seconds for shot in shots
    )


def test_normalize_provider_continuity_repairs_misplaced_and_overshot_fields() -> None:
    """字段错放、构图超 3/重复、名称当引用、文本超长都应被确定性归一化。"""
    from app.services import storyboard as storyboard_service

    reference_names = {
        ref: item.name for ref, item in _subject_references().items()
    }
    reference_types = {
        ref: item.subject_type for ref, item in _subject_references().items()
    }
    payload = _valid_storyboard_v2_payload()
    shot = payload["scenes"][0]["shot_groups"][0]["shots"][0]
    # narrative_purpose 错放进 visual，镜头层级缺失
    shot["visual"]["narrative_purpose"] = shot.pop("narrative_purpose")
    # 构图 4 项且重复 -> 去重截断到 3
    shot["visual"]["composition"] = ["三分法", "三分法", "纵深构图", "框景"]
    # 引用用名称而非短引用 -> 名称映射回短引用
    shot["character_refs"] = ["阿明"]
    shot["prop_refs"] = ["钥匙"]
    shot["sound_refs"] = ["门响"]
    shot["dialogue"]["speaker_ref"] = "阿明"
    # 文本超长 -> 截断
    shot["visual"]["action"] = "动" * 2000
    shot["visual"]["lighting"] = "光" * 400
    # schema_version 拼错 -> 强制 v2
    payload["schema_version"] = "storyboard.v2-typo"

    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    result = contract.scenes[0].shot_groups[0].shots[0]
    # 归一化成功即证明：visual.narrative_purpose 已回迁(否则 extra_forbidden)、
    # composition 已去重截断(否则 too_many)、文本已截断(否则 string_too_long)、
    # 名称已映射回短引用(否则 reference_not_allowed)、schema_version 已纠正。
    assert "交代阿明进入并触发冲突" in result.description
    assert result.characters == ["阿明"]
    assert result.props == ["钥匙"]
    assert result.sounds == ["门响"]
    assert result.dialogue_character == "阿明"


def test_normalize_provider_continuity_drops_unresolvable_optional_refs() -> None:
    """无法解析的可选引用应被丢弃而非卡住生成。"""
    from app.services import storyboard as storyboard_service

    reference_names = {
        ref: item.name for ref, item in _subject_references().items()
    }
    reference_types = {
        ref: item.subject_type for ref, item in _subject_references().items()
    }
    payload = _valid_storyboard_v2_payload()
    shot = payload["scenes"][0]["shot_groups"][0]["shots"][0]
    # 既非合法短引用、又非已知名称的垃圾引用 -> 丢弃
    shot["character_refs"] = ["C01", "GARBAGE99", "阿明"]
    shot["prop_refs"] = ["未知道具"]
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    result = contract.scenes[0].shot_groups[0].shots[0]
    assert result.characters == ["阿明"]  # C01 渲染为阿明，垃圾被丢
    assert result.props == []  # 未知道具无法解析被丢弃


def test_coverage_minimums_and_check_distinguish_sparse_from_rich() -> None:
    """覆盖度下限应让稀疏输出不合格、让充分输出合格。"""
    from app.services import storyboard as storyboard_service

    # 1365 字 -> min_shots=23 / min_groups=8 / min_scenes=3 / min_duration=92
    script = "剧" * 1365
    min_shots, min_groups, min_scenes, min_dur = (
        storyboard_service._coverage_minimums(script)
    )
    assert (min_shots, min_groups, min_scenes, min_dur) == (23, 8, 3, 92)

    # 极短剧本走 floor，不坍缩到 1
    assert storyboard_service._coverage_minimums("短") == (6, 3, 2, 24)

    # 用 mock 构造稀疏(3镜)与充分(24镜)两种 _ValidatedStoryboard
    def make_report(shot_count, group_count, scene_count, total_duration):
        return storyboard_service._CoverageReport(
            shot_count=shot_count,
            group_count=group_count,
            scene_count=scene_count,
            total_duration=total_duration,
            min_shots=23,
            min_groups=8,
            min_scenes=3,
            min_duration=92,
        )

    # 差输出 1场1组3镜15s -> 不足
    assert not make_report(3, 1, 1, 15).sufficient
    # 好输出 4场9组24镜108s -> 合格
    assert make_report(24, 9, 4, 108).sufficient
    # 边界：达到 80% slack 即合格（19/7/..）
    assert make_report(19, 7, 4, 74).sufficient


def test_normalize_provider_continuity_coerces_invalid_enums() -> None:
    """模型自造的非法枚举值应被收敛到中性默认，不阻断生成。"""
    from app.services import storyboard as storyboard_service

    reference_names = {
        ref: item.name for ref, item in _subject_references().items()
    }
    reference_types = {
        ref: item.subject_type for ref, item in _subject_references().items()
    }
    payload = _valid_storyboard_v2_payload()
    visual = payload["scenes"][0]["shot_groups"][0]["shots"][0]["visual"]
    visual["shot_size"] = "超广角"  # 非法
    visual["camera_angle"] = "斜上方"  # 非法
    visual["camera_movement"] = "甩镜头"  # 非法
    visual["composition"] = ["自造构图", "三分法", "三分法", "纵深构图"]  # 含非法+重复
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    # 归一化成功即证明非法枚举已收敛为合法默认值（否则 literal_error）
    shot = contract.scenes[0].shot_groups[0].shots[0]
    assert "平视" in shot.description


def test_normalize_provider_continuity_repairs_dialogue_speaker_visibility() -> None:
    """对白 speaker 缺失/不在 character_refs 应被确定性收敛，不抛 value_error 卡住生成。"""
    from app.services import storyboard as storyboard_service

    reference_names = {
        ref: item.name for ref, item in _subject_references().items()
    }
    reference_types = {
        ref: item.subject_type for ref, item in _subject_references().items()
    }

    # 分支 b：speaker 合法但不在 character_refs -> 补入
    payload = _valid_storyboard_v2_payload()
    shot = payload["scenes"][0]["shot_groups"][0]["shots"][0]
    shot["character_refs"] = []  # 清空，speaker C01 不在其中
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    assert contract.scenes[0].shot_groups[0].shots[0].dialogue_character == "阿明"

    # 分支 a：kind=dialogue 但 speaker_ref=None -> 从 character_refs 回填
    payload = _valid_storyboard_v2_payload()
    shot = payload["scenes"][0]["shot_groups"][0]["shots"][0]
    shot["dialogue"]["speaker_ref"] = None
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    assert contract.scenes[0].shot_groups[0].shots[0].dialogue_character == "阿明"

    # 分支 a 兜底：speaker=None 且 character_refs 也无角色 -> 降级 voice_over
    payload = _valid_storyboard_v2_payload()
    shot = payload["scenes"][0]["shot_groups"][0]["shots"][0]
    shot["character_refs"] = ["P01"]  # 只有道具，无角色可回填
    shot["dialogue"]["speaker_ref"] = None
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    # 降级为 voice_over，台词文本保留，不阻断生成
    result = contract.scenes[0].shot_groups[0].shots[0]
    assert result.dialogue_character == ""


def test_normalize_provider_continuity_resolves_background_ref_strongly() -> None:
    """background_ref 为 null/非法/名称时应强解析为合法背景引用，不卡死生成。"""
    from app.services import storyboard as storyboard_service

    reference_names = {
        ref: item.name for ref, item in _subject_references().items()
    }
    reference_types = {
        ref: item.subject_type for ref, item in _subject_references().items()
    }

    # 情形一：background_ref=null -> 兜底首个背景引用 BG01
    payload = _valid_storyboard_v2_payload()
    payload["scenes"][0]["background_ref"] = None
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    assert contract.scenes[0].background == "办公室"  # BG01 -> 办公室

    # 情形二：background_ref 写中文名 -> 名称映射
    payload = _valid_storyboard_v2_payload()
    payload["scenes"][0]["background_ref"] = "办公室"
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    assert contract.scenes[0].background == "办公室"

    # 情形三：background_ref 是非法自造值 -> 兜底首个背景引用
    payload = _valid_storyboard_v2_payload()
    payload["scenes"][0]["background_ref"] = "不存在的场景XX"
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    assert contract.scenes[0].background == "办公室"


def test_normalize_provider_continuity_clamps_oversized_group_duration() -> None:
    """组时长越界（>15s）绝不卡住生成：按镜头总时长是否合法分别处理。"""
    from app.services import storyboard as storyboard_service

    reference_names = {
        ref: item.name for ref, item in _subject_references().items()
    }
    reference_types = {
        ref: item.subject_type for ref, item in _subject_references().items()
    }

    # 情形一：声明 24s，但镜头实际只占 6s（声明虚高）-> 采用镜头总时长 6s
    payload = _valid_storyboard_v2_payload()
    payload["scenes"][0]["shot_groups"][0]["duration_seconds"] = 24
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    group_a = contract.scenes[0].shot_groups[0]
    assert group_a.duration_seconds == 6
    assert [s.end_seconds - s.start_seconds for s in group_a.shots] == [3, 3]
    assert group_a.shots[-1].end_seconds == 6

    # 情形二：声明 24s，镜头总时长也 24s（4 镜 ×6s 越界）-> 夹到 15s 并等比例缩放
    payload = _valid_storyboard_v2_payload()
    group = payload["scenes"][0]["shot_groups"][0]
    group["duration_seconds"] = 24
    group["shots"][0]["start_seconds"] = 0
    group["shots"][0]["end_seconds"] = 6
    group["shots"][1]["start_seconds"] = 6
    group["shots"][1]["end_seconds"] = 12
    group["shots"].append(dict(group["shots"][1]))  # 复制结构补两镜
    group["shots"][2]["display_no"] = 3
    group["shots"][2]["start_seconds"] = 12
    group["shots"][2]["end_seconds"] = 18
    group["shots"].append(dict(group["shots"][2]))
    group["shots"][3]["display_no"] = 4
    group["shots"][3]["start_seconds"] = 18
    group["shots"][3]["end_seconds"] = 24
    contract = storyboard_service._validate_provider_payload(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names=reference_names,
        subject_reference_types=reference_types,
    )
    group_b = contract.scenes[0].shot_groups[0]
    assert group_b.duration_seconds == 15
    shots = group_b.shots
    assert shots[0].start_seconds == 0
    assert shots[-1].end_seconds == 15
    assert all(
        shots[i + 1].start_seconds == shots[i].end_seconds
        for i in range(len(shots) - 1)
    )
    assert all(shot.end_seconds > shot.start_seconds for shot in shots)





async def test_storyboard_contract_exhaustion_redacts_provider_payload(
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from app.schemas.pipeline_contracts import ProviderContractError
    from app.services import storyboard as storyboard_service

    sensitive_sentinel = "STORYBOARD_PRIVATE_DIALOGUE_92f5c4c7"
    invalid_payload = _valid_storyboard_payload()
    invalid_payload["scenes"][0]["shot_groups"][0]["shots"][0][
        "start_seconds"
    ] = sensitive_sentinel
    call_llm = AsyncMock(return_value=invalid_payload)
    monkeypatch.setattr(storyboard_service, "_call_llm", call_llm)
    caplog.set_level(logging.WARNING, logger=storyboard_service.__name__)

    with pytest.raises(ProviderContractError) as exc_info:
        await storyboard_service._call_llm_with_contract(
            "system",
            "user",
            _subject_catalog(),
        )

    assert call_llm.await_count == 2
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert exc_info.value.diagnostic == {
        "version": 1,
        "category": "schema_validation",
        "schema": "StoryboardContract",
        "validation": {
            "error_count": 1,
            "codes": ["int_type"],
            "paths": ["scenes[].shot_groups[].shots[].start_seconds"],
        },
    }
    assert sensitive_sentinel not in "".join(traceback.format_exception(exc_info.value))
    assert sensitive_sentinel not in caplog.text
    assert [
        record.getMessage()
        for record in caplog.records
        if record.name == storyboard_service.__name__
    ] == [
        "分镜 Provider 响应未通过契约 attempt=1/2 category=schema_validation "
        "error_count=1 codes=['int_type'] "
        "paths=['scenes[].shot_groups[].shots[].start_seconds']",
        "分镜 Provider 响应未通过契约 attempt=2/2 category=schema_validation "
        "error_count=1 codes=['int_type'] "
        "paths=['scenes[].shot_groups[].shots[].start_seconds']",
    ]


def test_storyboard_prompt_and_response_schema_require_integer_continuous_timing() -> None:
    from app.services import storyboard as storyboard_service

    assert "display_no 必须严格为 1, 2, ..., N" in storyboard_service._SYSTEM_PROMPT
    assert "所有时码只能是整数" in storyboard_service._SYSTEM_PROMPT
    assert "不要输出 content、description 或 shot_line" in storyboard_service._SYSTEM_PROMPT
    user_prompt = storyboard_service._build_user_prompt(
        SimpleNamespace(episode_no=1, title="第一集", script_text="剧情"),
        [],
    )
    assert '"schema_version": "storyboard.v2"' in user_prompt
    assert '"display_no": 2' in user_prompt
    assert '"start_seconds": 3' in user_prompt
    assert '"end_seconds": 6' in user_prompt
    response_schema = json.dumps(storyboard_service._RESPONSE_SCHEMA, ensure_ascii=False)
    assert '"start_seconds"' in response_schema
    assert '"end_seconds"' in response_schema
    assert '"type": "integer"' in response_schema
    assert '"shot_size"' in response_schema
    provider_schema = json.dumps(
        storyboard_service._PROVIDER_RESPONSE_SCHEMA,
        ensure_ascii=False,
    )
    for unsupported_key in storyboard_service._UNSUPPORTED_PROVIDER_SCHEMA_KEYS:
        assert f'"{unsupported_key}"' not in provider_schema
    assert '"enum": ["storyboard.v2"]' in provider_schema


def test_storyboard_prompt_numbers_script_blocks_and_requires_explicit_coverage() -> None:
    from app.services import storyboard as storyboard_service

    prompt = storyboard_service._build_user_prompt(
        SimpleNamespace(
            episode_no=1,
            title="第一集",
            script_text="阿明推门进入。\n\n小林抬头回应。",
        ),
        [],
    )

    assert "[B01] 阿明推门进入。" in prompt
    assert "[B02] 小林抬头回应。" in prompt
    assert '"source_block_refs": ["B01"]' in prompt
    assert '"omitted_source_blocks": []' in prompt
    assert "source_block_refs 指明落实的 Bxx" in storyboard_service._SYSTEM_PROMPT


def test_normalize_provider_continuity_moves_quality_fields_out_of_visual() -> None:
    from app.services import storyboard as storyboard_service

    payload = _valid_storyboard_v2_payload()
    shot = payload["scenes"][0]["shot_groups"][0]["shots"][0]
    shot["visual"]["source_block_refs"] = ["B01"]
    shot["visual"]["character_positions"] = [
        {"ref": "C01", "position": "左中", "facing": "面朝右"}
    ]
    references = _subject_references()

    validated = storyboard_service._validate_provider_payload_result(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names={ref: item.name for ref, item in references.items()},
        subject_reference_types={ref: item.subject_type for ref, item in references.items()},
    )

    assert validated.v2_contract is not None
    normalized = validated.v2_contract.scenes[0].shot_groups[0].shots[0]
    assert normalized.source_block_refs == ["B01"]
    assert normalized.character_positions[0].position == "左中"


def test_v2_quality_state_persists_structured_shot_group_and_boundaries() -> None:
    from app.services import storyboard as storyboard_service

    payload = _valid_storyboard_v2_payload()
    first, second = payload["scenes"][0]["shot_groups"][0]["shots"]
    first["source_block_refs"] = ["B01"]
    second["source_block_refs"] = ["B02"]
    first["start_boundary"] = {
        "positions": [{"ref": "C01", "position": "左中", "facing": "面朝右"}],
        "visible_state": ["阿明刚推开门"],
    }
    second["end_boundary"] = {
        "positions": [{"ref": "C02", "position": "右中", "facing": "面朝左"}],
        "visible_state": ["小林抬头"],
    }
    references = _subject_references()
    validated = storyboard_service._validate_provider_payload_result(
        payload,
        subject_catalog=_subject_catalog(),
        subject_reference_names={ref: item.name for ref, item in references.items()},
        subject_reference_types={ref: item.subject_type for ref, item in references.items()},
    )
    assert validated.v2_contract is not None
    report = storyboard_service.evaluate_storyboard_quality(
        validated.v2_contract,
        expected_source_blocks=["B01", "B02"],
    )
    source_group = validated.v2_contract.scenes[0].shot_groups[0]
    quality = storyboard_service.build_shot_group_quality_summary(
        source_group,
        warnings=storyboard_service._warnings_for_group(
            report,
            scene_index=0,
            group_index=0,
        ),
    )
    frames = storyboard_service._build_boundary_reference_frames(
        source_group,
        subject_names_by_ref={ref: item.name for ref, item in references.items()},
    )
    structured = storyboard_service.build_shot_structured_data(
        source_group.shots[0],
        subject_ids_by_ref={"C01": "subject-c1"},
    )

    assert report.coverage.coverage_ratio == 1.0
    assert structured["schema_version"] == "storyboard.shot-structure.v1"
    assert structured["source_block_refs"] == ["B01"]
    assert quality["director_commands"][0]["frame_role"] == "first"
    assert frames["boundary_projections"]["start"]["prompt_fragment"].startswith(
        "【入画状态】"
    )


async def test_invalid_provider_contract_does_not_delete_old_storyboard(monkeypatch) -> None:
    from app.schemas.pipeline_contracts import ProviderContractError
    from app.services import storyboard as storyboard_service

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_no=1,
        title="第一集",
        script_text="阿明进入办公室。",
        episode_version=7,
    )
    subjects = [
        SimpleNamespace(id="subject-bg", name="办公室", type="background", episode_nos=[1], description="", image_entry_id="img-bg"),
        SimpleNamespace(id="subject-c", name="阿明", type="character", episode_nos=[1], description="", image_entry_id="img-c"),
    ]

    class _Scalars:
        def __init__(self, values):
            self._values = values

        def all(self):
            return self._values

    class _Result:
        def __init__(self, scalar=None, scalars=None):
            self._scalar = scalar
            self._scalars = scalars or []

        def scalar_one_or_none(self):
            return self._scalar

        def scalars(self):
            return _Scalars(self._scalars)

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[_Result(episode), _Result(scalars=subjects)]),
        flush=AsyncMock(),
        add=lambda _: None,
    )
    cleanup = AsyncMock()
    call_llm = AsyncMock(return_value={"scenes": []})
    monkeypatch.setattr(storyboard_service, "_call_llm", call_llm)
    monkeypatch.setattr(storyboard_service, "_cleanup_existing_storyboard", cleanup)

    with pytest.raises(ProviderContractError):
        await storyboard_service.generate_storyboard(db, "script-1", "episode-1", expected_episode_version=7)

    assert call_llm.await_count == 2
    cleanup.assert_not_awaited()
    db.flush.assert_not_awaited()


async def test_generate_storyboard_provisions_fallback_scene_when_no_background(monkeypatch) -> None:
    """分集缺少场景主体时应自动补建默认场景主体，避免 background_ref 无合法引用卡死生成。"""
    from app.models.subject import Subject
    from app.schemas.pipeline_contracts import ProviderContractError
    from app.services import storyboard as storyboard_service

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_no=1,
        title="第一集",
        script_text="阿明和林晚对话。",
        episode_version=7,
    )
    # 只有角色，没有场景/背景主体
    subjects = [
        SimpleNamespace(id="c1", name="阿明", type="character", episode_nos=[1], description="", image_entry_id=None, is_active=True),
        SimpleNamespace(id="c2", name="林晚", type="character", episode_nos=[1], description="", image_entry_id=None, is_active=True),
    ]

    class _Scalars:
        def __init__(self, values):
            self._values = values

        def all(self):
            return self._values

    class _Result:
        def __init__(self, scalar=None, scalars=None):
            self._scalar = scalar
            self._scalars = scalars or []

        def scalar_one_or_none(self):
            return self._scalar

        def scalars(self):
            return _Scalars(self._scalars)

    added: list = []
    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[_Result(episode), _Result(scalars=subjects)]),
        flush=AsyncMock(),
        add=lambda obj: added.append(obj),
    )
    monkeypatch.setattr(storyboard_service, "_call_llm", AsyncMock(return_value={"scenes": []}))
    monkeypatch.setattr(storyboard_service, "_cleanup_existing_storyboard", AsyncMock())

    with pytest.raises(ProviderContractError):
        await storyboard_service.generate_storyboard(
            db, "script-1", "episode-1", expected_episode_version=7
        )

    fallback = next(
        (s for s in added if isinstance(s, Subject) and s.type == "scene"),
        None,
    )
    assert fallback is not None
    assert fallback.name == "默认场景-第1集"
    assert fallback.episode_nos == [1]


async def test_materials_are_typed_and_sound_is_audio_not_visual_material() -> None:
    from app.services.storyboard import _classify_materials, _extract_audio_elements

    subjects = [
        SimpleNamespace(id="bg", name="办公室", type="background", image_entry_id="img-bg"),
        SimpleNamespace(id="c1", name="阿明", type="character", image_entry_id="img-c1"),
        SimpleNamespace(id="p1", name="钥匙", type="prop", image_entry_id="img-p1"),
        SimpleNamespace(id="s1", name="门响", type="sound", image_entry_id="img-s1"),
    ]
    materials = _classify_materials(["办公室", "阿明", "钥匙", "门响"], subjects)

    assert [item["name"] for item in materials["background"]] == ["办公室"]
    assert [item["name"] for item in materials["character"]] == ["阿明"]
    assert [item["name"] for item in materials["prop"]] == ["钥匙"]
    assert materials["other"] == []
    assert "门响" not in {item["name"] for group in materials.values() for item in group}

    class _Scalars:
        def all(self):
            return [
                SimpleNamespace(id="audio-c1", subject_id="c1"),
                SimpleNamespace(id="audio-s1", subject_id="s1"),
            ]

    class _Result:
        def scalars(self):
            return _Scalars()

    db = SimpleNamespace(execute=AsyncMock(return_value=_Result()))

    audio = await _extract_audio_elements(
        db,
        [
            {"dialogue_character": "阿明", "sounds": ["门响"]},
            {"dialogue_character": "", "sounds": ["门响"]},
        ],
        subjects,
    )
    assert audio == [
        {"subject_id": "c1", "name": "阿明", "asset_entry_id": "audio-c1", "type": "character"},
        {"subject_id": "s1", "name": "门响", "asset_entry_id": "audio-s1", "type": "sound"},
    ]


def test_subject_reference_catalog_excludes_archived_duplicate_and_binds_same_name_by_ref() -> None:
    from app.services import storyboard as storyboard_service

    subjects = [
        SimpleNamespace(
            id="character-current",
            name="同名",
            type="character",
            episode_nos=[1],
            description="当前集角色描述",
            image_entry_id="image-character",
            is_active=True,
        ),
        SimpleNamespace(
            id="character-archived",
            name="同名",
            type="character",
            episode_nos=[99],
            description="PRIVATE_ARCHIVED_SUBJECT_DESCRIPTION",
            image_entry_id="private-image",
            is_active=False,
        ),
        SimpleNamespace(
            id="prop-current",
            name="同名",
            type="prop",
            episode_nos=[1],
            description="当前集道具描述",
            image_entry_id="image-prop",
            is_active=True,
        ),
    ]
    references = storyboard_service._build_subject_reference_catalog(subjects, 1)
    prompt_block = storyboard_service._build_subject_list_block(
        subjects,
        1,
        references,
    )

    assert "PRIVATE_ARCHIVED_SUBJECT_DESCRIPTION" not in prompt_block
    assert "character-archived" not in prompt_block
    assert "private-image" not in prompt_block
    assert "当前集角色描述" in prompt_block
    assert "当前集道具描述" in prompt_block
    character_ref = next(
        ref for ref, item in references.items() if item.subject_type == "character"
    )
    prop_ref = next(ref for ref, item in references.items() if item.subject_type == "prop")
    materials = storyboard_service._classify_reference_materials(
        [character_ref, prop_ref],
        references,
    )
    assert materials["character"] == [
        {
            "ref": character_ref,
            "subject_id": "character-current",
            "asset_entry_id": "image-character",
            "name": "同名",
        }
    ]
    assert materials["prop"] == [
        {
            "ref": prop_ref,
            "subject_id": "prop-current",
            "asset_entry_id": "image-prop",
            "name": "同名",
        }
    ]


async def test_storyboard_non_object_provider_response_is_contract_error(monkeypatch) -> None:
    from app.schemas.pipeline_contracts import ProviderContractError
    from app.services import storyboard as storyboard_service

    monkeypatch.setattr(storyboard_service, "_call_llm", AsyncMock(return_value=["private"]))

    with pytest.raises(ProviderContractError) as exc_info:
        await storyboard_service._call_llm_with_contract(
            "system",
            "user",
            _subject_catalog(),
            subject_references=_subject_references(),
        )

    assert exc_info.value.diagnostic == {
        "version": 1,
        "category": "non_object_json",
        "schema": "StoryboardV2Contract",
        "response": {
            "kind": "list",
            "length_bucket": "1_128",
            "top_level_kind": "list",
        },
    }


async def test_audio_elements_use_approved_audio_asset_not_subject_image(monkeypatch) -> None:
    from app.services import storyboard as storyboard_service

    subjects = [
        SimpleNamespace(id="c1", name="阿明", type="character", image_entry_id="img-c1"),
        SimpleNamespace(id="s1", name="门响", type="sound", image_entry_id="img-sound-wrong"),
    ]
    audio_entries = [
        SimpleNamespace(
            id="audio-c1", subject_id="c1", kind="audio", is_approved=True,
            status="approved", superseded=False,
        ),
        SimpleNamespace(
            id="audio-s1", subject_id="s1", kind="audio", is_approved=True,
            status="approved", superseded=False,
        ),
    ]

    class _Scalars:
        def __init__(self, values):
            self._values = values

        def all(self):
            return self._values

    class _Result:
        def scalars(self):
            return _Scalars(audio_entries)

    db = SimpleNamespace(execute=AsyncMock(return_value=_Result()))

    audio = await storyboard_service._extract_audio_elements(
        db,
        [
            {"dialogue_character": "阿明", "sounds": ["门响"]},
            {"dialogue_character": "", "sounds": ["门响"]},
        ],
        subjects,
    )

    assert audio == [
        {"subject_id": "c1", "name": "阿明", "asset_entry_id": "audio-c1", "type": "character"},
        {"subject_id": "s1", "name": "门响", "asset_entry_id": "audio-s1", "type": "sound"},
    ]
    assert "img-sound-wrong" not in {item.get("asset_entry_id") for item in audio}


def test_workbench_materials_normalize_legacy_strings_and_split_audio() -> None:
    from app.crud.shot import _normalise_audio_elements, _to_materials

    materials = _to_materials(
        {
            "character": ["阿明", {"subject_id": "c2", "name": "小林", "asset_entry_id": "img-c2"}],
            "sound": ["门响"],
        }
    )
    assert [item.name for item in materials.character] == ["阿明", "小林"]
    assert materials.character[0].subject_id is None
    assert materials.background == []
    assert not hasattr(materials, "sound")

    assert _normalise_audio_elements({"characters": ["阿明"], "sounds": [{"name": "门响"}]}) == [
        {"name": "阿明", "type": "character"},
        {"name": "门响", "type": "sound"},
    ]


def test_group_sort_key_orders_g2_before_g10() -> None:
    from app.crud.shot import _group_sort_key

    groups = [SimpleNamespace(group_no="G10", group_index=10), SimpleNamespace(group_no="G2", group_index=2)]
    assert [g.group_no for g in sorted(groups, key=_group_sort_key)] == ["G2", "G10"]


async def test_validate_script_episode_distinguishes_cross_script_403_from_missing_404(monkeypatch) -> None:
    from app.api.v1 import shots

    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    cross_episode = SimpleNamespace(episode_id="episode-1", script_id="other-script", episode_version=1)
    user = SimpleNamespace(user_id="user-1", enabled=True)

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    db = SimpleNamespace(execute=AsyncMock(side_effect=[_Result(script), _Result(None), _Result(cross_episode)]))
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())

    with pytest.raises(ForbiddenError):
        await shots._validate_script_episode(db, user, "script-1", "episode-1")

    missing_db = SimpleNamespace(execute=AsyncMock(side_effect=[_Result(script), _Result(None), _Result(None)]))
    with pytest.raises(HTTPException) as exc_info:
        await shots._validate_script_episode(missing_db, user, "script-1", "missing")
    assert exc_info.value.status_code == 404


async def test_create_storyboard_job_reserves_and_enqueues_in_one_transaction(monkeypatch) -> None:
    from app.api.v1 import shots

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=3,
        client_save_seq=4,
        script_text="剧情",
    )
    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    account = SimpleNamespace(id="account-1", status="active")
    user = SimpleNamespace(user_id="user-1", enabled=True)

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

        def scalar_one(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(script),
                _Result(episode),
                _Result(None),
                _Result(None),
                _Result(account),
                _Result(0),
            ]
        ),
        add=lambda _: None,
        flush=AsyncMock(),
        commit=AsyncMock(),
    )
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())
    monkeypatch.setattr(
        shots,
        "create_job",
        AsyncMock(
            return_value=SimpleNamespace(
                job_id="job-1",
                attempt=1,
                status="pending",
                reservation_id=None,
                detail=None,
                task_id=None,
            )
        ),
    )
    monkeypatch.setattr(shots, "reserve", AsyncMock(return_value="reservation-1"))
    monkeypatch.setattr(shots, "enqueue_task", AsyncMock(return_value=SimpleNamespace(id="dispatch-1")))

    response = await shots.generate_storyboard(
        "script-1",
        "episode-1",
        3,
        "idem-1",
        db=db,
        user=user,
    )

    shots.reserve.assert_awaited_once()
    assert shots.reserve.await_args.kwargs["commit"] is False
    shots.enqueue_task.assert_awaited_once()
    db.commit.assert_awaited_once()
    assert response.task_id == "dispatch-1"
    assert response.job_id


async def test_generate_storyboard_without_credit_account_rejects_before_enqueue(monkeypatch) -> None:
    from app.api.v1 import shots

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=3,
        client_save_seq=4,
        script_text="剧情",
    )
    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    user = SimpleNamespace(user_id="user-1", enabled=True)

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(script),
                _Result(episode),
                _Result(None),
                _Result(None),
                _Result(None),
            ]
        ),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())
    monkeypatch.setattr(shots, "reserve", AsyncMock())
    monkeypatch.setattr(shots, "enqueue_task", AsyncMock())

    with pytest.raises(InsufficientCreditsError):
        await shots.generate_storyboard(
            "script-1",
            "episode-1",
            3,
            "idem-1",
            db=db,
            user=user,
        )

    shots.reserve.assert_not_awaited()
    shots.enqueue_task.assert_not_awaited()


async def test_generate_storyboard_requires_nonempty_idempotency_key_before_credit_lookup(monkeypatch) -> None:
    from app.api.v1 import shots

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=3,
        client_save_seq=4,
        script_text="剧情",
    )
    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    user = SimpleNamespace(user_id="user-1", enabled=True)

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[_Result(script), _Result(episode), _Result(None)]),
        rollback=AsyncMock(),
    )
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())
    monkeypatch.setattr(shots, "reserve", AsyncMock())
    monkeypatch.setattr(shots, "enqueue_task", AsyncMock())

    with pytest.raises(ConflictError, match="Idempotency-Key"):
        await shots.generate_storyboard(
            "script-1",
            "episode-1",
            3,
            "   ",
            db=db,
            user=user,
        )

    shots.reserve.assert_not_awaited()
    shots.enqueue_task.assert_not_awaited()


async def test_storyboard_idempotency_unique_conflict_recovers_winner(monkeypatch) -> None:
    from app.api.v1 import shots

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=3,
        client_save_seq=9,
        script_text="剧情",
    )
    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    account = SimpleNamespace(id="account-1", status="active")
    user = SimpleNamespace(user_id="user-1", enabled=True)
    winner = SimpleNamespace(
        job_id="job-winner",
        task_id="dispatch-winner",
        episode_id="episode-1",
        script_id="script-1",
        status="pending",
        request_hash=shots.storyboard_request_hash(
            script_id="script-1",
            episode_id="episode-1",
            episode_version=3,
            client_save_seq=9,
        ),
    )

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

        def scalar_one(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(script),
                _Result(episode),
                _Result(None),
                _Result(None),
                _Result(account),
                _Result(0),
                _Result(winner),
            ]
        ),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())
    monkeypatch.setattr(
        shots,
        "create_job",
        AsyncMock(side_effect=IntegrityError("insert", {}, Exception())),
    )
    monkeypatch.setattr(shots, "reserve", AsyncMock())
    monkeypatch.setattr(shots, "enqueue_task", AsyncMock())

    response = await shots.generate_storyboard(
        "script-1",
        "episode-1",
        3,
        "idem-race",
        db=db,
        user=user,
    )

    assert response.job_id == "job-winner"
    assert response.task_id == "dispatch-winner"
    shots.reserve.assert_not_awaited()
    shots.enqueue_task.assert_not_awaited()


def test_storyboard_generate_route_requires_standard_idempotency_header_only() -> None:
    from fastapi.routing import APIRoute

    from app.api.v1.shots import router

    route = next(
        route
        for route in router.routes
        if isinstance(route, APIRoute)
        and route.path == "/scripts/{script_id}/episodes/{episode_id}/generate-storyboard"
    )
    header_params = {param.alias for param in route.dependant.header_params}
    assert "Idempotency-Key" in header_params
    assert "X-Idempotency-Key" not in header_params


async def test_storyboard_job_list_total_counts_all_rows(monkeypatch) -> None:
    from app.api.v1 import shots

    user = SimpleNamespace(user_id="user-1", enabled=True)
    jobs = [
        SimpleNamespace(
            job_id="job-2", task_id=None, episode_id="episode-1", script_id="script-1",
            status="pending", progress=0, progress_stage=None, progress_message=None,
            error_message=None, detail=None,
        )
    ]
    monkeypatch.setattr(shots, "_validate_script_episode", AsyncMock())
    monkeypatch.setattr(shots, "list_jobs_for_episode", AsyncMock(return_value=jobs))
    monkeypatch.setattr(shots, "count_jobs_for_episode", AsyncMock(return_value=3))

    out = await shots.list_storyboard_jobs(
        "script-1",
        "episode-1",
        limit=1,
        offset=1,
        db=SimpleNamespace(),
        user=user,
    )

    assert len(out.items) == 1
    assert out.total == 3


async def test_storyboard_job_list_returns_successful_empty_state(monkeypatch) -> None:
    """尚未创建任务是正常列表状态，供页面无异常地发现最新任务。"""
    from app.api.v1 import shots

    user = SimpleNamespace(user_id="user-1", enabled=True)
    monkeypatch.setattr(shots, "_validate_script_episode", AsyncMock())
    monkeypatch.setattr(shots, "list_jobs_for_episode", AsyncMock(return_value=[]))
    monkeypatch.setattr(shots, "count_jobs_for_episode", AsyncMock(return_value=0))

    out = await shots.list_storyboard_jobs(
        "script-1",
        "episode-1",
        limit=1,
        offset=0,
        db=SimpleNamespace(),
        user=user,
    )

    assert out.items == []
    assert out.total == 0


async def test_storyboard_claim_lease_cas_and_cancel() -> None:
    from app.crud.storyboard import cancel_job, claim_job, complete_job

    now = datetime(2026, 7, 17, tzinfo=UTC)
    job = SimpleNamespace(
        job_id="job-1",
        script_id="script-1",
        episode_id="episode-1",
        episode_version=1,
        input_version=1,
        attempt=1,
        status="pending",
        progress=0,
        lease_owner=None,
        lease_token=None,
        lease_until=None,
        heartbeat_at=None,
        started_at=None,
        canceled_at=None,
        finished_at=None,
        detail=None,
    )

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(return_value=_Result(job)),
        get=AsyncMock(return_value=job),
        commit=AsyncMock(),
        flush=AsyncMock(),
    )

    token_1 = await claim_job(db, "job-1", owner="worker-a", now=now, lease_seconds=10)
    assert token_1 and job.status == "running"
    job.lease_until = now + timedelta(seconds=1)
    token_2 = await claim_job(db, "job-1", owner="worker-b", now=now + timedelta(seconds=2), lease_seconds=10)
    assert token_2 and token_2 != token_1
    assert not await complete_job(db, "job-1", token_1, status="succeeded", detail={})
    assert await complete_job(db, "job-1", token_2, status="succeeded", detail={})
    assert job.progress == 100
    assert job.progress_stage == "completed"
    assert job.progress_message == "分镜生成完成"

    job.status = "running"
    job.lease_token = "cancel-token"
    assert await cancel_job(db, "job-1", reason="user request")
    assert job.status == "canceled"
    assert not await complete_job(db, "job-1", "cancel-token", status="succeeded", detail={})


async def test_storyboard_failed_job_preserves_reservation_and_adds_safe_diagnostic() -> None:
    from app.core.job_state import PipelineFailure
    from app.crud.storyboard import fail_job_if_current_token
    from app.schemas.pipeline_contracts import ProviderContractError

    diagnostic = {
        "version": 1,
        "category": "schema_validation",
        "schema": "StoryboardV2Contract",
        "validation": {
            "error_count": 1,
            "codes": ["literal_error"],
            "paths": ["scenes[].shot_groups[].shots[].visual.shot_size"],
        },
    }
    failure = PipelineFailure.from_exception(
        ProviderContractError("private provider payload", diagnostic=diagnostic)
    )
    assert failure.diagnostic == diagnostic

    job = SimpleNamespace(
        status="running",
        lease_token="token-1",
        detail={"reservation_id": "reservation-1"},
        retryable=False,
        lease_owner="worker-1",
        lease_until=datetime.now(UTC) + timedelta(minutes=1),
        finished_at=None,
    )
    db = SimpleNamespace(get=AsyncMock(return_value=job), flush=AsyncMock())

    assert await fail_job_if_current_token(
        db,
        "job-1",
        "token-1",
        error_message=failure.error_message,
        error_type=failure.error_type,
        retryable=failure.retryable,
        diagnostic=failure.diagnostic,
    )
    assert job.status == "failed"
    assert job.detail == {
        "reservation_id": "reservation-1",
        "diagnostic": diagnostic,
    }


def test_storyboard_failure_diagnostic_redacts_unknown_paths_and_response_kinds() -> None:
    from app.core.job_state import PipelineFailure
    from app.schemas.pipeline_contracts import ProviderContractError

    failure = PipelineFailure.from_exception(
        ProviderContractError(
            "private",
            diagnostic={
                "version": 1,
                "category": "schema_validation",
                "schema": "StoryboardV2Contract",
                "validation": {
                    "error_count": 1,
                    "codes": ["literal_error"],
                    "paths": ["scenes[].PRIVATE_FIELD_WITH_SECRET"],
                },
            },
        )
    )
    assert failure.diagnostic is not None
    assert failure.diagnostic["validation"]["paths"] == [
        "scenes[].<unknown_field>"
    ]

    unsafe_kind = PipelineFailure.from_exception(
        ProviderContractError(
            "private",
            diagnostic={
                "version": 1,
                "category": "invalid_json",
                "schema": "StoryboardV2Contract",
                "response": {
                    "kind": "PRIVATE_RESPONSE_KIND",
                    "length_bucket": "1_128",
                    "top_level_kind": "invalid_json",
                },
            },
        )
    )
    assert unsafe_kind.diagnostic is None

    unsafe_semantic_code = PipelineFailure.from_exception(
        ProviderContractError(
            "private",
            diagnostic={
                "version": 1,
                "category": "semantic_validation",
                "schema": "StoryboardV2Contract",
                "validation": {
                    "error_count": 1,
                    "codes": ["PRIVATE_PROVIDER_TEXT"],
                    "paths": ["scenes[].scene_no"],
                },
            },
        )
    )
    assert unsafe_semantic_code.diagnostic is None


async def test_worker_checks_input_version_before_provider_and_before_commit(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    stale_job = SimpleNamespace(
        job_id="job-1",
        script_id="script-1",
        episode_id="episode-1",
        input_version=1,
        input_client_save_seq=1,
        episode_version=1,
        status="running",
        lease_token="token-1",
        canceled_at=None,
    )
    episode = SimpleNamespace(episode_version=2, client_save_seq=1)
    db = SimpleNamespace(
        get=AsyncMock(return_value=stale_job),
        execute=AsyncMock(),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )
    monkeypatch.setattr(storyboard_worker, "_load_episode", AsyncMock(return_value=episode))
    generate = AsyncMock()
    monkeypatch.setattr(storyboard_worker.storyboard_service, "generate_storyboard", generate)

    with pytest.raises(RuntimeError, match="input changed"):
        await storyboard_worker._run_claimed_storyboard_job(db, stale_job, "token-1")
    generate.assert_not_awaited()


async def test_worker_rolls_back_when_terminal_cas_fails(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(
        job_id="job-1",
        script_id="script-1",
        episode_id="episode-1",
        input_version=7,
        input_client_save_seq=4,
        episode_version=7,
        status="running",
        lease_token="token-current",
        canceled_at=None,
        reservation_id="reservation-1",
        detail={},
        triggered_by_user_id="user-1",
    )
    episode = SimpleNamespace(episode_version=7, client_save_seq=4)
    db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    monkeypatch.setattr(storyboard_worker, "_load_episode", AsyncMock(return_value=episode))
    monkeypatch.setattr(
        storyboard_worker.storyboard_service,
        "generate_storyboard",
        AsyncMock(return_value={"scenes": 1, "shot_groups": 1, "shots": 2}),
    )
    events: list[str] = []

    async def lock_current(*args, **kwargs):
        events.append("job_lock")
        return job

    async def settle(*args, **kwargs):
        events.append("settle")

    async def terminal_cas(*args, **kwargs):
        events.append("terminal_cas")
        return False

    monkeypatch.setattr(storyboard_worker, "_lock_current_storyboard_job", lock_current)
    monkeypatch.setattr(storyboard_worker, "settle", settle)
    monkeypatch.setattr(storyboard_worker, "heartbeat_job", AsyncMock(return_value=True))
    monkeypatch.setattr(storyboard_worker, "complete_job", terminal_cas)

    with pytest.raises(RuntimeError, match="lease"):
        await storyboard_worker._run_claimed_storyboard_job(db, job, "token-current")

    db.rollback.assert_awaited()
    db.commit.assert_not_awaited()
    assert events == ["job_lock", "settle", "terminal_cas"]


async def test_worker_delegates_generation_to_independent_heartbeat(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(
        job_id="job-1",
        script_id="script-1",
        episode_id="episode-1",
        input_version=7,
        input_client_save_seq=4,
        status="running",
        lease_token="token-current",
        canceled_at=None,
        reservation_id=None,
        detail={},
        triggered_by_user_id="user-1",
    )
    episode = SimpleNamespace(episode_version=7, client_save_seq=4)
    db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    monkeypatch.setattr(storyboard_worker, "_load_episode", AsyncMock(return_value=episode))
    generate_with_heartbeat = AsyncMock(return_value={"scenes": 1, "shot_groups": 1, "shots": 2})
    monkeypatch.setattr(
        storyboard_worker,
        "_generate_storyboard_with_heartbeat",
        generate_with_heartbeat,
    )
    monkeypatch.setattr(
        storyboard_worker,
        "_lock_current_storyboard_job",
        AsyncMock(return_value=job),
    )
    monkeypatch.setattr(storyboard_worker, "complete_job", AsyncMock(return_value=True))

    await storyboard_worker._run_claimed_storyboard_job(db, job, "token-current")

    generate_with_heartbeat.assert_awaited_once_with(db, job, "token-current")


async def test_worker_persists_quality_report_in_terminal_detail(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    report = {
        "version": 1,
        "status": "warning",
        "warnings": [{"code": "coverage_missing_block"}],
    }
    job = SimpleNamespace(
        job_id="job-quality",
        script_id="script-1",
        episode_id="episode-1",
        input_version=7,
        input_client_save_seq=4,
        status="running",
        lease_token="token-current",
        canceled_at=None,
        reservation_id=None,
        detail={},
        triggered_by_user_id="user-1",
    )
    episode = SimpleNamespace(episode_version=7, client_save_seq=4)
    db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    complete = AsyncMock(return_value=True)
    monkeypatch.setattr(storyboard_worker, "_load_episode", AsyncMock(return_value=episode))
    monkeypatch.setattr(
        storyboard_worker,
        "_generate_storyboard_with_heartbeat",
        AsyncMock(
            return_value={
                "scenes": 1,
                "shot_groups": 1,
                "shots": 2,
                "quality_report": report,
            }
        ),
    )
    monkeypatch.setattr(
        storyboard_worker,
        "_lock_current_storyboard_job",
        AsyncMock(return_value=job),
    )
    monkeypatch.setattr(storyboard_worker, "complete_job", complete)

    await storyboard_worker._run_claimed_storyboard_job(db, job, "token-current")

    assert complete.await_args.kwargs["detail"]["quality_report"] == report


async def test_worker_persists_input_provenance_before_terminal_cas(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(
        job_id="job-provenance",
        script_id="script-1",
        episode_id="episode-1",
        input_version=7,
        input_client_save_seq=4,
        input_provenance=None,
        status="running",
        lease_token="token-current",
        canceled_at=None,
        reservation_id=None,
        detail={},
        triggered_by_user_id="user-1",
    )
    episode = SimpleNamespace(episode_version=7, client_save_seq=4)
    db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    provenance = {"schema": "storyboard.provenance.v1"}
    observed: list[object] = []

    async def complete(*args, **kwargs):
        observed.append(job.input_provenance)
        return True

    monkeypatch.setattr(storyboard_worker, "_load_episode", AsyncMock(return_value=episode))
    monkeypatch.setattr(
        storyboard_worker,
        "_generate_storyboard_with_heartbeat",
        AsyncMock(
            return_value={
                "scenes": 1,
                "shot_groups": 1,
                "shots": 2,
                "input_provenance": provenance,
            }
        ),
    )
    monkeypatch.setattr(
        storyboard_worker,
        "_lock_current_storyboard_job",
        AsyncMock(return_value=job),
    )
    monkeypatch.setattr(storyboard_worker, "complete_job", complete)

    await storyboard_worker._run_claimed_storyboard_job(db, job, "token-current")

    assert observed == [provenance]
    db.commit.assert_awaited_once()


def test_storyboard_heartbeat_renews_in_independent_short_transactions(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    stop = asyncio.Event()
    sessions: list[SimpleNamespace] = []
    renewals = 0

    class SessionContext:
        async def __aenter__(self):
            db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
            sessions.append(db)
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def immediate_timeout(awaitable, *, timeout):
        awaitable.close()
        raise TimeoutError

    async def heartbeat(db, job_id, token, *, lease_seconds):
        nonlocal renewals
        assert job_id == "job-1"
        assert token == "token-1"
        assert lease_seconds == storyboard_worker._STORYBOARD_LEASE_SECONDS
        renewals += 1
        if renewals == 3:
            stop.set()
        return True

    monkeypatch.setattr(storyboard_worker.asyncio, "wait_for", immediate_timeout)
    monkeypatch.setattr(storyboard_worker, "async_session_factory", SessionContext)
    monkeypatch.setattr(storyboard_worker, "heartbeat_job", heartbeat)

    asyncio.run(storyboard_worker._heartbeat_storyboard_job_loop("job-1", "token-1", stop))

    assert renewals == 3
    assert [session.commit.await_count for session in sessions] == [1, 1, 1]


async def test_soft_timeout_cancellation_marks_current_job_failed(monkeypatch) -> None:
    from billiard.exceptions import SoftTimeLimitExceeded

    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(job_id="job-1")
    claim_db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
    run_db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
    sessions = iter((claim_db, run_db))

    class SessionContext:
        async def __aenter__(self):
            return next(sessions)

        async def __aexit__(self, exc_type, exc, tb):
            return False

    holder = storyboard_worker._StoryboardTaskLeaseHolder()
    holder.record_interrupt(SoftTimeLimitExceeded())
    cleanup = AsyncMock(return_value=True)

    async def canceled(*args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(storyboard_worker, "async_session_factory", SessionContext)
    monkeypatch.setattr(storyboard_worker, "claim_job", AsyncMock(return_value="token-1"))
    monkeypatch.setattr(storyboard_worker, "get_job", AsyncMock(return_value=job))
    monkeypatch.setattr(storyboard_worker, "update_job", AsyncMock())
    monkeypatch.setattr(storyboard_worker, "_run_claimed_storyboard_job", canceled)
    monkeypatch.setattr(storyboard_worker, "_mark_failed_after_rollback", cleanup)

    with pytest.raises(asyncio.CancelledError):
        await storyboard_worker._run_storyboard_job_by_id("job-1", holder)

    cleanup.assert_awaited_once()
    call = cleanup.await_args
    assert call.args[0] == "job-1"
    assert isinstance(call.args[1], TimeoutError)
    assert call.args[2] == "token-1"
    run_db.rollback.assert_awaited_once()


async def test_soft_timeout_during_claim_setup_marks_claimed_job_failed(monkeypatch) -> None:
    """超时落在领取提交窗口时，也必须清理已写入的当前租约。"""
    from billiard.exceptions import SoftTimeLimitExceeded

    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(job_id="job-claim-window")
    claim_db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())

    class SessionContext:
        async def __aenter__(self):
            return claim_db

        async def __aexit__(self, exc_type, exc, tb):
            return False

    holder = storyboard_worker._StoryboardTaskLeaseHolder()
    holder.record_interrupt(SoftTimeLimitExceeded())
    cleanup = AsyncMock(return_value=True)

    async def canceled_update(*args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(storyboard_worker, "async_session_factory", SessionContext)
    monkeypatch.setattr(storyboard_worker, "claim_job", AsyncMock(return_value="token-claim"))
    monkeypatch.setattr(storyboard_worker, "get_job", AsyncMock(return_value=job))
    monkeypatch.setattr(storyboard_worker, "update_job", canceled_update)
    monkeypatch.setattr(storyboard_worker, "_mark_failed_after_rollback", cleanup)

    with pytest.raises(asyncio.CancelledError):
        await storyboard_worker._run_storyboard_job_by_id("job-claim-window", holder)

    claim_db.rollback.assert_awaited_once()
    cleanup.assert_awaited_once()
    call = cleanup.await_args
    assert call.args[0] == "job-claim-window"
    assert isinstance(call.args[1], TimeoutError)
    assert call.args[2] == "token-claim"


def test_storyboard_task_inherits_global_timeout_window() -> None:
    from app.core.job_state import (
        TASK_HARD_TIME_LIMIT_SECONDS,
        TASK_SOFT_TIME_LIMIT_SECONDS,
    )
    from app.workers import storyboard as storyboard_worker

    task = storyboard_worker.generate_storyboard_task
    effective_soft_limit = task.soft_time_limit or storyboard_worker.celery_app.conf.task_soft_time_limit
    effective_hard_limit = task.time_limit or storyboard_worker.celery_app.conf.task_time_limit

    assert (effective_soft_limit, effective_hard_limit) == (
        TASK_SOFT_TIME_LIMIT_SECONDS,
        TASK_HARD_TIME_LIMIT_SECONDS,
    )


async def test_dispatch_reconciler_ignores_running_job_with_live_lease() -> None:
    """恢复器只能接管租约已到期的运行中任务，不能仅凭心跳时间重派。"""
    from sqlalchemy.dialects import postgresql

    from app.workers import storyboard as storyboard_worker

    class EmptyResult:
        def scalars(self):
            return self

        def all(self):
            return []

    db = SimpleNamespace(execute=AsyncMock(return_value=EmptyResult()))
    await storyboard_worker.reconcile_storyboard_dispatches(
        db,
        now=datetime(2026, 8, 11, tzinfo=UTC),
    )

    statement = db.execute.await_args.args[0]
    where_sql = " ".join(str(statement.whereclause.compile(dialect=postgresql.dialect())).split())
    assert "storyboard_jobs.heartbeat_at" not in where_sql
    assert "storyboard_jobs.lease_until" in where_sql
    assert "storyboard_jobs.status = %(status_2)s" in where_sql


async def test_dispatch_reconciler_optionally_audits_succeeded_provenance(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(job_id="job-stale")

    class Result:
        def __init__(self, values):
            self.values = values

        def scalars(self):
            return self

        def all(self):
            return self.values

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[Result([]), Result([job])]),
    )
    report = SimpleNamespace(status="stale", mismatches=("episode_script_sha256",))
    validate = AsyncMock(return_value=report)
    monkeypatch.setattr(
        storyboard_worker.settings,
        "storyboard_provenance_audit_enabled",
        True,
    )
    monkeypatch.setattr(
        storyboard_worker.settings,
        "storyboard_provenance_audit_batch_size",
        7,
    )
    monkeypatch.setattr(storyboard_worker, "validate_storyboard_provenance", validate)

    recovered = await storyboard_worker.reconcile_storyboard_dispatches(db)

    assert recovered == 0
    validate.assert_awaited_once_with(db, "job-stale")
    audit_statement = db.execute.await_args_list[1].args[0]
    assert "storyboard_jobs.status" in str(audit_statement)


async def test_worker_failure_path_does_not_release_or_mark_failed_for_stale_token(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    stale_job = SimpleNamespace(
        job_id="job-1",
        reservation_id="reservation-1",
        lease_token="new-token",
        status="running",
        canceled_at=None,
    )
    sessions = [
        SimpleNamespace(get=AsyncMock(return_value=stale_job), rollback=AsyncMock()),
    ]
    monkeypatch.setattr(storyboard_worker, "release", AsyncMock())

    released = await storyboard_worker._release_reserved_storyboard(
        sessions[0],
        stale_job,
        "分镜生成失败",
        lease_token="old-token",
    )

    assert released is False
    storyboard_worker.release.assert_not_awaited()
    sessions[0].get.assert_awaited_once()


async def test_storyboard_failure_is_atomic_and_stale_worker_cannot_release(
    monkeypatch,
) -> None:
    from app.workers import storyboard as storyboard_worker

    stale_job = SimpleNamespace(
        job_id="job-1",
        status="running",
        lease_token="new-token",
        canceled_at=None,
        reservation_id="reservation-1",
        detail={},
        triggered_by_user_id="user-1",
    )
    db = SimpleNamespace(
        get=AsyncMock(return_value=stale_job),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )

    class SessionContext:
        async def __aenter__(self):
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return None

    release_credit = AsyncMock()
    fail_job = AsyncMock(return_value=True)
    monkeypatch.setattr(storyboard_worker, "async_session_factory", SessionContext)
    monkeypatch.setattr(storyboard_worker, "release", release_credit)
    monkeypatch.setattr(storyboard_worker, "fail_job_if_current_token", fail_job)

    completed = await storyboard_worker._mark_failed_after_rollback(
        "job-1", RuntimeError("provider failed"), "old-token"
    )

    assert completed is False
    release_credit.assert_not_awaited()
    fail_job.assert_not_awaited()
    db.rollback.assert_awaited_once()
    db.commit.assert_not_awaited()


async def test_storyboard_failure_locks_job_before_release_and_checks_terminal_cas(
    monkeypatch,
) -> None:
    from app.workers import storyboard as storyboard_worker

    job = SimpleNamespace(
        job_id="job-1",
        status="running",
        lease_token="token-current",
        canceled_at=None,
        reservation_id="reservation-1",
        detail={},
        triggered_by_user_id="user-1",
        financial_cleanup_status=None,
        financial_cleanup_error=None,
    )
    db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())

    class SessionContext:
        async def __aenter__(self):
            return db

        async def __aexit__(self, exc_type, exc, tb):
            return None

    events: list[str] = []

    async def lock_current(*args, **kwargs):
        events.append("job_lock")
        return job

    async def release_credit(*args, **kwargs):
        events.append("release")
        assert args[0] is db

    async def fail_job(*args, **kwargs):
        events.append("terminal_cas")
        assert args[0] is db
        return False

    monkeypatch.setattr(storyboard_worker, "async_session_factory", SessionContext)
    monkeypatch.setattr(storyboard_worker, "_lock_current_storyboard_job", lock_current)
    monkeypatch.setattr(storyboard_worker, "release", release_credit)
    monkeypatch.setattr(storyboard_worker, "fail_job_if_current_token", fail_job)

    completed = await storyboard_worker._mark_failed_after_rollback(
        "job-1", RuntimeError("provider failed"), "token-current"
    )

    assert completed is False
    assert events == ["job_lock", "release", "terminal_cas"]
    db.rollback.assert_awaited_once()
    db.commit.assert_not_awaited()


def test_celery_storyboard_entry_uses_job_id_safe_path(monkeypatch) -> None:
    from app.workers import storyboard as storyboard_worker

    safe_runner = AsyncMock(return_value={"ok": True})
    unsafe_runner = AsyncMock()
    monkeypatch.setattr(storyboard_worker, "_run_storyboard_job_by_id", safe_runner, raising=False)
    monkeypatch.setattr(storyboard_worker, "_run_storyboard_generation", unsafe_runner)

    assert storyboard_worker._generate_storyboard_task("script-1", "episode-1", "user-1", "job-1") == {"ok": True}
    safe_runner.assert_awaited_once_with("job-1")
    unsafe_runner.assert_not_awaited()


def test_storyboard_outbox_canvas_signature_helper() -> None:
    from app.workers.storyboard import STORYBOARD_OUTBOX_TASK_TYPE, build_storyboard_canvas

    class _Task:
        def s(self, *args):
            return ("signature", args)

    payload = {
        "script_id": "script-1",
        "episode_id": "episode-1",
        "user_id": "user-1",
        "job_id": "job-1",
    }
    assert STORYBOARD_OUTBOX_TASK_TYPE == "storyboard.generate"
    assert build_storyboard_canvas(payload, _Task()) == (
        "signature",
        ("script-1", "episode-1", "user-1", "job-1"),
    )


def test_cover_uses_true_first_group_and_does_not_skip_to_later_groups() -> None:
    from app.crud.shot import _cover_from_ordered_groups

    groups = [
        SimpleNamespace(reference_frames={"first_frame": None}),
        SimpleNamespace(reference_frames={"first_frame": "later.png"}),
    ]
    assert _cover_from_ordered_groups(groups) is None


def test_cover_falls_back_to_first_group_last_frame_after_first_video_finishes() -> None:
    from app.crud.shot import _cover_from_ordered_groups

    groups = [
        SimpleNamespace(reference_frames={"first_frame": None, "last_frame": "first-video.png"}),
        SimpleNamespace(reference_frames={"first_frame": "later.png", "last_frame": "later-last.png"}),
    ]
    assert _cover_from_ordered_groups(groups) == "first-video.png"


def test_docker_compose_declares_worker_and_beat_topology() -> None:
    from pathlib import Path

    compose = Path("docker-compose.yml").read_text()
    assert "celery-worker:" in compose
    assert "celery-beat:" in compose
    assert "celery -A app.workers.celery_app worker -l info" in compose
    assert "celery -A app.workers.celery_app beat -l info" in compose
