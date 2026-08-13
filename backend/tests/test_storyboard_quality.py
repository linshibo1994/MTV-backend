"""分镜结构化质量规则测试；全部使用纯函数，不访问外部服务。"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from app.models.shot import Shot
from app.models.shot_group import ShotGroup, shot_group_content_hash
from app.schemas.storyboard_contracts import (
    ShotBoundary,
    StoryboardV2Contract,
    validate_storyboard_v2_contract,
)
from app.services.boundary_projection import project_boundary
from app.services.prompt_builder import (
    build_group_director_summaries,
    derive_frame_role,
    infer_action_beat,
)
from app.services.storyboard_quality import (
    build_script_blocks,
    build_shot_group_quality_summary,
    build_shot_structured_data,
    canonical_source_block_ref,
    evaluate_storyboard_quality,
)


def _subject_types() -> dict[str, str]:
    return {
        "BG01": "background",
        "C01": "character",
        "C02": "character",
        "P01": "prop",
    }


def _shot(
    *,
    display_no: int,
    start_seconds: int,
    end_seconds: int,
    action: str,
) -> dict:
    return {
        "display_no": display_no,
        "start_seconds": start_seconds,
        "end_seconds": end_seconds,
        "visual": {
            "shot_size": "中景",
            "composition": ["双人"],
            "camera_angle": "平视",
            "camera_movement": "固定",
            "action": action,
            "frame_description": "两名角色隔桌对视",
            "lighting": "室内柔光",
            "emotion": "紧张",
        },
        "narrative_purpose": "推进对峙",
        "character_refs": ["C01", "C02"],
        "prop_refs": ["P01"],
        "sound_refs": [],
        "sound_design": "环境底噪",
        "dialogue": None,
    }


def _legacy_v2_payload() -> dict:
    return {
        "schema_version": "storyboard.v2",
        "scenes": [
            {
                "scene_no": 1,
                "background_ref": "BG01",
                "shot_groups": [
                    {
                        "group_no": "G1",
                        "narrative_purpose": "建立冲突",
                        "duration_seconds": 6,
                        "shots": [
                            _shot(
                                display_no=1,
                                start_seconds=0,
                                end_seconds=3,
                                action="角色推门开始对峙",
                            ),
                            _shot(
                                display_no=2,
                                start_seconds=3,
                                end_seconds=6,
                                action="角色沉默并回望",
                            ),
                        ],
                    }
                ],
            }
        ],
    }


def test_old_storyboard_v2_remains_valid_and_new_fields_have_empty_defaults() -> None:
    contract = validate_storyboard_v2_contract(_legacy_v2_payload(), _subject_types())

    shot = contract.scenes[0].shot_groups[0].shots[0]
    assert shot.source_block_refs == []
    assert shot.coverage == []
    assert shot.visual.action_beat is None
    assert shot.start_boundary is None
    assert shot.end_boundary is None
    assert shot.character_positions == []
    assert shot.audience_visibility == []
    assert contract.omitted_source_blocks == []


def test_coverage_omissions_are_root_level_and_shot_level_omission_is_forbidden() -> None:
    payload = _legacy_v2_payload()
    payload["omitted_source_blocks"] = [{"block_ref": "B02", "reason": "重复定场信息"}]
    payload["scenes"][0]["shot_groups"][0]["shots"][0]["source_block_refs"] = ["B01"]
    contract = validate_storyboard_v2_contract(payload, _subject_types())
    assert contract.omitted_source_blocks[0].block_ref == "B02"

    invalid = deepcopy(payload)
    invalid["scenes"][0]["shot_groups"][0]["shots"][0]["omitted_with_reason"] = "省略"
    with pytest.raises(ValueError):
        validate_storyboard_v2_contract(invalid, _subject_types())


def test_nested_boundary_and_position_references_are_type_checked() -> None:
    payload = _legacy_v2_payload()
    shot = payload["scenes"][0]["shot_groups"][0]["shots"][0]
    shot["character_positions"] = [{"ref": "P01", "position": "左中", "facing": "面朝右"}]

    with pytest.raises(ValueError):
        validate_storyboard_v2_contract(payload, _subject_types())


def test_frame_role_and_action_beat_are_server_derived_including_single() -> None:
    assert derive_frame_role(1, 1) == "single"
    assert derive_frame_role(1, 3) == "first"
    assert derive_frame_role(2, 3) == "key"
    assert derive_frame_role(3, 3) == "last"
    assert infer_action_beat("角色突然推门", "key") == "trigger"
    assert infer_action_beat("角色沉默并回望", "first") == "aftermath"
    assert infer_action_beat("角色缓慢行走", "last") == "aftermath"

    contract = StoryboardV2Contract.model_validate(_legacy_v2_payload())
    summaries = build_group_director_summaries(contract.scenes[0].shot_groups[0])
    assert [item.frame_role for item in summaries] == ["first", "last"]
    assert "spatial_continuity" in summaries[0].must_categories
    assert "动作拍点=trigger" in summaries[0].render()


def test_explicit_action_beat_is_controlled_and_takes_precedence() -> None:
    payload = _legacy_v2_payload()
    visual = payload["scenes"][0]["shot_groups"][0]["shots"][0]["visual"]
    visual["action_beat"] = "aftermath"
    contract = validate_storyboard_v2_contract(payload, _subject_types())

    summary = build_group_director_summaries(contract.scenes[0].shot_groups[0])[0]
    snapshot = build_shot_structured_data(
        contract.scenes[0].shot_groups[0].shots[0]
    )

    assert summary.action_beat == "aftermath"
    assert "explicit_action_beat" in summary.reason_tags
    assert snapshot["visual"]["action_beat"] == "aftermath"

    visual["action_beat"] = "opening"
    with pytest.raises(ValueError):
        validate_storyboard_v2_contract(payload, _subject_types())


def test_audience_visibility_uses_status_with_permission_input_compatibility() -> None:
    payload = _legacy_v2_payload()
    first = payload["scenes"][0]["shot_groups"][0]["shots"][0]
    first["audience_visibility"] = [
        {
            "fact": "C01 的真实身份",
            "status": "withhold_now",
            "evidence": ["C01 始终背对镜头"],
            "reveal_trigger": "下一场 C02 叫出真名",
        }
    ]
    contract = validate_storyboard_v2_contract(payload, _subject_types())
    item = contract.scenes[0].shot_groups[0].shots[0].audience_visibility[0]

    assert item.status == "withhold_now"
    assert item.permission == "withhold_now"
    assert item.evidence == ["C01 始终背对镜头"]
    assert "status" in item.model_dump()
    assert "permission" not in item.model_dump()
    snapshot = build_shot_structured_data(
        contract.scenes[0].shot_groups[0].shots[0]
    )
    assert snapshot["audience_visibility"][0]["status"] == "withhold_now"
    assert snapshot["audience_visibility"][0]["evidence"] == ["C01 始终背对镜头"]

    legacy = deepcopy(payload)
    legacy_item = legacy["scenes"][0]["shot_groups"][0]["shots"][0][
        "audience_visibility"
    ][0]
    legacy_item["permission"] = legacy_item.pop("status")
    legacy_contract = validate_storyboard_v2_contract(legacy, _subject_types())
    assert (
        legacy_contract.scenes[0].shot_groups[0].shots[0].audience_visibility[0].status
        == "withhold_now"
    )

    schema = StoryboardV2Contract.model_json_schema()
    visibility_schema = schema["$defs"]["AudienceVisibilityItem"]
    assert "status" in visibility_schema["properties"]
    assert "permission" not in visibility_schema["properties"]
    assert visibility_schema["properties"]["evidence"]["maxItems"] == 5

    too_many_evidence = deepcopy(payload)
    too_many_evidence["scenes"][0]["shot_groups"][0]["shots"][0][
        "audience_visibility"
    ][0]["evidence"] = [f"证据 {index}" for index in range(6)]
    with pytest.raises(ValueError):
        validate_storyboard_v2_contract(too_many_evidence, _subject_types())


def test_explicit_withhold_requires_trigger_but_missing_field_remains_legacy_compatible() -> None:
    validate_storyboard_v2_contract(_legacy_v2_payload(), _subject_types())
    payload = _legacy_v2_payload()
    payload["scenes"][0]["shot_groups"][0]["shots"][0]["audience_visibility"] = [
        {"fact": "C01 的真实身份", "status": "withhold_now", "evidence": []}
    ]

    with pytest.raises(ValueError) as exc_info:
        validate_storyboard_v2_contract(payload, _subject_types())

    diagnostic = getattr(exc_info.value, "diagnostic")
    assert diagnostic["validation"]["codes"] == ["withhold_now_requires_reveal_trigger"]
    assert diagnostic["validation"]["paths"] == [
        "scenes[].shot_groups[].shots[].audience_visibility[].reveal_trigger"
    ]


def test_coverage_items_support_ranges_and_cross_shot_repeat_reasons() -> None:
    payload = _legacy_v2_payload()
    first, second = payload["scenes"][0]["shot_groups"][0]["shots"]
    first["coverage"] = [
        {
            "block_ref": "B01",
            "status": "covered",
            "source_range": {"char_start": 0, "char_end": 8},
            "summary": "推门动作",
        }
    ]
    second["coverage"] = [{"block_ref": "B01", "status": "repeated"}]
    contract = validate_storyboard_v2_contract(payload, _subject_types())

    snapshot = build_shot_structured_data(
        contract.scenes[0].shot_groups[0].shots[0]
    )
    assert snapshot["coverage"][0]["source_range"] == {
        "char_start": 0,
        "char_end": 8,
    }

    report = evaluate_storyboard_quality(contract, expected_source_blocks=["B01", "B02"])
    codes = {warning.code for warning in report.warnings}

    assert report.coverage.covered_block_refs == ("B01",)
    assert "coverage_cross_shot_duplicate_without_reason" in codes
    assert "coverage_missing_block" in codes

    second["coverage"][0]["repeat_reason"] = "用反应镜强调冲突升级"
    contract_with_reason = validate_storyboard_v2_contract(payload, _subject_types())
    codes_with_reason = {
        warning.code
        for warning in evaluate_storyboard_quality(
            contract_with_reason,
            expected_source_blocks=["B01"],
        ).warnings
    }
    assert "coverage_cross_shot_duplicate" in codes_with_reason
    assert "coverage_cross_shot_duplicate_without_reason" not in codes_with_reason


def test_natural_movement_boundaries_suppress_axis_warning_but_unsupported_jump_does_not() -> None:
    payload = _legacy_v2_payload()
    first, second = payload["scenes"][0]["shot_groups"][0]["shots"]
    first["character_positions"] = [
        {"ref": "C01", "position": "左中", "facing": "面朝右"}
    ]
    second["visual"]["action"] = "C01 完成走位"
    second["start_boundary"] = {
        "positions": [{"ref": "C01", "position": "左中", "facing": "面朝右"}]
    }
    second["end_boundary"] = {
        "positions": [{"ref": "C01", "position": "右中", "facing": "面朝左"}]
    }
    second["character_positions"] = [
        {"ref": "C01", "position": "右中", "facing": "面朝左"}
    ]
    moved = validate_storyboard_v2_contract(payload, _subject_types())
    moved_codes = {
        warning.code for warning in evaluate_storyboard_quality(moved).warnings
    }

    assert "position_axis_crossing" not in moved_codes
    assert "facing_direction_changed" not in moved_codes

    third = _shot(
        display_no=3,
        start_seconds=6,
        end_seconds=9,
        action="C01 保持站位注视 C02",
    )
    third["character_positions"] = [
        {"ref": "C01", "position": "右中", "facing": "面朝左"}
    ]
    payload["scenes"][0]["shot_groups"][0]["shots"].append(third)
    payload["scenes"][0]["shot_groups"][0]["duration_seconds"] = 9
    continued = validate_storyboard_v2_contract(payload, _subject_types())
    continued_codes = {
        warning.code for warning in evaluate_storyboard_quality(continued).warnings
    }
    assert "position_axis_crossing" not in continued_codes
    assert "facing_direction_changed" not in continued_codes

    payload["scenes"][0]["shot_groups"][0]["shots"].pop()
    payload["scenes"][0]["shot_groups"][0]["duration_seconds"] = 6
    second.pop("start_boundary")
    second.pop("end_boundary")
    second["visual"]["action"] = "C01 横穿到画面右侧并转身面朝左"
    action_explained = validate_storyboard_v2_contract(payload, _subject_types())
    action_explained_codes = {
        warning.code for warning in evaluate_storyboard_quality(action_explained).warnings
    }
    assert "position_axis_crossing" not in action_explained_codes
    assert "facing_direction_changed" not in action_explained_codes

    second["visual"]["action"] = "C01 沉默注视 C02"
    unsupported = validate_storyboard_v2_contract(payload, _subject_types())
    unsupported_codes = {
        warning.code for warning in evaluate_storyboard_quality(unsupported).warnings
    }
    assert {"position_axis_crossing", "facing_direction_changed"}.issubset(
        unsupported_codes
    )


def test_quality_evaluation_is_warn_only_for_coverage_boundary_position_and_visibility() -> None:
    payload = _legacy_v2_payload()
    first, second = payload["scenes"][0]["shot_groups"][0]["shots"]
    first["source_block_refs"] = ["B01"]
    first["end_boundary"] = {
        "positions": [{"ref": "C01", "position": "左中", "facing": "面朝右"}],
        "gaze": "看向画面右侧",
        "held_props": ["P01"],
    }
    first["character_positions"] = [
        {"ref": "C01", "position": "左中", "facing": "面朝右"}
    ]
    first["audience_visibility"] = [
        {
            "fact": "C01 的真实身份",
            "permission": "withhold_now",
            "reveal_trigger": "下一场 C02 认出 C01",
        }
    ]
    second["start_boundary"] = {
        "positions": [{"ref": "C01", "position": "右中", "facing": "面朝左"}],
        "gaze": "看向画面左侧",
        "held_props": [],
    }
    second["character_positions"] = [
        {"ref": "C01", "position": "右中", "facing": "面朝左"}
    ]
    payload["omitted_source_blocks"] = [{"block_ref": "B02"}]
    contract = validate_storyboard_v2_contract(payload, _subject_types())

    report = evaluate_storyboard_quality(
        contract,
        expected_source_blocks=["B01", "B02", "B03"],
    )

    codes = {warning.code for warning in report.warnings}
    assert {
        "coverage_omission_without_reason",
        "coverage_missing_block",
        "boundary_discontinuity",
        "position_axis_crossing",
        "facing_direction_changed",
        "audience_fact_never_revealed",
    }.issubset(codes)
    assert report.coverage.coverage_ratio == pytest.approx(2 / 3)
    assert all(warning.severity == "warning" for warning in report.warnings)
    assert report.as_dict()["status"] == "warning"


def test_script_blocks_and_structured_shot_snapshot_are_stable() -> None:
    text = "第一段动作。\n\n角色甲：第一句。\n角色乙：第二句。"
    blocks = build_script_blocks(text)
    assert [block.block_ref for block in blocks] == ["B01", "B02"]
    assert text[blocks[0].char_start : blocks[0].char_end] == blocks[0].text
    assert canonical_source_block_ref("b001") == "B01"

    contract = StoryboardV2Contract.model_validate(_legacy_v2_payload())
    snapshot = build_shot_structured_data(
        contract.scenes[0].shot_groups[0].shots[0],
        subject_ids_by_ref={"C01": "subject-1", "C02": "subject-2"},
        background_ref="BG01",
    )
    assert snapshot["schema_version"] == "storyboard.shot-structure.v1"
    assert snapshot["visual"]["camera_movement"] == "固定"
    assert snapshot["subject_ids_by_ref"] == {"C01": "subject-1", "C02": "subject-2"}
    assert snapshot["background_ref"] == "BG01"


def test_group_quality_summary_contains_server_derived_commands_and_boundaries() -> None:
    payload = _legacy_v2_payload()
    first, second = payload["scenes"][0]["shot_groups"][0]["shots"]
    first["source_block_refs"] = ["B01"]
    second["source_block_refs"] = ["B02"]
    first["start_boundary"] = {
        "positions": [{"ref": "C01", "position": "左中", "facing": "面朝右"}]
    }
    second["end_boundary"] = {
        "positions": [{"ref": "C01", "position": "左中", "facing": "面朝右"}]
    }
    contract = StoryboardV2Contract.model_validate(payload)

    summary = build_shot_group_quality_summary(contract.scenes[0].shot_groups[0])

    assert summary["coverage_block_refs"] == ["B01", "B02"]
    commands = summary["director_commands"]
    assert [item["frame_role"] for item in commands] == ["first", "last"]
    assert summary["boundary_snapshots"]["start"]["boundary_role"] == "start"
    assert summary["boundary_snapshots"]["end"]["boundary_role"] == "end"


def test_boundary_projection_is_role_limited_and_resolves_subject_names() -> None:
    boundary = ShotBoundary.model_validate(
        {
            "positions": [{"ref": "C01", "position": "左中", "facing": "面朝右"}],
            "hands": {"left": "持杯", "right": "自然下垂"},
            "held_props": ["P01"],
            "visible_state": ["眉头紧锁"],
        }
    )
    projection = project_boundary(
        boundary,
        role="start",
        subject_names={"C01": "阿明", "P01": "茶杯"},
    )

    assert projection is not None
    assert projection.role == "start"
    assert projection.prompt_fragment.startswith("【入画状态】")
    assert "阿明:左中、面朝右" in projection.prompt_fragment
    assert projection.snapshot["boundary_role"] == "start"


def test_shot_models_expose_structured_fields_and_hash_covers_quality_and_continuation() -> None:
    assert "structured_data" in Shot.__table__.columns
    assert "quality_summary" in ShotGroup.__table__.columns

    values = {
        "audio_elements": {},
        "duration_seconds": 6,
        "materials": {},
        "prompt": "镜头提示",
        "quality_summary": {},
        "video_duration_seconds": 6,
    }
    base = SimpleNamespace(**values)
    with_quality = SimpleNamespace(**{**values, "quality_summary": {"warnings": [{"code": "x"}]}})
    continuation = SimpleNamespace(**values, is_continuation=True)

    legacy_payload = {
        key: values[key]
        for key in (
            "audio_elements",
            "duration_seconds",
            "materials",
            "prompt",
            "video_duration_seconds",
        )
    }
    legacy_hash = hashlib.sha256(
        json.dumps(
            legacy_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert shot_group_content_hash(base) == legacy_hash
    assert shot_group_content_hash(base) != shot_group_content_hash(with_quality)
    assert shot_group_content_hash(base) != shot_group_content_hash(continuation)
