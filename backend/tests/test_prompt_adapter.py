"""视频提示词能力矩阵、适配器与结构增强纯函数测试。"""

from __future__ import annotations

import pytest

from app.providers.prompt_adapter import PromptAdaptationError, adapt_prompt, unicode_safe_prefix
from app.providers.registry import get_prompt_capability
from app.services.video_prompt import (
    CAMERA_MOVEMENT_VIDEO_PHRASES,
    build_subject_over_duration,
    compose_video_prompt,
)


def test_prompt_capability_uses_alias_and_model_override() -> None:
    default = get_prompt_capability("dashscope", model="wan2.7-i2v-plus")
    happyhorse = get_prompt_capability("bailian", model="HappyHorse-1.1-I2V")

    assert default.provider == "bailian"
    assert default.max_prompt_characters == 800
    assert happyhorse.max_prompt_characters == 2500
    assert happyhorse.adapter_version == "video-prompt-adapter.v1"
    assert happyhorse.supports_reference_audio is True


def test_prompt_capability_exposes_provider_syntax() -> None:
    assert get_prompt_capability("volcengine").prompt_syntax == "ark_content"
    assert get_prompt_capability("neolink").prompt_syntax == "ark_content"
    assert get_prompt_capability("mgrouter").prompt_syntax == "aicc_content"
    assert get_prompt_capability("openai").prompt_syntax == "plain"


def test_adapter_preserves_constraint_tail_when_chinese_prompt_is_truncated() -> None:
    constraint = "【约束】\n1. 人物特征高度一致\n2. 禁止出现任何形式的字幕"
    source = "主体持续向前行走，" * 120 + "\n" + constraint

    result = adapt_prompt(source, provider="bailian", model="wan2.7-i2v-plus")

    assert result.truncated is True
    assert result.adapted_characters <= 800
    assert result.text.endswith(constraint)
    assert result.protected_suffix_characters == len(constraint)
    assert result.original_characters == len(source)


def test_adapter_preserves_boundary_facts_together_with_constraint_tail() -> None:
    boundary = (
        "【冻结边界事实 boundary-facts】\n"
        "起始边界：阿明位于门外。\n结束边界：阿明已进入室内。"
    )
    constraint = "【约束】\n1. 人物特征高度一致\n2. 禁止出现任何形式的字幕"
    source = "冗长镜头描述，" * 120 + f"\n{boundary}\n{constraint}"

    result = adapt_prompt(source, provider="bailian", model="wan2.7-i2v-plus")

    protected = f"{boundary}\n{constraint}"
    assert result.truncated is True
    assert result.text.endswith(protected)
    assert result.protected_suffix_characters == len(protected)
    assert result.adapted_characters <= 800


def test_adapter_refuses_to_truncate_an_oversized_protected_tail() -> None:
    source = "开场\n【约束】" + "不得改变" * 300

    with pytest.raises(PromptAdaptationError, match="约束.*超过"):
        adapt_prompt(source, provider="bailian", model="wan2.7-i2v-plus")


def test_unicode_safe_prefix_does_not_split_emoji_or_combining_character() -> None:
    assert unicode_safe_prefix("甲👩🏽‍🚀乙", 4) == "甲"
    assert unicode_safe_prefix("甲e\u0301乙", 2) == "甲"
    assert unicode_safe_prefix("甲e\u0301乙", 3) == "甲e\u0301"


def test_all_controlled_camera_movements_have_a_reusable_phrase() -> None:
    assert set(CAMERA_MOVEMENT_VIDEO_PHRASES) == {
        "固定",
        "推镜",
        "拉镜",
        "摇镜",
        "移镜",
        "跟拍",
        "升降",
        "环绕",
        "手持",
    }
    assert all(phrase.startswith("Camera ") for phrase in CAMERA_MOVEMENT_VIDEO_PHRASES.values())


def test_compose_video_prompt_puts_motion_first_subject_timing_before_constraint_tail() -> None:
    constraint = "【约束】\n1. 人物一致\n2. 禁止字幕"
    shots = [
        {
            "display_no": 2,
            "start_seconds": 3,
            "end_seconds": 6,
            "characters": ["阿明"],
            "action": "停下并回头",
        },
        {
            "display_no": 1,
            "start_seconds": 0,
            "end_seconds": 3,
            "characters": ["阿明"],
            "visual": {"action": "推门进入"},
        },
    ]

    result = compose_video_prompt(
        f"场景是办公室\n{constraint}",
        camera_movement="推镜",
        shots=shots,
        duration_seconds=6,
    )

    assert result.startswith("Camera dollies forward smoothly at a steady pace.\n")
    assert "0s～3s：阿明：推门进入\n3s～6s：阿明：停下并回头" in result
    assert result.endswith(constraint)
    assert compose_video_prompt(
        result,
        camera_movement="推镜",
        shots=shots,
        duration_seconds=6,
    ) == result


def test_subject_over_duration_rejects_segment_outside_group_duration() -> None:
    with pytest.raises(ValueError, match="结束时间"):
        build_subject_over_duration(
            [{"start_seconds": 0, "end_seconds": 7, "action": "走入房间"}],
            duration_seconds=6,
        )


def test_unknown_camera_movement_is_not_silently_guessed() -> None:
    with pytest.raises(ValueError, match="不支持的运镜类型"):
        compose_video_prompt("场景", camera_movement="甩镜")
