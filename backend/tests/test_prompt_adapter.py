"""视频提示词能力矩阵、适配器与结构增强纯函数测试。"""

from __future__ import annotations

import pytest

from app.providers.prompt_adapter import PromptAdaptationError, adapt_prompt, unicode_safe_prefix
from app.providers.registry import get_prompt_capability
from app.services.video_prompt import (
    compose_video_prompt,
    render_video_execution_prompt,
)


def _complete_structured_snapshot(
    *,
    display_no: int,
    start_seconds: int,
    end_seconds: int,
    visual: dict[str, object],
    sound_design: str = "无",
    dialogue: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": "storyboard.shot-structure.v1",
        "display_no": display_no,
        "start_seconds": start_seconds,
        "end_seconds": end_seconds,
        "visual": visual,
        "narrative_purpose": "展现角色当前的行动与状态",
        "character_refs": ["C01"],
        "prop_refs": [],
        "sound_refs": [],
        "sound_design": sound_design,
        "dialogue": dialogue,
        "source_block_refs": ["B01"],
    }


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


def test_render_video_execution_prompt_uses_natural_per_shot_direction() -> None:
    constraint = "【约束】\n1. 人物一致\n2. 禁止字幕"
    shots = [
        {
            "display_no": 1,
            "start_seconds": 0,
            "end_seconds": 3,
            "characters": ["阿明"],
            "props": ["茶杯"],
            "structured_data": _complete_structured_snapshot(
                display_no=1,
                start_seconds=0,
                end_seconds=3,
                visual={
                    "shot_size": "全景",
                    "composition": ["三分法", "纵深构图"],
                    "camera_angle": "平视",
                    "camera_movement": "固定",
                    "lighting": "冷色屏幕光与暖色室内灯交织",
                    "frame_description": "阿明独自在电脑前工作",
                    "action": "持续敲键盘",
                    "emotion": "沉稳",
                },
                sound_design="键盘轻响与设备风扇声持续",
            ),
        },
        {
            "display_no": 2,
            "start_seconds": 3,
            "end_seconds": 6,
            "characters": ["阿明"],
            "dialogue_character": "阿明",
            "structured_data": _complete_structured_snapshot(
                display_no=2,
                start_seconds=3,
                end_seconds=6,
                visual={
                    "shot_size": "近景",
                    "composition": ["前景遮挡"],
                    "camera_angle": "斜侧",
                    "camera_movement": "推镜",
                    "lighting": "屏幕光照亮面部",
                    "frame_description": "阿明抬眼看向屏幕",
                    "action": "停下敲键盘并回头",
                    "emotion": "警觉",
                },
                dialogue={
                    "speaker_ref": "C01",
                    "kind": "voice_over",
                    "text": "系统性变革到了深水区，技术迭代一天都不能停……",
                },
            ),
        },
    ]

    rendered = render_video_execution_prompt(
        shots,
        background="融媒众创空间大楼-办公桌前-内-夜",
        duration_seconds=6,
        subject_names_by_ref={"C01": "老姜"},
    )

    assert rendered.startswith("故事发生在融媒众创空间大楼-办公桌前-内-夜。\n")
    assert "【镜头1:0s～3s】全景·平视，镜头保持固定。" in rendered
    assert "【镜头2:3s～6s】近景·斜侧，镜头缓慢向主体推进。" in rendered
    assert "主体位于画面一侧，另一侧保留环境空间" in rendered
    assert "前景有虚化物体局部遮挡画面边缘" in rendered
    assert "画面中保留茶杯等道具" in rendered
    assert "环境中可听见键盘轻响与设备风扇声持续" in rendered
    assert "人物呈现沉稳。" in rendered
    assert "面部和肢体自然稳定" in rendered
    assert "老姜的画外音响起，内容为“系统性变革到了深水区，技术迭代一天都不能停……”；画面中的人物不张口" in rendered
    assert "构图是【" not in rendered
    assert "声音设计是【" not in rendered
    assert "【角色】" not in rendered
    assert "Camera " not in rendered
    assert (
        compose_video_prompt(
            rendered,
            shots=shots,
            duration_seconds=6,
        )
        == rendered
    )
    manual_execution = render_video_execution_prompt(
        shots,
        background="融媒众创空间大楼-办公桌前-内-夜",
        duration_seconds=6,
        subject_names_by_ref={"C01": "老姜"},
        include_constraint_lock=False,
    )
    completed_manual_execution = compose_video_prompt(
        manual_execution,
        shots=shots,
        duration_seconds=6,
    )
    assert completed_manual_execution.count("【镜头1:0s～3s】") == 1
    assert completed_manual_execution.endswith("画面不出现任何可读文字、字幕、Logo 或水印。")

    result = compose_video_prompt(
        f"创作意图\n{constraint}",
        shots=shots,
        duration_seconds=6,
    )

    assert result.startswith("创作意图\n")
    assert "【镜头1:0s～3s】全景·平视，镜头保持固定。" in result
    assert "【镜头2:3s～6s】近景·斜侧，镜头缓慢向主体推进。" in result
    assert result.endswith(constraint)
    assert compose_video_prompt(result, shots=shots, duration_seconds=6) == result


def test_render_video_execution_prompt_rejects_segment_outside_group_duration() -> None:
    with pytest.raises(ValueError, match="结束时间"):
        render_video_execution_prompt(
            [
                {
                    "display_no": 1,
                    "start_seconds": 0,
                    "end_seconds": 7,
                    "structured_data": _complete_structured_snapshot(
                        display_no=1,
                        start_seconds=0,
                        end_seconds=7,
                        visual={
                            "shot_size": "中景",
                            "composition": ["单人"],
                            "camera_angle": "平视",
                            "camera_movement": "固定",
                            "lighting": "室内灯光",
                            "frame_description": "人物走入房间",
                            "action": "走入房间",
                            "emotion": "平静",
                        },
                    ),
                }
            ],
            duration_seconds=6,
        )


def test_compose_video_prompt_replaces_legacy_tagged_group_prompt_with_natural_fallback() -> None:
    legacy_prompt = (
        "【镜头1:0s～3s】场景是融媒众创空间大楼-办公桌前-内-夜，全景·平视，固定，"
        "冷色调的屏幕光与暖色调的室内灯光交织，老姜独自在电脑前加班，"
        "构图是【三分法、纵深构图】，"
        "声音设计是【远处城市低频车流声，空调外机低鸣；声音桥延续至室内】\n"
        "【角色】老姜、电脑、茶杯\n"
        "【叙事目的】建立深夜办公环境与老姜的工作状态\n"
        "【约束】人物特征高度一致，禁止出现任何形式的字幕。"
    )
    shots = [
        {
            "display_no": 1,
            "start_seconds": 0,
            "end_seconds": 3,
            "characters": ["老姜"],
            "props": ["电脑", "茶杯"],
            "shot_line": legacy_prompt.splitlines()[0],
            "content": "老姜独自在电脑前加班，专注处理屏幕上的工作。",
            "structured_data": {
                "visual": {
                    "shot_size": "全景",
                    "composition": ["三分法", "纵深构图"],
                    "camera_angle": "平视",
                    "camera_movement": "固定",
                    "lighting": "冷色调的屏幕光与暖色调的室内灯光交织",
                    "frame_description": "夜色中的办公区域灯火通明",
                    "action": "持续处理电脑上的工作",
                    "emotion": "沉稳",
                },
                "sound_design": "远处城市低频车流声，空调外机低鸣；声音桥延续至室内",
            },
        }
    ]

    rendered = compose_video_prompt(legacy_prompt, shots=shots, duration_seconds=3)

    assert "构图是【" not in rendered
    assert "声音设计是【" not in rendered
    assert "【角色】" not in rendered
    assert "【叙事目的】" not in rendered
    assert "主体位于画面一侧，另一侧保留环境空间" in rendered
    assert "前景、中景和背景形成明显纵深" in rendered
    assert "环境中可听见远处城市低频车流声，空调外机低鸣；声音桥延续至室内" in rendered
    assert rendered.endswith("【约束】人物特征高度一致，禁止出现任何形式的字幕。")
