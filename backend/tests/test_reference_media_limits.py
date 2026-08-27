"""视频生成多模态参考素材限额的聚焦测试。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.core.errors import BadRequestError
from app.core.reference_media_limits import (
    MAX_REFERENCE_AUDIOS,
    MAX_REFERENCE_IMAGES,
    MAX_REFERENCE_MEDIA,
    MAX_REFERENCE_VIDEOS,
    ReferenceMediaCounts,
    ReferenceMediaLimitExceeded,
    ReferenceMediaLimits,
    effective_reference_media_limits_for_group,
    public_reference_media_limits,
    reference_media_limit_violations,
    reference_media_limits_for_provider,
    trim_automatic_reference_selection,
    validate_reference_media_counts,
    validate_shot_group_reference_selection,
)
from app.services.reference_aggregator import (
    AggregatedReferences,
    VideoReference,
    _aggregate_videos,
)
from app.services.shot_group_edit import _normalise_video_references


def _items(prefix: str, count: int) -> list[dict[str, str]]:
    return [{"name": f"{prefix}-{index}", "subject_id": f"{prefix}-{index}"} for index in range(count)]


def test_continuation_first_frame_occupies_image_and_total_quota() -> None:
    materials = {
        "character": _items("image", MAX_REFERENCE_IMAGES),
        "background": [],
        "prop": [],
        "other": [],
        "video": [],
    }
    with pytest.raises(ReferenceMediaLimitExceeded, match="含承接上一组尾帧"):
        validate_shot_group_reference_selection(
            materials,
            {"characters": [], "sounds": []},
            reserve_continuation_first_frame=True,
        )

    counts = validate_shot_group_reference_selection(
        {**materials, "character": _items("image", MAX_REFERENCE_IMAGES - 1)},
        {"characters": [], "sounds": []},
        reserve_continuation_first_frame=True,
    )
    assert counts.images == MAX_REFERENCE_IMAGES - 1
    assert counts.first_frames == 1
    assert counts.total == MAX_REFERENCE_IMAGES


def test_total_quota_rejects_valid_individual_categories() -> None:
    with pytest.raises(ReferenceMediaLimitExceeded, match="总数最多支持"):
        validate_reference_media_counts(
            ReferenceMediaCounts(
                images=MAX_REFERENCE_IMAGES,
                audios=MAX_REFERENCE_AUDIOS,
                videos=1,
            )
        )


def test_limit_violation_list_reports_each_exceeded_category() -> None:
    violations = reference_media_limit_violations(
        ReferenceMediaCounts(
            images=MAX_REFERENCE_IMAGES + 1,
            audios=MAX_REFERENCE_AUDIOS + 1,
            videos=MAX_REFERENCE_VIDEOS + 1,
        )
    )

    assert [violation.code for violation in violations] == [
        "reference_images_limit_exceeded",
        "reference_audios_limit_exceeded",
        "reference_videos_limit_exceeded",
        "reference_media_total_limit_exceeded",
    ]


@pytest.mark.parametrize(
    ("provider", "model", "expected"),
    [
        ("volcengine", "doubao-seedance", ReferenceMediaLimits(9, 3, 3, 12)),
        ("neolink", "doubao-seedance", ReferenceMediaLimits(9, 3, 3, 12)),
        (
            "mgrouter",
            "AICC-Doubao-Seedance-2.0",
            ReferenceMediaLimits(
                9,
                3,
                0,
                12,
                first_frame_exclusive=True,
                audio_requires_reference_image=True,
            ),
        ),
        ("gemini", "veo-3", ReferenceMediaLimits(4, 0, 0, 4)),
        ("openai", "sora-2", ReferenceMediaLimits(1, 0, 0, 1)),
        (
            "bailian",
            "wan2.6-i2v",
            ReferenceMediaLimits(1, 1, 0, 2, requires_input_image=True),
        ),
        (
            "bailian",
            "wanx2.1-kf2v",
            ReferenceMediaLimits(1, 0, 0, 1, requires_input_image=True),
        ),
        ("bailian", "wan2.1-t2v", ReferenceMediaLimits(0, 0, 0, 0)),
        (
            "bailian",
            "wan3.0-video",
            ReferenceMediaLimits(
                10,
                5,
                5,
                20,
                first_frame_exclusive=True,
                requires_input_image=False,
            ),
        ),
        (
            "bailian",
            "wan3.0-video-prime",
            ReferenceMediaLimits(
                10,
                5,
                5,
                20,
                first_frame_exclusive=True,
                requires_input_image=False,
            ),
        ),
    ],
)
def test_provider_reference_limits_match_adapter_input_capabilities(
    provider: str,
    model: str,
    expected: ReferenceMediaLimits,
) -> None:
    assert reference_media_limits_for_provider(provider, model) == expected


def test_bailian_wan30_supports_first_frame_and_pure_text_generation() -> None:
    # wan3.0 支持纯文生（requires_input_image=False），但 images>0 使首帧
    # 续帧链路（materials 第一图作为 first_frame 提交）保持可用。
    limits = reference_media_limits_for_provider("bailian", "wan3.0-video-prime")

    assert limits.supports_first_frame is True
    assert limits.requires_input_image is False
    # 纯文生：无图无音频不触发 reference_input_image_required。
    assert reference_media_limit_violations(
        ReferenceMediaCounts(images=0, audios=0, videos=0),
        limits=limits,
    ) == []


def test_wan30_automatic_merge_preserves_all_supported_reference_modalities() -> None:
    limits = reference_media_limits_for_provider("bailian", "wan3.0-video-prime")
    materials, audio = trim_automatic_reference_selection(
        {
            "character": _items("image", 12),
            "background": [],
            "prop": [],
            "other": [],
            "video": [
                {"name": f"video-{index}", "video_job_id": f"job-{index}"}
                for index in range(7)
            ],
        },
        {
            "characters": _items("audio", 7),
            "sounds": [],
        },
        limits=limits,
    )

    assert [item["name"] for item in materials["character"]] == [
        f"image-{index}" for index in range(10)
    ]
    assert [item["name"] for item in audio["characters"]] == [
        f"audio-{index}" for index in range(5)
    ]
    assert [item["name"] for item in materials["video"]] == [
        f"video-{index}" for index in range(5)
    ]


def test_wan30_first_frame_mode_rejects_reference_media() -> None:
    limits = reference_media_limits_for_provider("bailian", "wan3.0-video-prime")

    assert limits.first_frame_exclusive is True
    with pytest.raises(ReferenceMediaLimitExceeded, match="参考图最多支持 1 个"):
        validate_shot_group_reference_selection(
            {
                "character": _items("image", 1),
                "background": [],
                "prop": [],
                "other": [],
                "video": [],
            },
            {"characters": [], "sounds": []},
            reserve_continuation_first_frame=True,
            limits=limits,
        )


def test_text_to_video_model_disables_continuation_first_frame_capability() -> None:
    limits = reference_media_limits_for_provider("bailian", "wan2.1-t2v")

    assert limits.supports_first_frame is False
    assert public_reference_media_limits(limits)["continuation_first_frame_images"] == 0


@pytest.mark.asyncio
async def test_first_frame_reservation_is_disabled_when_model_cannot_consume_images() -> None:
    from app.services.video_generation import _reserves_first_frame

    db = SimpleNamespace(execute=AsyncMock())
    group = SimpleNamespace(is_continuation=True)

    assert await _reserves_first_frame(db, group, supports_first_frame=False) is False
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_video_workbench_disables_video_references_for_unsupported_provider(
    monkeypatch,
) -> None:
    from app.crud import shot
    from app.services.runtime_models import RuntimeVideoModel

    class _Result:
        def scalar_one_or_none(self):
            return None

    monkeypatch.setattr(
        shot,
        "resolve_video_model",
        AsyncMock(
            return_value=RuntimeVideoModel(
                provider="bailian",
                model="wan2.6-i2v",
                reference_image_audit_required=False,
                task_provider_key="bailian",
            )
        ),
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=_Result()))

    workbench = await shot.get_video_workbench(db, "script-1", "episode-1")

    assert workbench.reference_limits == {
        "images": 1,
        "audios": 1,
        "videos": 0,
        "total": 2,
        "continuation_first_frame_images": 0,
        "audio_requires_reference_image": False,
        "requires_input_image": True,
    }


@pytest.mark.asyncio
async def test_video_workbench_disables_continuation_frame_for_text_to_video_model(
    monkeypatch,
) -> None:
    from app.crud import shot
    from app.services.runtime_models import RuntimeVideoModel

    class _Result:
        def scalar_one_or_none(self):
            return None

    monkeypatch.setattr(
        shot,
        "resolve_video_model",
        AsyncMock(
            return_value=RuntimeVideoModel(
                provider="bailian",
                model="wan2.1-t2v",
                reference_image_audit_required=False,
                task_provider_key="bailian",
            )
        ),
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=_Result()))

    workbench = await shot.get_video_workbench(db, "script-1", "episode-1")

    assert workbench.reference_limits == {
        "images": 0,
        "audios": 0,
        "videos": 0,
        "total": 0,
        "continuation_first_frame_images": 0,
        "audio_requires_reference_image": False,
        "requires_input_image": False,
    }


def test_mgrouter_first_frame_group_uses_exclusive_effective_quota() -> None:
    limits = reference_media_limits_for_provider("mgrouter", "AICC-Doubao-Seedance-2.0")
    effective_limits = effective_reference_media_limits_for_group(
        limits,
        reserve_first_frame=True,
    )

    assert public_reference_media_limits(effective_limits) == {
        "images": 1,
        "audios": 0,
        "videos": 0,
        "total": 1,
        "continuation_first_frame_images": 0,
        "audio_requires_reference_image": True,
        "requires_input_image": False,
    }
    with pytest.raises(ReferenceMediaLimitExceeded, match="参考图最多支持 1 个"):
        validate_shot_group_reference_selection(
            {
                "character": _items("image", 1),
                "background": [],
                "prop": [],
                "other": [],
                "video": [],
            },
            {"characters": [], "sounds": []},
            reserve_continuation_first_frame=True,
            limits=limits,
        )

    materials, audio = trim_automatic_reference_selection(
        {
            "character": _items("image", 3),
            "background": [],
            "prop": [],
            "other": [],
            "video": [],
        },
        {"characters": _items("audio", 2), "sounds": []},
        reserve_continuation_first_frame=True,
        limits=limits,
    )
    assert all(not items for items in materials.values())
    assert all(not items for items in audio.values())


def test_automatic_merge_drops_mgrouter_audio_without_normal_reference_image() -> None:
    limits = reference_media_limits_for_provider("mgrouter", "AICC-Doubao-Seedance-2.0")

    materials, audio = trim_automatic_reference_selection(
        {"character": [], "background": [], "prop": [], "other": [], "video": []},
        {"characters": _items("audio", 2), "sounds": []},
        limits=limits,
    )

    assert all(not items for items in materials.values())
    assert all(not items for items in audio.values())


def test_bailian_i2v_requires_an_image_or_first_frame() -> None:
    limits = reference_media_limits_for_provider("bailian", "wanx2.1-kf2v")

    assert [violation.code for violation in reference_media_limit_violations(
        ReferenceMediaCounts(images=0, audios=0, videos=0),
        limits=limits,
    )] == ["reference_input_image_required"]
    validate_reference_media_counts(
        ReferenceMediaCounts(images=0, audios=0, videos=0, first_frames=1),
        limits=limits,
    )


def test_reserved_first_frame_does_not_satisfy_bailian_i2v_input_requirement() -> None:
    limits = reference_media_limits_for_provider("bailian", "wan2.6-i2v")

    assert [violation.code for violation in reference_media_limit_violations(
        ReferenceMediaCounts(
            images=0,
            audios=0,
            videos=0,
            first_frames=1,
            available_first_frames=0,
        ),
        limits=limits,
    )] == ["reference_input_image_required"]


def test_automatic_merge_without_bailian_i2v_image_remains_repairable() -> None:
    limits = reference_media_limits_for_provider("bailian", "wan2.6-i2v")
    materials, audio = trim_automatic_reference_selection(
        {"character": [], "background": [], "prop": [], "other": [], "video": []},
        {"characters": [], "sounds": []},
        limits=limits,
    )

    assert [violation.code for violation in reference_media_limit_violations(
        ReferenceMediaCounts(images=0, audios=0, videos=0),
        limits=limits,
    )] == ["reference_input_image_required"]
    assert all(not items for items in materials.values())
    assert all(not items for items in audio.values())


def test_automatic_merge_trims_all_modalities_to_stable_quota() -> None:
    materials, audio = trim_automatic_reference_selection(
        {
            "character": _items("image", MAX_REFERENCE_IMAGES + 3),
            "background": [],
            "prop": [],
            "other": [],
            "video": [
                {"name": f"video-{index}", "video_job_id": f"job-{index}"}
                for index in range(MAX_REFERENCE_VIDEOS)
            ],
        },
        {
            "characters": [
                {"name": f"audio-{index}", "subject_id": f"audio-{index}"}
                for index in range(MAX_REFERENCE_AUDIOS + 2)
            ],
            "sounds": [],
        },
    )

    image_count = sum(len(materials[key]) for key in ("character", "background", "prop", "other"))
    audio_count = len(audio["characters"]) + len(audio["sounds"])
    video_count = len(materials["video"])
    assert image_count == MAX_REFERENCE_IMAGES
    assert audio_count == MAX_REFERENCE_AUDIOS
    assert video_count == 0
    assert image_count + audio_count + video_count == MAX_REFERENCE_MEDIA
    assert [item["name"] for item in materials["character"]] == [
        f"image-{index}" for index in range(MAX_REFERENCE_IMAGES)
    ]


def test_video_reference_must_be_completed_job_with_storage_output() -> None:
    completed = SimpleNamespace(
        status="succeeded",
        output_json={"video_storage_key": "generate_videos/job-1/video.mp4"},
    )
    assert _normalise_video_references(
        [{"name": "上一镜头", "video_job_id": "job-1"}],
        successful_video_jobs={"job-1": completed},
    ) == [
        {
            "name": "上一镜头",
            "video_job_id": "job-1",
            "storage_key": "generate_videos/job-1/video.mp4",
        }
    ]

    with pytest.raises(BadRequestError, match="当前剧本已成功"):
        _normalise_video_references(
            [{"name": "失败视频", "video_job_id": "job-2"}],
            successful_video_jobs={
                "job-2": SimpleNamespace(status="failed", output_json={})
            },
        )


async def test_stored_video_key_survives_source_job_cleanup() -> None:
    """服务端保存的产物键应继续作为引用，不能依赖源任务记录存在。"""

    result = AggregatedReferences()
    await _aggregate_videos(
        SimpleNamespace(),
        "script-1",
        [
            VideoReference(
                label="上一镜头",
                video_job_id="deleted-source-job",
                storage_key="generate_videos/source/video.mp4",
            )
        ],
        result,
    )

    assert result.reference_videos == [
        {
            "kind": "storage",
            "storage_key": "generate_videos/source/video.mp4",
            "mime_type": "video/mp4",
            "source_video_job_id": "deleted-source-job",
        }
    ]
    assert result.missing_videos == []
