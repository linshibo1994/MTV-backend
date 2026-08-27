"""视频生成多模态参考素材的统一限额与自动收敛规则。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

MAX_REFERENCE_IMAGES = 9
MAX_REFERENCE_AUDIOS = 3
MAX_REFERENCE_VIDEOS = 3
MAX_REFERENCE_MEDIA = 12
CONTINUATION_FIRST_FRAME_IMAGES = 1
# 自动镜头组合产生的后续图生组使用该 JSON 标记，而不是篡改用户在工作台
# 上设置的 ``is_continuation``。它只表达该组应从严格直接前驱取得首帧。
CONTINUATION_FROM_PREDECESSOR_KEY = "continuation_from_predecessor"
REFERENCE_VIDEO_SUPPORTED_PROVIDERS = frozenset({"volcengine", "neolink"})

IMAGE_MATERIAL_KEYS = ("character", "background", "prop", "other")
VIDEO_MATERIAL_KEY = "video"


@dataclass(frozen=True)
class ReferenceMediaCounts:
    """提交给视频模型前的参考素材数量。"""

    images: int
    audios: int
    videos: int
    first_frames: int = 0
    # ``first_frames`` 可用于提前预留容量；此字段只表达当前确实可提交的首帧。
    # 为 None 时兼容旧调用，视为与预留数量相同。
    available_first_frames: int | None = None

    @property
    def total(self) -> int:
        return self.images + self.audios + self.videos + self.first_frames

    @property
    def available_input_images(self) -> int:
        """返回当前可实际传给模型的普通图片与首帧数量。"""

        return self.images + (
            self.first_frames
            if self.available_first_frames is None
            else self.available_first_frames
        )


@dataclass(frozen=True)
class ReferenceMediaLimits:
    """某个视频供应商实际支持的参考媒体上限。"""

    images: int = MAX_REFERENCE_IMAGES
    audios: int = MAX_REFERENCE_AUDIOS
    videos: int = MAX_REFERENCE_VIDEOS
    total: int = MAX_REFERENCE_MEDIA
    # 某些供应商把首帧放在互斥输入槽中。首帧存在时，普通图/声音/视频
    # 不能和它并行提交；工作台需改用该镜头组的有效限额。
    first_frame_exclusive: bool = False
    # 某些供应商的参考音频必须和至少一张普通参考图一起提交，首帧不能替代。
    audio_requires_reference_image: bool = False
    # 图生视频模型必须至少接收一张普通参考图或首帧；纯文生模型不受此约束。
    requires_input_image: bool = False

    @property
    def supports_first_frame(self) -> bool:
        """当前适配器是否会把首帧实际作为图片输入提交给模型。"""

        # 现有适配器的首帧与图片引用共用同一输入能力；图片额度为零的纯文生
        # 模型不能承接上组尾帧，不能只在计数层忽略它而仍向 Worker 传递。
        return self.images > 0


def has_automatic_predecessor_frame_dependency(shot_group: object) -> bool:
    """判断自动组合是否为该组写入了严格前驱首帧依赖标记。"""

    reference_frames = getattr(shot_group, "reference_frames", None)
    return (
        isinstance(reference_frames, dict)
        and reference_frames.get(CONTINUATION_FROM_PREDECESSOR_KEY) is True
    )


def should_follow_direct_predecessor(
    shot_group: object,
    *,
    limits: ReferenceMediaLimits | None = None,
) -> bool:
    """判断当前模型下是否必须从同场景严格直接前驱取得首帧。

    显式续帧保持用户的 ``is_continuation`` 语义；自动组合的图生组则通过
    ``continuation_from_predecessor`` 标记进入相同依赖链，避免把用户开关
    强制改成 true。
    """

    effective_limits = limits or ReferenceMediaLimits()
    if not effective_limits.supports_first_frame:
        return False
    return bool(getattr(shot_group, "is_continuation", False)) or (
        effective_limits.requires_input_image
        and has_automatic_predecessor_frame_dependency(shot_group)
    )


class ReferenceMediaLimitExceeded(ValueError):
    """参考素材数量超过供应商统一上限。"""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


def reference_media_limits_for_provider(
    provider: str | None,
    model: str | None = None,
) -> ReferenceMediaLimits:
    """返回当前运行时供应商/模型真实可消费的参考媒体能力。

    仅对适配层已经明确传入上游 API 的能力开放额度。这样切换供应商后，
    工作台不会允许用户选择随后被 Worker 忽略的参考素材。
    """

    provider_key = str(provider or "").strip().lower()
    model_name = str(model or "").strip().lower()
    if provider_key in REFERENCE_VIDEO_SUPPORTED_PROVIDERS:
        return ReferenceMediaLimits()
    if provider_key == "mgrouter":
        # AICC 的首帧是互斥输入：存在首帧时不能同时提交普通图或声音；
        # 无首帧时参考声音还必须至少搭配一张普通参考图。
        return ReferenceMediaLimits(
            images=9,
            audios=3,
            videos=0,
            total=12,
            first_frame_exclusive=True,
            audio_requires_reference_image=True,
        )
    if provider_key == "gemini":
        # Veo 使用一张输入首帧和最多三张额外参考图，不支持音频/视频参考。
        return ReferenceMediaLimits(images=4, audios=0, videos=0, total=4)
    if provider_key == "openai":
        # Sora 适配层当前只提交一张 input_reference。
        return ReferenceMediaLimits(images=1, audios=0, videos=0, total=1)
    if provider_key == "bailian":
        # 百炼仅 I2V 模型会消费首帧；其中 Wan 2.5/2.6/2.7 可额外消费一段驱动音频。
        image_supported = model_name.startswith(
            (
                "wan2.7-i2v",
                "wan2.5-i2v",
                "wan2.6-i2v",
                "wanx2.1-i2v",
                "wanx2.1-kf2v",
                "wan2.2-i2v",
                "happyhorse",
            )
        )
        audio_supported = model_name.startswith(("wan2.7-i2v", "wan2.5-i2v", "wan2.6-i2v"))
        # 万相 3.0 全能参考模式支持 10 张参考图、5 段参考音频、5 段参考视频，
        # 合计最多 20 个。显式首帧/尾帧与这些参考媒体互斥。
        if model_name.startswith("wan3.0-video"):
            return ReferenceMediaLimits(
                images=10,
                audios=5,
                videos=5,
                total=20,
                first_frame_exclusive=True,
                requires_input_image=False,
            )
        images = 1 if image_supported else 0
        audios = 1 if audio_supported else 0
        return ReferenceMediaLimits(
            images=images,
            audios=audios,
            videos=0,
            total=images + audios,
            requires_input_image=image_supported,
        )
    # 未知或未适配的输入能力一律关闭，避免静默丢弃用户选择。
    return ReferenceMediaLimits(images=0, audios=0, videos=0, total=0)


def supports_reference_videos(provider: str | None, model: str | None = None) -> bool:
    """当前运行时供应商是否能把参考视频真实传给模型。"""

    return reference_media_limits_for_provider(provider, model).videos > 0


def effective_reference_media_limits_for_group(
    limits: ReferenceMediaLimits | None = None,
    *,
    reserve_first_frame: bool = False,
) -> ReferenceMediaLimits:
    """返回镜头组实际可编辑、可提交的参考素材额度。

    一般供应商的首帧只占用图片和总数名额；MGRouter 则使用独占首帧输入槽。
    因此同一运行时模型下，不同镜头组也可能拥有不同的有效额度。
    """

    effective_limits = limits or ReferenceMediaLimits()
    if not (
        reserve_first_frame
        and effective_limits.supports_first_frame
        and effective_limits.first_frame_exclusive
    ):
        return effective_limits
    return replace(
        effective_limits,
        images=CONTINUATION_FIRST_FRAME_IMAGES,
        audios=0,
        videos=0,
        total=CONTINUATION_FIRST_FRAME_IMAGES,
    )


def public_reference_media_limits(
    limits: ReferenceMediaLimits | None = None,
    *,
    supports_videos: bool | None = None,
) -> dict[str, int | bool]:
    """返回可直接下发给前端的固定限额，不暴露供应商内部配置。"""

    effective_limits = limits or ReferenceMediaLimits()
    videos = effective_limits.videos
    if supports_videos is False:
        videos = 0

    return {
        "images": effective_limits.images,
        "audios": effective_limits.audios,
        "videos": videos,
        "total": effective_limits.total,
        # 自动首尾帧衔接已停用，工作台不再为它保留可选素材名额。
        "continuation_first_frame_images": 0,
        "audio_requires_reference_image": effective_limits.audio_requires_reference_image,
        "requires_input_image": effective_limits.requires_input_image,
    }


def validate_reference_media_counts(
    counts: ReferenceMediaCounts,
    *,
    limits: ReferenceMediaLimits | None = None,
) -> None:
    """校验实际会传入供应商的媒体数量。

    ``first_frames`` 代表续帧或首帧约束，必须同时占用图片和总数配额。
    """

    violations = reference_media_limit_violations(counts, limits=limits)
    if violations:
        raise violations[0]


def reference_media_limit_violations(
    counts: ReferenceMediaCounts,
    *,
    limits: ReferenceMediaLimits | None = None,
) -> list[ReferenceMediaLimitExceeded]:
    """返回全部命中的限额问题，供提交前检查接口逐项展示。"""

    effective_limits = limits or ReferenceMediaLimits()
    effective_images = counts.images + counts.first_frames
    first_frame_hint = "（含承接上一组尾帧）" if counts.first_frames else ""
    violations: list[ReferenceMediaLimitExceeded] = []
    if effective_images > effective_limits.images:
        violations.append(
            ReferenceMediaLimitExceeded(
                "reference_images_limit_exceeded",
                f"参考图最多支持 {effective_limits.images} 个{first_frame_hint}，当前为 {effective_images} 个",
            )
        )
    if counts.audios > effective_limits.audios:
        violations.append(
            ReferenceMediaLimitExceeded(
                "reference_audios_limit_exceeded",
                f"参考声音最多支持 {effective_limits.audios} 个，当前为 {counts.audios} 个",
            )
        )
    if counts.videos > effective_limits.videos:
        violations.append(
            ReferenceMediaLimitExceeded(
                "reference_videos_limit_exceeded",
                f"参考视频最多支持 {effective_limits.videos} 个，当前为 {counts.videos} 个",
            )
        )
    if counts.total > effective_limits.total:
        violations.append(
            ReferenceMediaLimitExceeded(
                "reference_media_total_limit_exceeded",
                "参考素材总数最多支持 "
                f"{effective_limits.total} 个（图片、声音和视频{first_frame_hint}），当前为 {counts.total} 个",
            )
        )
    if effective_limits.requires_input_image and counts.available_input_images == 0:
        violations.append(
            ReferenceMediaLimitExceeded(
                "reference_input_image_required",
                "当前图生视频模型至少需要 1 张参考图片或首帧",
            )
        )
    if (
        counts.audios
        and effective_limits.audios > 0
        and effective_limits.audio_requires_reference_image
        and not counts.images
    ):
        violations.append(
            ReferenceMediaLimitExceeded(
                "reference_audio_requires_reference_image",
                "当前视频模型使用参考声音时，至少需要 1 张普通参考图片",
            )
        )
    return violations


def count_material_references(materials: object) -> tuple[int, int]:
    """统计归一化 ``materials`` 中的图片和视频引用数量。"""

    if not isinstance(materials, dict):
        return 0, 0
    image_count = sum(
        len(items)
        for key in IMAGE_MATERIAL_KEYS
        if isinstance((items := materials.get(key)), list)
    )
    video_items = materials.get(VIDEO_MATERIAL_KEY)
    video_count = len(video_items) if isinstance(video_items, list) else 0
    return image_count, video_count


def count_audio_references(audio_elements: object) -> int:
    """统计归一化 ``audio_elements`` 中的声音引用数量。"""

    if isinstance(audio_elements, dict):
        return sum(
            len(items)
            for key in ("characters", "sounds")
            if isinstance((items := audio_elements.get(key)), list)
        )
    return len(audio_elements) if isinstance(audio_elements, list) else 0


def validate_shot_group_reference_selection(
    materials: object,
    audio_elements: object,
    *,
    reserve_continuation_first_frame: bool,
    available_first_frames: int | None = None,
    limits: ReferenceMediaLimits | None = None,
) -> ReferenceMediaCounts:
    """校验编辑后的镜头组引用，续帧预留一张首帧配额。"""

    images, videos = count_material_references(materials)
    counts = ReferenceMediaCounts(
        images=images,
        audios=count_audio_references(audio_elements),
        videos=videos,
        first_frames=CONTINUATION_FIRST_FRAME_IMAGES if reserve_continuation_first_frame else 0,
        available_first_frames=available_first_frames,
    )
    validate_reference_media_counts(
        counts,
        limits=effective_reference_media_limits_for_group(
            limits,
            reserve_first_frame=reserve_continuation_first_frame,
        ),
    )
    return counts


def _entry_identity(item: object, *, fallback_index: int) -> str:
    """为自动合并裁剪保留稳定去重键，兼容历史字符串引用。"""

    if isinstance(item, dict):
        for key in ("video_job_id", "asset_entry_id", "subject_id", "ref", "name"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return f"{key}:{value.strip()}"
    if isinstance(item, str) and item.strip():
        return f"name:{item.strip()}"
    return f"index:{fallback_index}"


def trim_automatic_reference_selection(
    materials: object,
    audio_elements: object,
    *,
    reserve_continuation_first_frame: bool = False,
    limits: ReferenceMediaLimits | None = None,
) -> tuple[dict[str, list[Any]], dict[str, list[Any]]]:
    """对自动组合产物按稳定顺序裁剪，使其必定满足供应商限额。

    自动生成的镜头组没有用户主动选择语义，因此保留图片、声音、视频的原有顺序，
    在总数冲突时按图片、声音、视频的优先级收敛。手工编辑不会调用本函数，而是
    返回明确的 400 错误，避免静默丢失用户选择。
    """

    source_materials = materials if isinstance(materials, dict) else {}
    source_audio = audio_elements if isinstance(audio_elements, dict) else {}
    trimmed_materials: dict[str, list[Any]] = {
        key: [] for key in (*IMAGE_MATERIAL_KEYS, VIDEO_MATERIAL_KEY)
    }
    trimmed_audio: dict[str, list[Any]] = {"characters": [], "sounds": []}
    effective_limits = effective_reference_media_limits_for_group(
        limits,
        reserve_first_frame=reserve_continuation_first_frame,
    )
    reserve_first_frame = (
        reserve_continuation_first_frame and effective_limits.supports_first_frame
    )
    total = CONTINUATION_FIRST_FRAME_IMAGES if reserve_first_frame else 0

    image_budget = max(0, effective_limits.images - total)
    seen_images: set[str] = set()
    for key in IMAGE_MATERIAL_KEYS:
        values = source_materials.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            identity = _entry_identity(item, fallback_index=len(seen_images))
            if identity in seen_images:
                continue
            if len(seen_images) >= image_budget or total >= effective_limits.total:
                continue
            seen_images.add(identity)
            trimmed_materials[key].append(item)
            total += 1

    seen_audio: set[str] = set()
    # MGRouter 的声音引用必须搭配至少一张普通参考图；首帧不算普通图。
    # 自动组合不能保留一个随后必定被编辑/创建接口拒绝的声音引用。
    may_keep_audio = not effective_limits.audio_requires_reference_image or bool(seen_images)
    if may_keep_audio:
        for key in ("characters", "sounds"):
            values = source_audio.get(key)
            if not isinstance(values, list):
                continue
            for item in values:
                identity = _entry_identity(item, fallback_index=len(seen_audio))
                if identity in seen_audio:
                    continue
                if len(seen_audio) >= effective_limits.audios or total >= effective_limits.total:
                    continue
                seen_audio.add(identity)
                trimmed_audio[key].append(item)
                total += 1

    seen_videos: set[str] = set()
    values = source_materials.get(VIDEO_MATERIAL_KEY)
    if isinstance(values, list):
        for item in values:
            identity = _entry_identity(item, fallback_index=len(seen_videos))
            if identity in seen_videos:
                continue
            if len(seen_videos) >= effective_limits.videos or total >= effective_limits.total:
                continue
            seen_videos.add(identity)
            trimmed_materials[VIDEO_MATERIAL_KEY].append(item)
            total += 1

    return trimmed_materials, trimmed_audio


__all__ = [
    "CONTINUATION_FIRST_FRAME_IMAGES",
    "IMAGE_MATERIAL_KEYS",
    "MAX_REFERENCE_AUDIOS",
    "MAX_REFERENCE_IMAGES",
    "MAX_REFERENCE_MEDIA",
    "MAX_REFERENCE_VIDEOS",
    "ReferenceMediaCounts",
    "ReferenceMediaLimits",
    "ReferenceMediaLimitExceeded",
    "REFERENCE_VIDEO_SUPPORTED_PROVIDERS",
    "count_audio_references",
    "count_material_references",
    "effective_reference_media_limits_for_group",
    "public_reference_media_limits",
    "reference_media_limits_for_provider",
    "reference_media_limit_violations",
    "supports_reference_videos",
    "trim_automatic_reference_selection",
    "validate_reference_media_counts",
    "validate_shot_group_reference_selection",
]
