"""视频生成供应商抽象与工厂。

统一覆盖火山方舟 Seedance、阿里百炼万相、OpenAI Sora 和 Google Gemini Veo；
未知厂商立即失败，避免静默切换后产生错误计费。
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from app.core.config import settings


@dataclass(frozen=True)
class VideoGenerationRequest:
    prompt: str
    reference_images: list[dict]
    reference_image_names: list[str]
    reference_audios: list[dict]
    reference_audio_names: list[str]
    model: str
    job_id: str
    first_frame: str | None = None
    duration_seconds: int | None = None
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True)
class VideoGenerationResult:
    video_storage_key: str
    provider_job_id: str | None = None
    metrics: dict = field(default_factory=dict)


@dataclass(frozen=True)
class VideoGenerationProgress:
    stage: str
    message: str
    progress: int
    provider_task_id: str | None = None


ProgressCallback = Callable[[VideoGenerationProgress], None]


@runtime_checkable
class VideoGenProvider(Protocol):
    """图生视频供应商协议 — 多参考图 + 参考音频 → 视频 storage_key。"""

    @property
    def provider_key(self) -> str:
        """供应商标识，如 jianyu / modelsai。"""
        ...

    async def generate(
        self,
        request: VideoGenerationRequest,
        on_progress: ProgressCallback | None = None,
    ) -> VideoGenerationResult:
        """使用 typed 请求执行视频生成。"""
        ...

    async def generate_all_reference(
        self,
        prompt: str,
        reference_images: list[dict],
        reference_audios: list[dict],
        model: str,
        *,
        first_frame: str | None = None,
        duration_seconds: int | None = None,
        job_id: str | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> str:
        """多参考图图生视频。

        Args:
            prompt: 镜头组 prompt（含镜头分时段 + 角色 + 叙事目的 + 约束）。
            reference_images: 参考图列表，元素 {kind:'storage', storage_key, mime_type, size_bytes, sha256}。
            reference_audios: 参考音频列表（结构同参考图，可空）。
            model: 视频模型，如 doubao-seedance-2-0-260128。
            first_frame: 首帧参考图 storage_key（上一镜头组尾帧，用于链式衔接，可空）。
            duration_seconds: 期望视频时长（秒），转发到 provider 请求体，可空。
            job_id: 调用方视频任务 ID，供 provider 拼 storage_key 追溯，可空。
            on_progress: 可选进度回调 (stage, message, progress 0-100)。

        Returns:
            生成视频的 storage_key（存入对象存储后的路径）。
        """
        ...


def get_video_provider(provider_key: str | None = None) -> VideoGenProvider:
    """按 provider_key 获取视频生成供应商实例。

    路由策略（.env 主导 + 别名兼容）：
    - jianyu / modelsai / volcengine / doubao / seedance -> 豆包 Seedance (volcengine)
    - bailian / wanx / wan2 / qwen-video -> 阿里百炼通义万相视频
    - openai / sora -> OpenAI Sora
    - gemini / google / veo -> Google Gemini Veo
    - 未传 provider_key -> 按 settings.video_gen_provider 选厂商
    - 未识别 -> 明确报错，不静默回退
    缺少对应密钥时实例可创建，但调用 generate 时抛 RuntimeError。
    """
    key = (provider_key or settings.video_gen_provider or "").strip().lower()
    # 1) 已知别名
    if key in ("jianyu", "modelsai", "volcengine", "doubao", "seedance"):
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        return VolcengineVideoGenProvider(provider_key=key or settings.video_default_provider)
    if key in ("bailian", "wanx", "wan2", "qwen-video"):
        from app.providers.video_gen_bailian import BailianVideoGenProvider

        return BailianVideoGenProvider(provider_key=key or "bailian")
    if key in ("openai", "sora"):
        from app.providers.video_gen_openai import OpenAIVideoGenProvider

        return OpenAIVideoGenProvider(provider_key=key or "openai")
    if key in ("gemini", "google", "veo"):
        from app.providers.video_gen_google import GoogleVideoGenProvider

        return GoogleVideoGenProvider(provider_key=key)

    # 未识别的非空 key 必须失败，不能静默调用并产生其他厂商费用。
    from app.providers.registry import require_capability

    require_capability(key, "video")
    raise AssertionError(f"遗漏的视频 provider 实现: {key}")
