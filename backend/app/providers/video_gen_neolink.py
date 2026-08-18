"""NeoLink Seedance 视频生成供应商实现。"""

from __future__ import annotations

import logging
from urllib.parse import urlsplit, urlunsplit

from app.core.config import settings
from app.providers.registry import ProviderConfigurationError
from app.providers.video_gen import (
    VideoCancellationResult,
    VideoGenerationRequest,
    VideoGenerationResult,
    unsupported_video_cancellation,
)
from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

_NEOLINK_TASK_STATUSES = frozenset({"queued", "running", "succeeded", "failed", "expired"})
_REFERENCE_URL_EXPIRY_SECONDS = 900
_DEFAULT_RATIO = "16:9"

logger = logging.getLogger(__name__)


class NeolinkVideoGenProvider(VolcengineVideoGenProvider):
    """通过 NeoLink 的 Ark 兼容内容生成接口调用 Seedance。"""

    def __init__(self) -> None:
        super().__init__(provider_key="neolink")

    @staticmethod
    def _uses_dedicated_config() -> bool:
        return settings.is_explicitly_configured(
            "neolink_api_key"
        ) or settings.is_explicitly_configured("neolink_base_url")

    def _resolve_ark_api_key(self) -> str:
        if self._uses_dedicated_config():
            return settings.neolink_api_key
        if settings.is_explicitly_configured("video_gen_api_key") and settings.video_gen_api_key:
            return settings.video_gen_api_key
        return settings.neolink_api_key

    def _resolve_ark_base_url(self) -> str:
        configured = settings.neolink_base_url
        if (
            not self._uses_dedicated_config()
            and settings.is_explicitly_configured("video_gen_base_url")
            and settings.video_gen_base_url
        ):
            configured = settings.video_gen_base_url
        return _normalize_neolink_ark_base_url(configured)

    def _check_keys(self) -> None:
        if not self._resolve_ark_api_key().strip():
            raise ProviderConfigurationError(
                "缺少 NeoLink API Key（NEOLINK_API_KEY 或 VIDEO_GEN_API_KEY），"
                "无法调用 Seedance 视频生成"
            )

    async def cancel(self, provider_task_id: str) -> VideoCancellationResult:
        """NeoLink 未公开远端取消契约，保留本地取消和对账语义。"""
        return unsupported_video_cancellation(self.provider_key, provider_task_id)

    async def _cancel_remote_task(self, _tasks, provider_task_id: str) -> VideoCancellationResult:
        return unsupported_video_cancellation(self.provider_key, provider_task_id)

    async def _encode_storage_refs(
        self,
        refs: list[dict],
        default_mime: str,
        *,
        budget: list[int] | None = None,
    ) -> list[str]:
        """NeoLink 仅接收其服务端可抓取的短期 HTTPS 媒体 URL。"""
        from app.providers.storage import get_storage

        del default_mime, budget
        if not refs:
            return []
        storage = get_storage()
        urls: list[str] = []
        for ref in refs:
            storage_key = ref.get("storage_key") if isinstance(ref, dict) else None
            if not isinstance(storage_key, str) or not storage_key:
                raise ProviderConfigurationError("NeoLink 参考媒体缺少 storage_key")
            urls.append(await self._external_reference_url(storage, storage_key))
        return urls

    async def _encode_single(
        self,
        storage_key: str,
        *,
        budget: list[int] | None = None,
    ) -> str:
        """把首帧转换为 NeoLink 可抓取的短期 HTTPS URL。"""
        from app.providers.storage import get_storage

        del budget
        return await self._external_reference_url(get_storage(), storage_key)

    @staticmethod
    async def _external_reference_url(storage, storage_key: str) -> str:
        from app.providers._url_safety import validate_public_http_url

        try:
            url = await storage.external_read_url(
                storage_key,
                expires_seconds=_REFERENCE_URL_EXPIRY_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 - 存储错误统一转换为配置错误
            logger.warning(
                "NeoLink 参考媒体外部读取 URL 生成失败 error_type=%s",
                type(exc).__name__,
            )
            raise ProviderConfigurationError("NeoLink 参考媒体外部读取 URL 生成失败") from None
        if not isinstance(url, str) or not url.strip():
            raise ProviderConfigurationError(
                "当前存储后端无法为 NeoLink 参考媒体提供外部 HTTPS URL"
            )

        value = url.strip()
        try:
            parsed = urlsplit(value)
            port = parsed.port or 443
            validate_public_http_url(value)
        except (TypeError, ValueError):
            raise ProviderConfigurationError(
                "NeoLink 参考媒体外部读取 URL 必须是绝对 HTTPS URL"
            ) from None
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or port != 443
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ProviderConfigurationError(
                "NeoLink 参考媒体外部读取 URL 必须是绝对 HTTPS URL"
            )
        return value

    def _build_create_kwargs(
        self,
        model: str,
        prompt: str,
        image_urls: list[str],
        audio_urls: list[str],
        first_frame_url: str | None,
        duration_seconds: int | None,
        video_urls: list[str] | None = None,
    ) -> dict:
        """按 NeoLink 已验证的内容生成契约构造创建参数。"""
        create_kwargs: dict = {
            "model": model,
            "content": self._build_content(
                prompt,
                image_urls,
                audio_urls,
                first_frame_url,
                video_urls,
            ),
            "generate_audio": True,
            "ratio": _DEFAULT_RATIO,
            "watermark": False,
        }
        if duration_seconds and duration_seconds > 0:
            create_kwargs["duration"] = duration_seconds
        return create_kwargs

    @staticmethod
    def _extract_status(resp) -> str:
        status = VolcengineVideoGenProvider._extract_status(resp)
        if status not in _NEOLINK_TASK_STATUSES:
            raise RuntimeError("neolink_video_status_unknown")
        return status

    async def generate(
        self,
        request: VideoGenerationRequest,
        on_progress=None,
    ) -> VideoGenerationResult:
        result = await super().generate(request, on_progress)
        metrics = dict(result.metrics)
        provider_status = metrics.pop("volcengine_status", None)
        if provider_status is not None:
            metrics["neolink_status"] = provider_status
        return VideoGenerationResult(
            video_storage_key=result.video_storage_key,
            provider_job_id=result.provider_job_id,
            metrics=metrics,
        )


def _normalize_neolink_ark_base_url(value: str) -> str:
    """把文档给出的 /api 基址规范化为 Ark SDK 所需的 /api/v1。"""
    try:
        parsed = urlsplit(value.strip())
        port = parsed.port
    except ValueError as exc:
        raise ProviderConfigurationError("NEOLINK_BASE_URL 格式无效") from exc

    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ProviderConfigurationError(
            "NEOLINK_BASE_URL 必须是不含用户信息、查询或片段的 HTTPS 443 地址"
        )

    path = parsed.path.rstrip("/")
    if path == "/api":
        path = "/api/v1"
    elif path != "/api/v1":
        raise ProviderConfigurationError("NEOLINK_BASE_URL 必须以 /api 或 /api/v1 结尾")
    return urlunsplit(("https", parsed.netloc, path, "", ""))
