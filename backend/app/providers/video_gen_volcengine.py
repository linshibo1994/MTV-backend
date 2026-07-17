"""字节豆包 Seedance 2.0 视频生成供应商实现。

底层模型 doubao-seedance-2-0-260128 (Seedance 2.0)，使用火山方舟
``AsyncArk.content_generation.tasks`` 接口。jianyu / modelsai 两个 provider_key
路由到同一底层模型。

参考: PLAN 5.2 模型映射表。真实接入需 VOLCENGINE_ACCESS_KEY / VOLCENGINE_SECRET_KEY；
缺 key 或缺 SDK 时 generate_all_reference 抛 RuntimeError，由 worker 捕获后 release 积分，
不阻塞队列（PLAN 5.4 / 5.5）。

实现要点:
1. _check_keys: 校验 ak/sk，缺失抛 RuntimeError。
2. _submit_and_poll: 用 AsyncArk 提交内容生成任务并轮询至终态，成功后下载 video_url。
3. _store_video: 将视频字节经 storage.put 存入对象存储，返回 storage_key。

参考图、首帧和参考音频以 storage_key 形式传入，提交前经 storage.get 读取并编码为
data URL，避免要求私有对象存储暴露公网地址。
"""

import asyncio
import base64
import logging
import time

from app.core.config import settings
from app.providers.video_gen import (
    VideoGenerationProgress,
    VideoGenerationRequest,
    VideoGenerationResult,
)

logger = logging.getLogger(__name__)

# 轮询间隔（秒）
_POLL_INTERVAL = 5.0
# 轮询超时（秒），约 10 分钟
_POLL_TIMEOUT = 600


class VolcengineVideoGenProvider:
    """豆包 Seedance 2.0 (volcengine) 图生视频实现。

    通过 volcengine 视频生成 2.0 的 all_reference 接口提交多参考图任务，
    轮询任务完成，将视频下载后存入对象存储，返回 storage_key。

    提供 on_progress 回调驱动 worker 进度更新。
    """

    def __init__(self, *, provider_key: str = "modelsai") -> None:
        # jianyu | modelsai，仅标识不同，底层模型一致
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    def _check_keys(self) -> None:
        """校验 volcengine 密钥，缺失抛 RuntimeError（不阻塞队列）。"""
        if not settings.volcengine_access_key or not settings.volcengine_secret_key:
            raise RuntimeError(
                "缺少 volcengine 密钥（VOLCENGINE_ACCESS_KEY / VOLCENGINE_SECRET_KEY），"
                "无法调用豆包 Seedance 视频生成"
            )

    def _build_client(self):
        """构造火山方舟异步客户端。"""
        try:
            from volcenginesdkarkruntime import AsyncArk
        except ImportError as exc:  # pragma: no cover - 依赖缺失兜底
            raise RuntimeError(
                "缺少 volcenginesdkarkruntime，无法调用豆包 Seedance 视频生成"
            ) from exc

        return AsyncArk(
            ak=settings.volcengine_access_key,
            sk=settings.volcengine_secret_key,
            region=settings.volcengine_region,
        )

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
        on_progress=None,
    ) -> str:
        request = VideoGenerationRequest(
            prompt=prompt,
            reference_images=reference_images,
            reference_image_names=[],
            reference_audios=reference_audios,
            reference_audio_names=[],
            model=model,
            job_id=job_id or "unknown",
            first_frame=first_frame,
            duration_seconds=duration_seconds,
        )

        def _legacy_progress(progress: VideoGenerationProgress) -> None:
            if on_progress is not None:
                on_progress(progress.stage, progress.message, progress.progress)

        result = await self.generate(request, _legacy_progress if on_progress else None)
        return result.video_storage_key

    async def generate(
        self,
        request: VideoGenerationRequest,
        on_progress=None,
    ) -> VideoGenerationResult:
        """多参考图图生视频 → 返回视频 storage_key。

        流程:
        1. 校验密钥
        2. 构造请求参数（prompt + 参考图/首帧/参考音频 base64）
        3. 提交 volcengine 视频生成任务（异步）
        4. 轮询任务状态，回调进度
        5. 任务完成 → 下载视频字节 → 存入对象存储 → 返回 storage_key
        6. 失败抛 RuntimeError

        Args:
            model: 视频模型，默认 doubao-seedance-2-0-260128。
            first_frame: 首帧参考图 storage_key（上一组尾帧，链式衔接）。
            duration_seconds: 期望视频时长（秒），转发到 provider 请求体 duration 字段。
            job_id: 调用方视频任务 ID，用于拼 storage_key 便于追溯（缺失回退 task_id）。
            on_progress: 回调 (stage:str, message:str, progress:int)。
        """
        self._check_keys()
        use_model = request.model or settings.seedance_model

        def _notify(
            stage: str,
            message: str,
            progress: int,
            provider_task_id: str | None = None,
        ) -> None:
            if on_progress is not None:
                try:
                    on_progress(
                        VideoGenerationProgress(
                            stage=stage,
                            message=message,
                            progress=progress,
                            provider_task_id=provider_task_id,
                        )
                    )
                except Exception:  # noqa: BLE001 — 进度回调失败不应中断生成
                    logger.warning("视频生成进度回调失败", exc_info=True)

        # 提交阶段
        _notify("submitting", "正在提交视频生成任务", 10)

        # Ark 的 content 字段接收 URL；这里使用 data URL 承载私有存储中的媒体。
        image_urls = await self._encode_storage_refs(request.reference_images, "image/png")
        audio_urls = await self._encode_storage_refs(request.reference_audios, "audio/mpeg")
        first_frame_url: str | None = None
        if request.first_frame:
            first_frame_url = await self._encode_single(request.first_frame)

        # ===== volcengine SDK 调用 =====
        video_bytes, task_meta = await self._submit_and_poll(
            use_model, request.prompt, image_urls, audio_urls, first_frame_url, _notify,
            duration_seconds=request.duration_seconds,
            job_id=request.job_id,
            provider_task_id=request.metadata.get("provider_task_id"),
        )

        # 存入对象存储
        _notify("storing", "正在存储视频", 95)
        storage_key = await self._store_video(video_bytes, task_meta)

        _notify("succeeded", "视频生成完成", 100)
        return VideoGenerationResult(
            video_storage_key=storage_key,
            provider_job_id=task_meta.get("task_id"),
            metrics={
                "provider_key": self.provider_key,
                "task_id": task_meta.get("task_id"),
                "volcengine_status": task_meta.get("volcengine_status"),
            },
        )

    async def _encode_storage_refs(self, refs: list[dict], default_mime: str) -> list[str]:
        """批量读取参考媒体并编码为 data URL；任一项缺失时关闭失败。"""
        from app.providers.storage import get_storage

        if not refs:
            return []
        storage = get_storage()
        encoded: list[str] = []
        for ref in refs:
            key = ref.get("storage_key") if isinstance(ref, dict) else None
            if not key:
                continue
            try:
                data = await storage.get(key)
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError("reference_read_failed") from exc
            if not data:
                raise RuntimeError("reference_read_failed")
            mime_type = str(ref.get("mime_type") or default_mime).split(";", 1)[0].strip()
            if "/" not in mime_type:
                mime_type = default_mime
            encoded.append(self._as_data_url(data, mime_type))
        return encoded

    async def _encode_single(self, storage_key: str) -> str:
        """读取单个首帧并编码为图片 data URL。"""
        from app.providers.storage import get_storage

        try:
            data = await storage.get(storage_key) if (storage := get_storage()) else b""
        except Exception as exc:  # noqa: BLE001
            logger.warning("读取首帧参考失败 storage_key=%s", storage_key, exc_info=True)
            raise RuntimeError("reference_read_failed") from exc
        if not data:
            raise RuntimeError("reference_read_failed")
        return self._as_data_url(data, "image/png")

    @staticmethod
    def _as_data_url(data: bytes, mime_type: str) -> str:
        return f"data:{mime_type};base64,{base64.b64encode(data).decode('ascii')}"

    def _build_content(
        self,
        prompt: str,
        image_urls: list[str],
        audio_urls: list[str],
        first_frame_url: str | None,
    ) -> list[dict]:
        """构造 Ark 内容生成 API 的多模态 content。"""
        content: list[dict] = [{"type": "text", "text": prompt}]
        if first_frame_url:
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": first_frame_url},
                    "role": "first_frame",
                }
            )
        content.extend(
            {
                "type": "image_url",
                "image_url": {"url": url},
                "role": "reference_image",
            }
            for url in image_urls
        )
        content.extend(
            {
                "type": "audio_url",
                "audio_url": {"url": url},
                "role": "reference_audio",
            }
            for url in audio_urls
        )
        return content

    async def _submit_and_poll(
        self,
        model: str,
        prompt: str,
        image_urls: list[str],
        audio_urls: list[str],
        first_frame_url: str | None,
        _notify,
        *,
        duration_seconds: int | None = None,
        job_id: str | None = None,
        provider_task_id: str | None = None,
    ) -> tuple[bytes, dict]:
        """通过 Ark 提交视频任务并轮询至完成。"""
        client = self._build_client()
        tasks = client.content_generation.tasks
        try:
            task_id = provider_task_id
            if not task_id:
                create_kwargs: dict = {
                    "model": model,
                    "content": self._build_content(
                        prompt, image_urls, audio_urls, first_frame_url
                    ),
                    "generate_audio": bool(audio_urls),
                    "return_last_frame": True,
                }
                if duration_seconds and duration_seconds > 0:
                    create_kwargs["duration"] = duration_seconds
                submit_resp = await tasks.create(**create_kwargs)
                task_id = self._extract_task_id(submit_resp)
                if not task_id:
                    raise RuntimeError("volcengine 视频生成提交失败，未返回 task_id")
                _notify("submitted", "视频生成任务已提交", 15, task_id)

            _notify("polling", "视频生成中，轮询任务状态", 20, task_id)
            deadline = time.monotonic() + _POLL_TIMEOUT
            started_at = time.monotonic()
            last_progress = 20
            result_resp = None
            while True:
                result_resp = await tasks.get(task_id=task_id)
                status = self._extract_status(result_resp)
                if status in {"failed", "succeeded", "cancelled"}:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError(f"volcengine 视频生成轮询超时 task_id={task_id}")
                pct = min(90, 20 + int((time.monotonic() - started_at) / _POLL_TIMEOUT * 70))
                if pct > last_progress:
                    last_progress = pct
                    _notify("generating", "视频生成中", pct, task_id)
                await asyncio.sleep(_POLL_INTERVAL)

            status = self._extract_status(result_resp)
            if status != "succeeded":
                error = self._extract_error(result_resp)
                raise RuntimeError(
                    f"volcengine 视频生成失败 task_id={task_id} status={status} error={error}"
                )

            video_url = self._extract_video_url(result_resp)
            if not video_url:
                raise RuntimeError(
                    f"volcengine 视频生成完成但未返回 video_url task_id={task_id}"
                )

            _notify("downloading", "正在下载生成视频", 90, task_id)
            video_bytes = await self._download(video_url)
            return video_bytes, {
                "task_id": task_id,
                "volcengine_status": status,
                "video_url": video_url,
                "last_frame_url": self._extract_last_frame_url(result_resp),
                "job_id": job_id,
            }
        finally:
            await client.close()

    @staticmethod
    def _extract_task_id(resp) -> str:
        """从 Ark 提交响应中提取 task_id，兼容测试用 dict。"""
        direct = getattr(resp, "id", None)
        if direct:
            return str(direct)
        data = resp
        if not isinstance(data, dict):
            data = getattr(resp, "__dict__", {}) or {}
            # SDK 返回对象时尝试取 .data
            data = getattr(resp, "data", data) or {}
        if isinstance(data, dict):
            inner = data.get("data", data)
            if isinstance(inner, dict):
                return str(inner.get("task_id") or inner.get("id") or "")
        return ""

    @staticmethod
    def _extract_status(resp) -> str:
        """从 Ark 轮询响应中提取状态。"""
        direct = getattr(resp, "status", None)
        if direct:
            return str(direct).lower()
        data = resp
        if not isinstance(data, dict):
            data = getattr(resp, "data", {}) or {}
        if isinstance(data, dict):
            inner = data.get("data", data)
            if isinstance(inner, dict):
                return str(inner.get("status") or inner.get("code") or "").lower()
        return ""

    @staticmethod
    def _extract_video_url(resp) -> str:
        """从 Ark 轮询响应中提取 video_url。"""
        content_obj = getattr(resp, "content", None)
        direct = getattr(content_obj, "video_url", None)
        if direct:
            return str(direct)
        data = resp
        if not isinstance(data, dict):
            data = getattr(resp, "data", {}) or {}
        if isinstance(data, dict):
            inner = data.get("data", data)
            if isinstance(inner, dict):
                # 兼容 video_url / origin_video_url / resp_data
                url = inner.get("video_url") or inner.get("origin_video_url") or ""
                if not url:
                    # 部分响应把 URL 放在 content/resp_data 列表
                    content = inner.get("content") or inner.get("resp_data") or []
                    if isinstance(content, list) and content:
                        first = content[0]
                        if isinstance(first, dict):
                            url = first.get("video_url") or first.get("url") or ""
                return str(url or "")
        return ""

    @staticmethod
    def _extract_last_frame_url(resp) -> str:
        content = getattr(resp, "content", None)
        if content is not None:
            return str(getattr(content, "last_frame_url", "") or "")
        if isinstance(resp, dict):
            content = resp.get("content") or resp.get("data", {}).get("content")
            if isinstance(content, dict):
                return str(content.get("last_frame_url") or "")
        return ""

    @staticmethod
    def _extract_error(resp) -> str:
        error = getattr(resp, "error", None)
        if error is None and isinstance(resp, dict):
            error = resp.get("error") or resp.get("data", {}).get("error")
        if isinstance(error, dict):
            return str(error.get("message") or error.get("code") or "unknown")
        return str(getattr(error, "message", None) or getattr(error, "code", None) or error or "unknown")

    async def _download(self, url: str) -> bytes:
        """下载视频 URL 字节流，逐跳 SSRF 校验 + 流式大小限制。"""
        from app.providers._url_safety import safe_download_bytes

        return await safe_download_bytes(url, max_bytes=1024 * 1024 * 1024)

    async def _store_video(self, video_bytes: bytes, task_meta: dict) -> str:
        """将视频字节存入对象存储，返回 storage_key。

        走 app.providers.storage 抽象（T10 实现），storage_key 形如
        generate_images/video/<job_or_task>.mp4。
        """
        # 延迟 import，storage provider 未就绪时不阻塞模块加载
        from app.providers.storage import get_storage

        storage = get_storage()
        # 用 task_meta 中的标识拼 key；job_id 由调用方经 task_meta 传入更佳
        ident = task_meta.get("job_id") or task_meta.get("task_id") or "unknown"
        storage_key = f"generate_images/video/{ident}.mp4"
        await storage.put(storage_key, video_bytes)
        return storage_key
