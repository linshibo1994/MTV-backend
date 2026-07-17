"""Google Gemini 图片生成 Provider（兼容旧 banana-2 别名）。

通过 google-genai SDK 的 generate_content 接口，开启 IMAGE 输出模态生成图片。
模型优先读取 IMAGE_GEN_MODEL；Key 和网关同样优先读取图片能力级配置。
缺 Key 时抛 RuntimeError，由 worker 捕获后 release 积分（不卡死队列）。
"""

from __future__ import annotations

from google import genai
from google.genai import types as gtypes

from app.core.config import settings
from app.providers.image_gen import ImageResult
from app.providers.registry import ProviderConfigurationError


class GoogleImageGenProvider:
    """banana-2 -> Google Nano Banana 文生图实现。

    Nano Banana 单次 generate_content 仅返回一张图片，多张场景在内部循环调用
    （Google 路径无法利用 n 参数批量化，count 张即 count 次往返）。
    """

    async def _generate_one(
        self, client: genai.Client, prompt: str, aspect: str, model: str
    ) -> ImageResult:
        """单次调用 Nano Banana 生成一张图片。"""
        # 开启图像输出模态，Nano Banana 据此返回 inline 图片数据。
        # 通过 image_config.aspect_ratio 传入比例（06 §3.1 契约要求 16:9 等），
        # Nano Banana 目前不支持固定像素尺寸（如 2k），尺寸由模型侧控制，
        # 输出分辨率默认 2k 量级，此处不做强制（契约保真度由模型保证）。
        image_config = gtypes.ImageConfig(aspect_ratio=aspect or None)
        config = gtypes.GenerateContentConfig(
            response_modalities=["IMAGE"],
            image_config=image_config,
        )
        response = await client.aio.models.generate_content(
            model=model,
            contents=prompt,
            config=config,
        )

        # 从候选内容中提取首个图片 part 的二进制数据；可空响应 fail closed。
        candidates = getattr(response, "candidates", None)
        if not candidates:
            raise RuntimeError("Nano Banana 返回内容为空，无法解析图片")
        content = getattr(candidates[0], "content", None)
        parts = getattr(content, "parts", None)
        if not parts:
            raise RuntimeError("Nano Banana 返回内容为空，无法解析图片")

        for part in parts:
            inline = getattr(part, "inline_data", None)
            if inline and getattr(inline, "data", None):
                # inline.data 已为 bytes（google-genai SDK 约定）
                data = inline.data
                if isinstance(data, str):
                    # 兼容部分版本返回 base64 字符串的情况
                    import base64

                    return ImageResult(
                        data=base64.b64decode(data), mime_type="image/png"
                    )
                return ImageResult(data=data, mime_type="image/png")  # type: ignore[arg-type]

        raise RuntimeError("Nano Banana 返回内容中未找到图片数据 (inline_data)")

    async def generate(
        self, prompt: str, model: str, size: str, aspect: str, count: int = 1
    ) -> list[ImageResult]:
        api_key = (
            settings.image_gen_api_key
            if settings.is_explicitly_configured("image_gen_api_key")
            and settings.image_gen_api_key
            else settings.gemini_api_key or settings.google_api_key
        )
        if not api_key:
            raise ProviderConfigurationError(
                "未配置 IMAGE_GEN_API_KEY（或 GOOGLE_API_KEY/GEMINI_API_KEY），"
                "无法调用 Gemini 图片生成"
            )
        use_model = (
            settings.image_gen_model
            if (
                settings.is_explicitly_configured("image_gen_provider")
                or settings.is_explicitly_configured("image_gen_model")
            )
            and settings.image_gen_model
            else (
                model
                if model and model != "banana-2"
                else settings.google_image_model
            )
        )
        http_options = (
            gtypes.HttpOptions(base_url=settings.image_gen_base_url.rstrip("/"))
            if settings.is_explicitly_configured("image_gen_base_url")
            and settings.image_gen_base_url
            else None
        )

        client = genai.Client(api_key=api_key, http_options=http_options)
        # Nano Banana 单次仅返回一张，count>1 时循环调用
        results: list[ImageResult] = []
        try:
            for _ in range(max(count, 1)):
                data = await self._generate_one(client, prompt, aspect, use_model)
                results.append(data)
        finally:
            await client.aio.aclose()
        return results
