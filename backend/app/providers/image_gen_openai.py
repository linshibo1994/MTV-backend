"""gpt-image-2 文生图 Provider — OpenAI GPT Image 系列。

通过 openai.AsyncOpenAI.images.generate 生成图片，取 b64_json 解码为 bytes。
GPT image 模型恒返回 b64_json（无 url 回退）。size 档位用 1536x1024 / 1024x1536 /
1024x1024（跨 gpt-image-1/1.5/2 均 canonical 安全，旧 1792x1024 仅 gpt-image-2 可用）。

缺 image_gen_api_key（回退 openai_api_key）时抛 RuntimeError，由 worker 捕获后 release 积分。
"""

from __future__ import annotations

import base64
from typing import Literal

from openai import AsyncOpenAI

from app.core.config import settings
from app.providers.image_gen import ImageResult

OpenAIImageSize = Literal[
    "1024x1024",
    "1536x1024",
    "1024x1536",
    "auto",
]


def _map_size(size: str, aspect: str) -> OpenAIImageSize:
    """将画布尺寸/比例映射为 GPT image canonical size 档位。

    跨 gpt-image-1/1.5/2 通用；aspect 决定方向：
    - 9:16 -> 1024x1536（纵版）
    - 1:1  -> 1024x1024（方版）
    - 16:9 及其它 -> 1536x1024（横版，对应 06 §3.1 横版契约）
    """
    if aspect == "9:16":
        return "1024x1536"
    if aspect == "1:1":
        return "1024x1024"
    return "1536x1024"


class OpenAIImageGenProvider:
    """GPT image 系列文生图实现。

    利用 images.generate(n=count) 一次请求返回多张图片（多张选一场景减少往返）。
    """

    async def generate(
        self, prompt: str, model: str, size: str, aspect: str, count: int = 1
    ) -> list[ImageResult]:
        api_key = settings.image_gen_api_key or settings.openai_api_key
        if not api_key:
            raise RuntimeError("未配置图片生成 API key，无法调用 GPT image 文生图")
        use_model = model or settings.image_gen_model or settings.openai_image_model
        base_url = settings.image_gen_base_url or None

        client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        openai_size = _map_size(size, aspect)
        n = max(count, 1)
        resp = await client.images.generate(
            model=use_model,
            prompt=prompt,
            size=openai_size,
            n=n,
        )

        image_data = None if resp is None else resp.data
        if not image_data:
            raise RuntimeError("GPT image 未返回任何图片数据")

        results: list[ImageResult] = []
        for item in image_data:
            # GPT image 系列恒返回 b64_json
            b64 = getattr(item, "b64_json", None)
            if b64:
                results.append(
                    ImageResult(data=base64.b64decode(b64), mime_type="image/png")
                )
                continue
            url = getattr(item, "url", None)
            if url:
                # dall-e 系列可能返回 url；按既有安全策略禁用 URL 回退（防 SSRF）
                raise RuntimeError("GPT image URL 回退已禁用")
            raise RuntimeError("GPT image 返回内容中未找到图片数据 (b64_json/url)")

        if not results:
            raise RuntimeError("GPT image 未返回任何图片数据")
        return results
