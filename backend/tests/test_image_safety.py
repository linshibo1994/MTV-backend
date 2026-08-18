"""图片内容安全拒绝归一化与确定性软化策略测试。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.providers.image_gen import (
    ImageSafetyCategory,
    ImageSafetyRejectedError,
    PromptSoftenLevel,
    classify_image_safety_code,
    soften_prompt,
)


@pytest.mark.parametrize(
    ("code", "category"),
    [
        ("content_filter", ImageSafetyCategory.SAFETY),
        ("PROHIBITED_CONTENT", ImageSafetyCategory.PROHIBITED_CONTENT),
        ("DataInspectionFailed", ImageSafetyCategory.SAFETY),
        ("Input.DataInspectionFailed", ImageSafetyCategory.SAFETY),
        ("InputImageSensitiveContentDetected.PrivacyInformation", ImageSafetyCategory.PRIVACY),
    ],
)
def test_classify_image_safety_code_uses_only_stable_codes(
    code: str, category: ImageSafetyCategory
) -> None:
    result = classify_image_safety_code(code)

    assert result is not None
    assert result[0] is category


@pytest.mark.parametrize(
    "value",
    [
        "invalid_api_key",
        "insufficient_quota",
        "rate_limit_exceeded",
        "service_unavailable",
        "bad_request",
        "the message says content_filter but this is free text",
        429,
        None,
    ],
)
def test_classify_image_safety_code_does_not_swallow_operational_errors(value: object) -> None:
    assert classify_image_safety_code(value) is None


def test_soften_prompt_is_deterministic_and_preserves_placeholders() -> None:
    source = "{鲜血_character_ref}在{{scene_name}}中遭遇鲜血淋漓的搏斗，镜头为<camera>"

    gentle = soften_prompt(source, level=PromptSoftenLevel.GENTLE)
    symbolic = soften_prompt(gentle.prompt, level=PromptSoftenLevel.SYMBOLIC)

    assert gentle.strategy_version == "image-safety-soften.v1"
    assert gentle.prompt == soften_prompt(source, level="gentle").prompt
    assert "克制的受伤痕迹" in gentle.prompt
    assert "剪影化、非接触式的动作对峙" in symbolic.prompt
    for token in ("{鲜血_character_ref}", "{{scene_name}}", "<camera>"):
        assert token in symbolic.prompt

    strongest_from_source = soften_prompt(source, level=PromptSoftenLevel.SYMBOLIC)
    assert strongest_from_source.prompt == symbolic.prompt


def test_soften_prompt_reports_no_change_without_known_trigger() -> None:
    result = soften_prompt("阳光下的纸飞机", level="conservative")

    assert result.changed is False
    assert result.prompt == "阳光下的纸飞机"


@pytest.mark.asyncio
async def test_openai_provider_normalizes_explicit_content_filter(monkeypatch) -> None:
    from httpx import Request, Response
    from openai import BadRequestError

    from app.providers import image_gen_openai
    from app.providers.image_gen_openai import OpenAIImageGenProvider

    configured = SimpleNamespace(
        image_gen_provider="openai",
        image_gen_api_key="image-key",
        openai_api_key="",
        image_gen_model="gpt-image-2",
        openai_image_model="gpt-image-2",
        image_gen_base_url="",
        is_explicitly_configured=lambda name: name in {"image_gen_provider", "image_gen_api_key"},
    )
    monkeypatch.setattr(image_gen_openai, "settings", configured)
    response = Response(
        400,
        request=Request("POST", "https://api.openai.com/v1/images/generations"),
        json={"error": {"code": "content_filter", "message": "PRIVATE_BODY"}},
    )
    create = AsyncMock(
        side_effect=BadRequestError(
            "PRIVATE_BODY",
            response=response,
            body={"error": {"code": "content_filter", "message": "PRIVATE_BODY"}},
        )
    )
    close = AsyncMock()
    monkeypatch.setattr(
        image_gen_openai,
        "AsyncOpenAI",
        lambda **_kwargs: SimpleNamespace(images=SimpleNamespace(generate=create), close=close),
    )

    with pytest.raises(ImageSafetyRejectedError) as exc_info:
        await OpenAIImageGenProvider().generate("用户私有提示", "gpt-image-2", "1k", "1:1")

    assert exc_info.value.provider == "openai"
    assert exc_info.value.code == "content_filter"
    assert "PRIVATE_BODY" not in str(exc_info.value)
    close.assert_awaited_once()


@pytest.mark.asyncio
async def test_openai_provider_normalizes_content_filter_finish_reason(monkeypatch) -> None:
    from openai import ContentFilterFinishReasonError

    from app.providers import image_gen_openai
    from app.providers.image_gen_openai import OpenAIImageGenProvider

    configured = SimpleNamespace(
        image_gen_provider="openai",
        image_gen_api_key="image-key",
        openai_api_key="",
        image_gen_model="gpt-image-2",
        openai_image_model="gpt-image-2",
        image_gen_base_url="",
        is_explicitly_configured=lambda name: name in {"image_gen_provider", "image_gen_api_key"},
    )
    monkeypatch.setattr(image_gen_openai, "settings", configured)
    monkeypatch.setattr(
        image_gen_openai,
        "AsyncOpenAI",
        lambda **_kwargs: SimpleNamespace(
            images=SimpleNamespace(
                generate=AsyncMock(side_effect=ContentFilterFinishReasonError())
            ),
            close=AsyncMock(),
        ),
    )

    with pytest.raises(ImageSafetyRejectedError, match="content_filter"):
        await OpenAIImageGenProvider().generate("提示", "gpt-image-2", "1k", "1:1")


@pytest.mark.asyncio
async def test_openai_provider_keeps_auth_error_outside_safety_path(monkeypatch) -> None:
    from httpx import Request, Response
    from openai import AuthenticationError

    from app.providers import image_gen_openai
    from app.providers.image_gen_openai import OpenAIImageGenProvider

    configured = SimpleNamespace(
        image_gen_provider="openai",
        image_gen_api_key="image-key",
        openai_api_key="",
        image_gen_model="gpt-image-2",
        openai_image_model="gpt-image-2",
        image_gen_base_url="",
        is_explicitly_configured=lambda name: name in {"image_gen_provider", "image_gen_api_key"},
    )
    monkeypatch.setattr(image_gen_openai, "settings", configured)
    auth_error = AuthenticationError(
        "PRIVATE_AUTH_BODY",
        response=Response(
            401,
            request=Request("POST", "https://api.openai.com/v1/images/generations"),
        ),
        body={"error": {"code": "invalid_api_key"}},
    )
    monkeypatch.setattr(
        image_gen_openai,
        "AsyncOpenAI",
        lambda **_kwargs: SimpleNamespace(
            images=SimpleNamespace(generate=AsyncMock(side_effect=auth_error)),
            close=AsyncMock(),
        ),
    )

    with pytest.raises(AuthenticationError):
        await OpenAIImageGenProvider().generate("提示", "gpt-image-2", "1k", "1:1")


@pytest.mark.asyncio
async def test_google_provider_normalizes_explicit_safety_finish_reason() -> None:
    from app.providers.image_gen_google import GoogleImageGenProvider

    response = SimpleNamespace(
        prompt_feedback=None,
        candidates=[SimpleNamespace(finish_reason="IMAGE_SAFETY", content=None)],
    )
    client = SimpleNamespace(
        aio=SimpleNamespace(
            models=SimpleNamespace(generate_content=AsyncMock(return_value=response))
        )
    )

    with pytest.raises(ImageSafetyRejectedError) as exc_info:
        await GoogleImageGenProvider()._generate_one(client, "提示", "1:1", "1K", "model")

    assert exc_info.value.provider == "gemini"
    assert exc_info.value.category is ImageSafetyCategory.IMAGE_SAFETY


def test_bailian_classifier_ignores_status_and_uses_structured_code_only() -> None:
    from app.providers.image_gen_bailian import (
        _classify_response_safety,
        _raise_if_safety_rejected,
    )

    _raise_if_safety_rejected({"code": "InvalidApiKey", "message": "content_filter"})
    _raise_if_safety_rejected({"code": "Throttling", "message": "safety"})

    class AuthResponse:
        status_code = 401

        @staticmethod
        def json():
            return {"code": "DataInspectionFailed"}

    _classify_response_safety(AuthResponse())

    with pytest.raises(ImageSafetyRejectedError) as exc_info:
        _raise_if_safety_rejected(
            {"output": {"code": "Input.DataInspectionFailed", "message": "PRIVATE_BODY"}}
        )
    assert exc_info.value.provider == "bailian"
    assert "PRIVATE_BODY" not in str(exc_info.value)
