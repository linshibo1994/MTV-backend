"""LLM JSON 安全兜底链专项测试。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.providers.llm import LLMJSONResponseError, parse_json_object


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ('```json\n{"name": "阿明"}\n```', {"name": "阿明"}),
        ('｛“name”：“阿明”，“tags”：［“角色”，］｝', {"name": "阿明", "tags": ["角色"]}),
        ("{name: '阿明', active: True,}", {"name": "阿明", "active": True}),
        ("Result(name='阿明', count=2)", {"name": "阿明", "count": 2}),
    ],
)
def test_parse_json_object_accepts_supported_safe_fallbacks(raw: str, expected: dict) -> None:
    assert parse_json_object(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "Result(__import__('os').system('false'))",
        "Result(value=(lambda: 1)())",
        "Result(*[1])",
        "Result(**{'value': 1})",
        "module.Result(value=1)",
        "_Private(value=1)",
        "{'value': float('nan')}",
    ],
)
def test_parse_json_object_rejects_executable_or_ambiguous_python(raw: str) -> None:
    with pytest.raises(LLMJSONResponseError) as exc_info:
        parse_json_object(raw)
    assert exc_info.value.diagnostic["category"] == "invalid_json"
    assert exc_info.value.repair_attempted is True
    assert raw not in str(exc_info.value)
    assert raw not in repr(exc_info.value.diagnostic)


def test_parse_json_object_preserves_non_object_contract() -> None:
    with pytest.raises(LLMJSONResponseError) as exc_info:
        parse_json_object("[1, 2, 3]")
    assert exc_info.value.diagnostic == {
        "version": 1,
        "category": "non_object_json",
        "response": {
            "kind": "list",
            "length_bucket": "1_128",
            "top_level_kind": "list",
        },
    }


def test_parse_json_object_error_only_records_safe_repair_shape() -> None:
    private_body = "PRIVATE_PROVIDER_BODY_824f"
    with pytest.raises(LLMJSONResponseError) as exc_info:
        parse_json_object(private_body, kind="tool_input")
    assert exc_info.value.repair_attempted is True
    assert exc_info.value.diagnostic == {
        "version": 1,
        "category": "invalid_json",
        "response": {
            "kind": "tool_input",
            "length_bucket": "1_128",
            "top_level_kind": "invalid_json",
        },
    }
    assert private_body not in str(exc_info.value)
    assert private_body not in repr(exc_info.value.diagnostic)


@pytest.mark.asyncio
async def test_openai_compatible_provider_uses_shared_repair_chain() -> None:
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "test"
    provider._vendor = "openai_compatible"
    provider._client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=AsyncMock(
                    return_value=SimpleNamespace(
                        choices=[
                            SimpleNamespace(
                                message=SimpleNamespace(content="Result(value=1)")
                            )
                        ]
                    )
                )
            )
        )
    )

    assert await provider.complete_json("system", "user") == {"value": 1}


@pytest.mark.asyncio
async def test_anthropic_skips_invalid_text_then_repairs_next_text(monkeypatch) -> None:
    from app.providers.llm_anthropic import AnthropicLLMProvider

    provider = AnthropicLLMProvider(api_key="key", model="test")
    monkeypatch.setattr(
        provider,
        "_create",
        AsyncMock(
            return_value={
                "content": [
                    {"type": "text", "text": "not-json"},
                    {"type": "text", "text": "｛“value”：1，｝"},
                ]
            }
        ),
    )

    assert await provider.complete_json("system", "user") == {"value": 1}


@pytest.mark.asyncio
async def test_anthropic_preserves_non_object_error_instead_of_skipping(monkeypatch) -> None:
    from app.providers.llm_anthropic import AnthropicLLMProvider

    provider = AnthropicLLMProvider(api_key="key", model="test")
    monkeypatch.setattr(
        provider,
        "_create",
        AsyncMock(return_value={"content": [{"type": "text", "text": "[1]"}]}),
    )

    with pytest.raises(LLMJSONResponseError) as exc_info:
        await provider.complete_json("system", "user")
    assert exc_info.value.diagnostic["category"] == "non_object_json"
