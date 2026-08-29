"""请求级 LLM 供应商+模型动态路由与保底模型回退测试。"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.core.config import Settings


def _settings(**kwargs: Any) -> Settings:
    """构造不读取真实 .env 的确定性测试配置。"""
    return Settings(_env_file=None, **kwargs)  # type: ignore[call-arg]


@pytest.fixture(autouse=True)
def _reset_provider_cache():
    from app.providers import reset_llm_provider_cache

    reset_llm_provider_cache()
    yield
    reset_llm_provider_cache()


@pytest.fixture
def _bailian_settings(monkeypatch):
    """配置百炼凭证并替换全局 settings，供动态路由解析。"""
    from app.core import config

    configured = _settings(dashscope_api_key="dashscope-test-key")
    monkeypatch.setattr(config, "settings", configured)
    return configured


class _FakeAsyncOpenAI:
    """记录构造参数的 AsyncOpenAI 替身，不发起真实网络。"""

    instances: list["_FakeAsyncOpenAI"] = []

    def __init__(self, **kwargs: Any) -> None:
        self.config = kwargs
        _FakeAsyncOpenAI.instances.append(self)


@pytest.fixture
def _fake_openai_client(monkeypatch):
    _FakeAsyncOpenAI.instances = []
    from app.providers import llm_openai_compat

    monkeypatch.setattr(llm_openai_compat, "AsyncOpenAI", _FakeAsyncOpenAI)
    yield _FakeAsyncOpenAI
    _FakeAsyncOpenAI.instances = []


def test_get_llm_provider_dynamic_construction_and_cache(
    _bailian_settings, _fake_openai_client
) -> None:
    """显式 (provider, model) 动态构造实例；同参数命中缓存复用客户端。"""
    from app.providers import get_llm_provider
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    first = get_llm_provider("bailian", "qwen-max")
    second = get_llm_provider("bailian", "qwen-max")
    other_model = get_llm_provider("bailian", "qwen-plus")

    assert isinstance(first, OpenAICompatLLMProvider)
    assert first._model == "qwen-max"
    assert first._base_url == "https://dashscope.aliyuncs.com/compatible-mode/v1"
    # 相同 (厂商, 模型, 凭证) 命中缓存，不重建客户端。
    assert first is second
    # 不同模型独立实例。
    assert other_model is not first
    assert other_model._model == "qwen-plus"
    assert len(_fake_openai_client.instances) == 2


def test_get_llm_provider_model_falls_back_to_vendor_default(
    _bailian_settings, _fake_openai_client
) -> None:
    """model 缺省时使用厂商 .env 保底模型（百炼为 qwen-plus）。"""
    from app.providers import get_llm_provider

    provider = get_llm_provider("bailian")

    assert provider._model == "qwen-plus"


def test_get_llm_provider_without_provider_keeps_singleton(
    monkeypatch, _fake_openai_client
) -> None:
    """未传 provider 时保持服务端默认单例链（settings.text_llm_*）。"""
    from app.core import config
    from app.providers import get_llm_provider

    configured = _settings(
        text_llm_provider="bailian",
        text_llm_api_key="text-key",
        text_llm_model="kimi/custom-model",
        text_llm_base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    monkeypatch.setattr(config, "settings", configured)

    provider = get_llm_provider()
    assert provider._model == "kimi/custom-model"


def test_resolve_provider_credentials_reads_vendor_dedicated_fields(
    monkeypatch,
) -> None:
    """各厂商凭证从专属 env 字段解析，含 base_url 与保底模型。"""
    from app.core import config
    from app.providers import resolve_provider_credentials

    configured = _settings(
        deepseek_api_key="deepseek-key",
        anthropic_api_key="anthropic-key",
        gemini_api_key="gemini-key",
        neolink_api_key="neolink-key",
        neolink_base_url="https://neolink.com/api",
        dashscope_api_key="dashscope-key",
    )
    monkeypatch.setattr(config, "settings", configured)

    assert resolve_provider_credentials("deepseek") == (
        "deepseek-key",
        "https://api.deepseek.com",
        "deepseek-v4-flash",
    )
    assert resolve_provider_credentials("claude") == (
        "anthropic-key",
        "https://api.anthropic.com",
        "claude-sonnet-5",
    )
    assert resolve_provider_credentials("gemini") == (
        "gemini-key",
        "",
        "gemini-2.5-flash",
    )
    assert resolve_provider_credentials("neolink") == (
        "neolink-key",
        "https://neolink.com/api",
        "qwen-turbo",
    )
    assert resolve_provider_credentials("dashscope") == (
        "dashscope-key",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "qwen-plus",
    )


def test_resolve_provider_credentials_missing_key_rejected(monkeypatch) -> None:
    """厂商 Key 缺失时抛 ProviderConfigurationError，不静默换厂商。"""
    from app.core import config
    from app.providers import resolve_provider_credentials
    from app.providers.registry import ProviderConfigurationError

    monkeypatch.setattr(config, "settings", _settings(dashscope_api_key=""))

    with pytest.raises(ProviderConfigurationError, match="未配置 API Key"):
        resolve_provider_credentials("bailian")


def test_resolve_provider_credentials_rejects_non_llm_vendor() -> None:
    """不支持 LLM 能力的厂商（如 volcengine）被能力校验拒绝。"""
    from app.providers import resolve_provider_credentials
    from app.providers.registry import UnsupportedCapabilityError

    with pytest.raises(UnsupportedCapabilityError):
        resolve_provider_credentials("volcengine")


class _StubLLMProvider:
    """记录调用并可编程抛错的 LLM 替身。"""

    def __init__(self, *, model: str, error: Exception | None = None) -> None:
        self.model = model
        self.error = error
        self.call_count = 0

    async def complete_json(
        self,
        system: str,
        user: str,
        *,
        response_schema: dict | None = None,
    ) -> dict:
        self.call_count += 1
        if self.error is not None:
            raise self.error
        return {"model": self.model}

    async def chat(
        self,
        messages: list[dict],
        *,
        system: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        self.call_count += 1
        if self.error is not None:
            raise self.error
        return f"reply-from-{self.model}"


class _FakeStatusError(RuntimeError):
    """携带 status_code 属性的 SDK 风格异常替身。"""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"provider error status={status_code}")
        self.status_code = status_code


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("LLM 供应商调用失败 status=404"),
        RuntimeError("LLM 供应商调用失败 status=403"),
        RuntimeError("LLM 供应商调用失败 status=400"),
        RuntimeError("LLM 供应商调用失败 status=422"),
        _FakeStatusError(404),
    ],
    ids=["404-runtime", "403-runtime", "400-runtime", "422-runtime", "status-attr"],
)
async def test_fallback_provider_retries_on_model_unavailable(error) -> None:
    """模型不可用类 4xx（含百炼 403）触发同厂商保底模型重试。"""
    from app.providers import FallbackLLMProvider

    primary = _StubLLMProvider(model="qwen3.8-max-prime", error=error)
    fallback = _StubLLMProvider(model="qwen-plus")

    wrapper = FallbackLLMProvider(primary, fallback)  # type: ignore[arg-type]
    result = await wrapper.complete_json("system json", "user")

    assert result == {"model": "qwen-plus"}
    assert primary.call_count == 1
    assert fallback.call_count == 1


@pytest.mark.asyncio
async def test_fallback_provider_does_not_retry_transient_errors() -> None:
    """瞬态网络错误（ConnectionError）不换模型，原样上抛交给上层重试。"""
    from app.providers import FallbackLLMProvider

    primary = _StubLLMProvider(model="qwen3.8-max-prime", error=ConnectionError("LLM 供应商暂不可用"))
    fallback = _StubLLMProvider(model="qwen-plus")

    wrapper = FallbackLLMProvider(primary, fallback)  # type: ignore[arg-type]
    with pytest.raises(ConnectionError):
        await wrapper.complete_json("system json", "user")

    assert primary.call_count == 1
    assert fallback.call_count == 0


@pytest.mark.asyncio
async def test_fallback_provider_does_not_retry_auth_errors() -> None:
    """认证失败（401）不属于模型不可用，不触发换模型重试。"""
    from app.providers import FallbackLLMProvider

    primary = _StubLLMProvider(
        model="qwen3.8-max-prime", error=RuntimeError("LLM 供应商调用失败 status=401")
    )
    fallback = _StubLLMProvider(model="qwen-plus")

    wrapper = FallbackLLMProvider(primary, fallback)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="status=401"):
        await wrapper.chat([{"role": "user", "content": "hi"}])

    assert primary.call_count == 1
    assert fallback.call_count == 0


@pytest.mark.asyncio
async def test_fallback_provider_without_fallback_raises_original() -> None:
    """fallback 为 None 时保持 primary 原始错误。"""
    from app.providers import FallbackLLMProvider

    primary = _StubLLMProvider(
        model="qwen3.8-max-prime", error=RuntimeError("LLM 供应商调用失败 status=404")
    )
    wrapper = FallbackLLMProvider(primary, None)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="status=404"):
        await wrapper.complete_json("system json", "user")


@pytest.mark.asyncio
async def test_fallback_provider_surfaces_fallback_failure() -> None:
    """保底模型也失败时如实上抛第二个错误，不吞掉失败事实。"""
    from app.providers import FallbackLLMProvider

    primary = _StubLLMProvider(
        model="qwen3.8-max-prime", error=RuntimeError("LLM 供应商调用失败 status=404")
    )
    fallback = _StubLLMProvider(
        model="qwen-plus", error=RuntimeError("LLM 供应商调用失败 status=403")
    )
    wrapper = FallbackLLMProvider(primary, fallback)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="status=403"):
        await wrapper.complete_json("system json", "user")


def test_build_llm_with_fallback_skips_wrapper_for_default_model(
    _bailian_settings, _fake_openai_client
) -> None:
    """model 为空或等于保底模型时直接返回 primary，不做同模型双请求。"""
    from app.providers import FallbackLLMProvider, build_llm_with_fallback
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    no_model = build_llm_with_fallback("bailian", None)
    same_as_default = build_llm_with_fallback("bailian", "qwen-plus")

    assert isinstance(no_model, OpenAICompatLLMProvider)
    assert not isinstance(no_model, FallbackLLMProvider)
    assert isinstance(same_as_default, OpenAICompatLLMProvider)
    assert not isinstance(same_as_default, FallbackLLMProvider)


def test_build_llm_with_fallback_wraps_explicit_model(
    _bailian_settings, _fake_openai_client
) -> None:
    """显式模型与保底模型不同时包装回退层，两实例模型正确。"""
    from app.providers import FallbackLLMProvider, build_llm_with_fallback

    wrapper = build_llm_with_fallback("bailian", "qwen3.8-max-prime")

    assert isinstance(wrapper, FallbackLLMProvider)
    assert wrapper._primary._model == "qwen3.8-max-prime"  # type: ignore[attr-defined]
    assert wrapper.fallback_provider is not None
    assert wrapper.fallback_provider._model == "qwen-plus"  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_pipeline_context_routes_llm_by_selection(monkeypatch) -> None:
    """contextvar 设置请求级选择后，get_llm_from_context 返回动态实例。"""
    from app.core import config
    from app.providers import FallbackLLMProvider
    from app.services.pipeline_context import (
        LLMSelection,
        get_llm_from_context,
        llm_selection_scope,
    )

    configured = _settings(
        dashscope_api_key="dashscope-test-key",
        text_llm_provider="bailian",
        text_llm_api_key="text-key",
        text_llm_model="qwen-turbo",
    )
    monkeypatch.setattr(config, "settings", configured)
    from app.providers import llm_openai_compat

    monkeypatch.setattr(
        llm_openai_compat, "AsyncOpenAI", lambda **kwargs: SimpleNamespace(config=kwargs)
    )

    # 无选择：回退服务端默认单例。
    default_provider = get_llm_from_context()
    with llm_selection_scope(LLMSelection(provider="bailian", model="qwen-max")):
        dynamic_provider = get_llm_from_context()
        # 显式模型与保底模型（qwen-plus）不同，得到带回退的包装实例。
        assert isinstance(dynamic_provider, FallbackLLMProvider)
        assert dynamic_provider._primary._model == "qwen-max"  # type: ignore[attr-defined]
        assert dynamic_provider.fallback_provider is not None
        assert dynamic_provider.fallback_provider._model == "qwen-plus"  # type: ignore[attr-defined]

    # 作用域退出后恢复默认，不影响后续调用。
    assert get_llm_from_context() is default_provider


@pytest.mark.asyncio
async def test_pipeline_context_model_without_provider_uses_default(
    monkeypatch,
) -> None:
    """只传 model 未传 provider 时保持服务端默认，避免误发给默认厂商。"""
    from app.core import config
    from app.services.pipeline_context import (
        LLMSelection,
        get_llm_from_context,
        llm_selection_scope,
    )

    configured = _settings(
        dashscope_api_key="dashscope-test-key",
        text_llm_provider="bailian",
        text_llm_api_key="text-key",
        text_llm_model="qwen-turbo",
    )
    monkeypatch.setattr(config, "settings", configured)
    from app.providers import llm_openai_compat

    monkeypatch.setattr(
        llm_openai_compat, "AsyncOpenAI", lambda **kwargs: SimpleNamespace(config=kwargs)
    )

    default_provider = get_llm_from_context()
    with llm_selection_scope(LLMSelection(provider=None, model="qwen-max")):
        assert get_llm_from_context() is default_provider


def test_dynamic_provider_cache_is_bounded(_bailian_settings, _fake_openai_client) -> None:
    """动态实例缓存有上限，避免长驻进程无限增长。"""
    from app.providers import _DYNAMIC_CACHE_MAX_ENTRIES, _dynamic_provider_cache
    from app.providers import get_llm_provider

    for index in range(_DYNAMIC_CACHE_MAX_ENTRIES + 5):
        get_llm_provider("bailian", f"model-{index}")

    assert len(_dynamic_provider_cache) <= _DYNAMIC_CACHE_MAX_ENTRIES


@pytest.mark.asyncio
async def test_storyboard_generate_storyboard_sets_contextvar(monkeypatch) -> None:
    """generate_storyboard 的 llm_provider/llm_model 参数进入 contextvar 作用域。"""
    from app.services import storyboard as storyboard_service
    from app.services.pipeline_context import LLMSelection, current_llm_selection

    captured: list = []

    async def spy_impl(db, script_id, episode_id, **kwargs):
        captured.append(current_llm_selection())
        return {"scenes": 0, "shot_groups": 0, "shots": 0}

    monkeypatch.setattr(storyboard_service, "_generate_storyboard_impl", spy_impl)

    await storyboard_service.generate_storyboard(
        SimpleNamespace(),
        "script-1",
        "episode-1",
        llm_provider="bailian",
        llm_model="qwen-max",
    )

    assert captured == [
        LLMSelection(provider="bailian", model="qwen-max"),
    ]
    # 作用域退出后不泄漏。
    assert current_llm_selection() is None


def test_scene_audition_queue_persists_llm_selection(monkeypatch) -> None:
    """queue_audition 把请求级 LLM 选择写入 outbox payload。"""
    import asyncio

    from app.services import scene_auditions as service

    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=2,
        client_save_seq=0,
        script_text="剧情",
    )

    class _Scalars:
        def __init__(self, values):
            self._values = values

        def all(self):
            return self._values

    class _Result:
        def __init__(self, scalar=None, scalars=None):
            self._scalar = scalar
            self._scalars = scalars or []

        def scalar_one_or_none(self):
            return self._scalar

        def scalars(self):
            return _Scalars(self._scalars)

    enqueue_payload: dict = {}

    async def fake_enqueue(db, *, task_type, job_id, attempt, payload):
        enqueue_payload.update(payload)
        return SimpleNamespace(id="dispatch-1")

    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(episode),  # episode 查询
                _Result(scalar=None),  # 既有 audition 查询
            ]
        ),
        add=lambda _: None,
        flush=AsyncMock(),
    )
    monkeypatch.setattr(
        "app.services.task_dispatch.enqueue_task", fake_enqueue
    )

    async def scenario():
        return await service.queue_audition(
            db,
            script_id="script-1",
            episode_id="episode-1",
            scene_no=1,
            expected_episode_version=2,
            scene_type="identity_reveal",
            llm_provider="bailian",
            llm_model="qwen-max",
        )

    asyncio.run(scenario())

    assert enqueue_payload["llm_provider"] == "bailian"
    assert enqueue_payload["llm_model"] == "qwen-max"
