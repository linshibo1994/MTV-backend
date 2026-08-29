"""运行时模型配置与视频审核厂商门禁测试。"""

from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from app.services.runtime_models import (
    RuntimeModel,
    RuntimeVideoModel,
    resolve_env_video_model,
    resolve_request_video_model,
    resolve_video_model,
)


@pytest.mark.asyncio
async def test_resolve_request_video_model_overrides_everything() -> None:
    """显式请求 provider/model 优先于 database active 与 env 默认。"""
    from app.core.config import Settings

    configured = Settings(
        _env_file=None,
        video_provider_selection_mode="env",
        video_gen_provider="volcengine",
        video_gen_model="env-model-must-not-win",
    )

    resolved = await resolve_request_video_model(
        SimpleNamespace(),
        configured,
        provider="dashscope",
        model="wan3.0-video-prime",
    )

    assert resolved == RuntimeVideoModel(
        provider="bailian",
        model="wan3.0-video-prime",
        reference_image_audit_required=False,
        task_provider_key="bailian",
    )


@pytest.mark.asyncio
async def test_resolve_request_video_model_falls_back_to_vendor_default() -> None:
    """model 缺省时使用该厂商的 .env 保底模型。"""
    from app.core.config import Settings

    configured = Settings(
        _env_file=None,
        dashscope_video_model="wanx2.1-t2v-turbo",
    )

    resolved = await resolve_request_video_model(
        SimpleNamespace(),
        configured,
        provider="bailian",
    )

    assert resolved.provider == "bailian"
    assert resolved.model == "wanx2.1-t2v-turbo"
    assert resolved.task_provider_key == "bailian"


@pytest.mark.asyncio
async def test_resolve_request_video_model_normalizes_alias_to_stable_key() -> None:
    """别名（如 dashscope/aicc）规范化为主 key，保证数据库与 worker 可路由。"""
    from app.core.config import Settings

    configured = Settings(_env_file=None)

    bailian = await resolve_request_video_model(
        SimpleNamespace(), configured, provider="dashscope", model="wan-custom"
    )
    assert bailian.task_provider_key == "bailian"

    mgrouter = await resolve_request_video_model(
        SimpleNamespace(), configured, provider="aicc"
    )
    assert mgrouter.task_provider_key == "mgrouter"
    assert mgrouter.provider == "mgrouter"

    volcengine = await resolve_request_video_model(
        SimpleNamespace(), configured, provider="doubao"
    )
    assert volcengine.task_provider_key == "volcengine"
    assert volcengine.provider == "volcengine"
    assert volcengine.reference_image_audit_required is True


@pytest.mark.asyncio
async def test_resolve_request_video_model_rejects_non_video_vendor() -> None:
    """不支持视频能力的厂商被能力校验拒绝。"""
    from app.core.config import Settings
    from app.providers.registry import UnsupportedCapabilityError

    configured = Settings(_env_file=None)

    with pytest.raises(UnsupportedCapabilityError):
        await resolve_request_video_model(
            SimpleNamespace(), configured, provider="deepseek", model="x"
        )


@pytest.mark.asyncio
async def test_resolve_request_video_model_empty_falls_back_to_default(
    monkeypatch,
) -> None:
    """provider/model 均缺省时回退服务端默认解析。"""
    from app.services import runtime_models

    monkeypatch.setattr(
        runtime_models.video_crud,
        "get_active_provider",
        AsyncMock(return_value=SimpleNamespace(provider_key="bailian", model="wan2.6-i2v")),
    )
    configured = SimpleNamespace(
        video_provider_selection_mode="database",
        video_default_provider="modelsai",
        video_gen_model="",
        seedance_model="seedance-fallback",
    )

    resolved = await resolve_request_video_model(
        SimpleNamespace(), configured, provider=None, model=None
    )

    assert resolved.provider == "bailian"


@pytest.mark.asyncio
async def test_resolve_video_model_uses_database_active_provider_and_normalizes_alias(monkeypatch) -> None:
    """database 模式必须与实际任务一样使用 active 配置，而不是 VIDEO_GEN_PROVIDER。"""
    from app.services import runtime_models

    get_active_provider = AsyncMock(
        return_value=SimpleNamespace(provider_key="modelsai", model="doubao-seedance-test")
    )
    monkeypatch.setattr(runtime_models.video_crud, "get_active_provider", get_active_provider)
    configured = SimpleNamespace(
        video_provider_selection_mode="database",
        video_default_provider="bailian",
        video_gen_model="env-model-must-not-win",
        seedance_model="seedance-fallback",
    )

    resolved = await resolve_video_model(SimpleNamespace(), configured)

    assert resolved == RuntimeVideoModel(
        provider="volcengine",
        model="doubao-seedance-test",
        reference_image_audit_required=True,
        task_provider_key="modelsai",
    )
    get_active_provider.assert_awaited_once_with(ANY, "all_reference")


@pytest.mark.asyncio
async def test_resolve_video_model_skips_audit_for_bailian(monkeypatch) -> None:
    from app.services import runtime_models

    monkeypatch.setattr(
        runtime_models.video_crud,
        "get_active_provider",
        AsyncMock(return_value=SimpleNamespace(provider_key="bailian", model="wan2.6-i2v")),
    )
    configured = SimpleNamespace(
        video_provider_selection_mode="database",
        video_default_provider="modelsai",
        video_gen_model="",
        seedance_model="seedance-fallback",
    )

    resolved = await resolve_video_model(SimpleNamespace(), configured)

    assert resolved.provider == "bailian"
    assert resolved.model == "wan2.6-i2v"
    assert resolved.reference_image_audit_required is False


def test_resolve_env_video_model_normalizes_aicc_without_volcengine_audit() -> None:
    from app.core.config import Settings

    configured = Settings(
        _env_file=None,
        video_provider_selection_mode="env",
        video_gen_provider="aicc",
        mgrouter_api_key="test-key",
        video_gen_model="",
    )

    resolved = resolve_env_video_model(configured)

    assert resolved == RuntimeVideoModel(
        provider="mgrouter",
        model="AICC-Doubao-Seedance-2.0",
        reference_image_audit_required=False,
        task_provider_key="mgrouter",
    )


def test_resolve_env_video_model_uses_neolink_seedance_without_volcengine_audit() -> None:
    from app.core.config import Settings

    configured = Settings(
        _env_file=None,
        video_provider_selection_mode="env",
        video_gen_provider="neolink",
        neolink_api_key="test-key",
        neolink_base_url="https://neolink.com/api",
        video_gen_model="AICC-Doubao-Seedance-2.0",
    )

    resolved = resolve_env_video_model(configured)

    assert resolved == RuntimeVideoModel(
        provider="neolink",
        model="doubao-seedance-2-0-260128",
        reference_image_audit_required=False,
        task_provider_key="neolink",
    )


@pytest.mark.asyncio
async def test_runtime_models_api_returns_only_public_model_metadata(client, monkeypatch) -> None:
    from app.api.v1 import runtime_models as api
    from app.core.auth import get_current_user

    async def override_user():
        return SimpleNamespace(user_id="user-1", enabled=True, is_admin=False)

    monkeypatch.setattr(api, "resolve_text_llm_model", lambda: RuntimeModel("bailian", "qwen-plus"))
    monkeypatch.setattr(api, "resolve_chat_llm_model", lambda: RuntimeModel("anthropic", "claude-test"))
    monkeypatch.setattr(api, "resolve_image_gen_model", lambda: RuntimeModel("bailian", "wan2.1-t2i"))
    monkeypatch.setattr(
        api,
        "resolve_video_model",
        AsyncMock(
            return_value=RuntimeVideoModel(
                "volcengine", "doubao-seedance-test", True, "volcengine"
            )
        ),
    )
    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user

    response = await client.get("/v3api/runtime-models")

    assert response.status_code == 200
    assert response.json() == {
        "text_llm": {"provider": "bailian", "model": "qwen-plus"},
        "chat_llm": {"provider": "anthropic", "model": "claude-test"},
        "image_gen": {"provider": "bailian", "model": "wan2.1-t2i"},
        "video_gen": {
            "provider": "volcengine",
            "model": "doubao-seedance-test",
            "reference_image_audit_required": True,
            "reference_video_supported": True,
            "reference_video_limit": 3,
        },
    }


@pytest.mark.asyncio
async def test_runtime_models_api_exposes_zero_video_reference_limit_for_bailian(
    client,
    monkeypatch,
) -> None:
    from app.api.v1 import runtime_models as api
    from app.core.auth import get_current_user

    async def override_user():
        return SimpleNamespace(user_id="user-1", enabled=True, is_admin=False)

    monkeypatch.setattr(api, "resolve_text_llm_model", lambda: RuntimeModel("bailian", "qwen-plus"))
    monkeypatch.setattr(api, "resolve_chat_llm_model", lambda: RuntimeModel("anthropic", "claude-test"))
    monkeypatch.setattr(api, "resolve_image_gen_model", lambda: RuntimeModel("bailian", "wan2.1-t2i"))
    monkeypatch.setattr(
        api,
        "resolve_video_model",
        AsyncMock(
            return_value=RuntimeVideoModel(
                "bailian", "wan2.6-i2v", False, "bailian"
            )
        ),
    )
    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user

    response = await client.get("/v3api/runtime-models")

    assert response.status_code == 200
    assert response.json()["video_gen"] == {
        "provider": "bailian",
        "model": "wan2.6-i2v",
        "reference_image_audit_required": False,
        "reference_video_supported": False,
        "reference_video_limit": 0,
    }
