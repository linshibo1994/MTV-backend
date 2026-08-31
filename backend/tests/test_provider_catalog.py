"""供应商目录 API 测试 — 结构、available 标记与密钥不泄漏。"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.config import Settings


def _settings(**kwargs) -> Settings:
    """构造不读取真实 .env 的确定性测试配置。"""
    return Settings(_env_file=None, **kwargs)  # type: ignore[call-arg]


def test_provider_catalog_returns_expected_structure(monkeypatch) -> None:
    """目录包含全部厂商，字段结构与能力标签完整。"""
    from app.core import config
    from app.services.provider_catalog import build_provider_catalog

    monkeypatch.setattr(config, "settings", _settings())
    catalog = build_provider_catalog()

    keys = {entry.key for entry in catalog.providers}
    assert keys == {
        "bailian",
        "deepseek",
        "volcengine",
        "neolink",
        "openai",
        "anthropic",
        "gemini",
        "mgrouter",
    }
    by_key = {entry.key: entry for entry in catalog.providers}
    # 能力标签与 registry 一致。
    assert set(by_key["bailian"].capabilities) == {"text", "chat", "image", "video"}
    assert set(by_key["deepseek"].capabilities) == {"text", "chat"}
    assert set(by_key["volcengine"].capabilities) == {"video"}
    # 推荐模型带中文 label 与能力分组。
    bailian_models = {model.id: model for model in by_key["bailian"].models}
    assert bailian_models["qwen-plus"].label == "通义千问 Plus"
    assert set(bailian_models["qwen-plus"].capabilities) == {"text", "chat"}
    assert set(bailian_models["wan2.1-t2i-plus"].capabilities) == {"image"}
    assert set(bailian_models["qwen-image-3.0"].capabilities) == {"image"}
    assert bailian_models["qwen-image-3.0"].label == "Qwen Image 3.0"
    assert set(bailian_models["qwen-image-3.0-pro"].capabilities) == {"image"}
    assert bailian_models["qwen-image-3.0-pro"].label == "Qwen Image 3.0 Pro"
    assert set(bailian_models["wan3.0-video-plus"].capabilities) == {"video"}
    # 未配置凭证时全部 unavailable。
    assert all(entry.available is False for entry in catalog.providers)


def test_provider_catalog_marks_available_and_default_model(monkeypatch) -> None:
    """已配置凭证的供应商标记 available；.env 保底模型加入目录并标 is_default。"""
    from app.core import config
    from app.services.provider_catalog import build_provider_catalog

    monkeypatch.setattr(
        config,
        "settings",
        _settings(
            dashscope_api_key="dashscope-key",
            deepseek_api_key="deepseek-key",
            text_llm_provider="bailian",
            text_llm_api_key="dashscope-key",
            text_llm_model="kimi/custom-model",
        ),
    )
    catalog = build_provider_catalog()
    by_key = {entry.key: entry for entry in catalog.providers}

    assert by_key["bailian"].available is True
    assert by_key["deepseek"].available is True
    assert by_key["openai"].available is False

    # .env 显式配置的 text_llm_model（厂商匹配 bailian 时回填链生效前提之外，
    # 这里直接验证目录把它标为 default）与 deepseek 默认模型。
    bailian_models = {model.id: model for model in by_key["bailian"].models}
    assert bailian_models["kimi/custom-model"].is_default is True
    assert by_key["bailian"].default_model is not None
    deepseek_models = {model.id: model for model in by_key["deepseek"].models}
    assert deepseek_models["deepseek-v4-flash"].is_default is True


def test_provider_catalog_env_default_model_merges_into_recommended(monkeypatch) -> None:
    """.env 保底模型与推荐清单重合时只更新标记，不产生重复条目。"""
    from app.core import config
    from app.services.provider_catalog import build_provider_catalog

    monkeypatch.setattr(
        config,
        "settings",
        _settings(
            deepseek_api_key="deepseek-key",
            deepseek_model="deepseek-v4-pro",
        ),
    )
    catalog = build_provider_catalog()
    by_key = {entry.key: entry for entry in catalog.providers}

    deepseek_ids = [model.id for model in by_key["deepseek"].models]
    assert deepseek_ids.count("deepseek-v4-pro") == 1
    assert by_key["deepseek"].default_model == "deepseek-v4-pro"


@pytest.mark.asyncio
async def test_provider_catalog_api_requires_login(client) -> None:
    """未登录访问目录返回 401。"""
    response = await client.get("/v3api/provider-catalog")

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_provider_catalog_api_returns_catalog_without_secrets(
    client, monkeypatch
) -> None:
    """登录后可读目录；响应不包含任何密钥或 base_url。"""
    from app.api.v1 import provider_catalog as api
    from app.core import config
    from app.core.auth import get_current_user

    async def override_user():
        return SimpleNamespace(user_id="user-1", enabled=True, is_admin=False)

    monkeypatch.setattr(
        config,
        "settings",
        _settings(
            dashscope_api_key="sk-super-secret-dashscope",
            deepseek_api_key="sk-super-secret-deepseek",
        ),
    )
    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user

    response = await client.get("/v3api/provider-catalog")

    assert response.status_code == 200
    payload = response.json()
    serialized = str(payload)
    assert "sk-super-secret-dashscope" not in serialized
    assert "sk-super-secret-deepseek" not in serialized
    assert "api_key" not in serialized
    assert "base_url" not in serialized
    by_key = {entry["key"]: entry for entry in payload["providers"]}
    assert by_key["bailian"]["available"] is True
    assert by_key["openai"]["available"] is False
    del api
