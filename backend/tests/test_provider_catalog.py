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
        "entpilot",
    }
    by_key = {entry.key: entry for entry in catalog.providers}
    # 能力标签与 registry 一致。
    assert set(by_key["bailian"].capabilities) == {"text", "chat", "image", "video"}
    assert set(by_key["deepseek"].capabilities) == {"text", "chat"}
    assert set(by_key["volcengine"].capabilities) == {"video"}
    assert set(by_key["entpilot"].capabilities) == {"video"}
    entpilot_models = {model.id: model for model in by_key["entpilot"].models}
    assert set(entpilot_models) == {
        "seedance-2.0-1080p",
        "seedance-2.0-480p",
        "seedance-2.0-4k",
        "seedance-2.0-720p",
        "seedance-2.0-fast-480p",
        "seedance-2.0-fast-720p",
        "seedance-2.5-1080p",
        "seedance-2.5-480p",
        "seedance-2.5-720p",
    }
    assert by_key["entpilot"].label == "EntPilot Seedance"
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
            entpilot_api_key="entpilot-key",
            text_llm_provider="bailian",
            text_llm_api_key="dashscope-key",
            text_llm_model="kimi/custom-model",
            video_gen_provider="entpilot",
            video_gen_model="seedance-2.5-720p",
        ),
    )
    catalog = build_provider_catalog()
    by_key = {entry.key: entry for entry in catalog.providers}

    assert by_key["bailian"].available is True
    assert by_key["deepseek"].available is True
    assert by_key["entpilot"].available is True
    assert by_key["openai"].available is False

    # .env 显式配置的 text_llm_model（厂商匹配 bailian 时回填链生效前提之外，
    # 这里直接验证目录把它标为 default）与 deepseek 默认模型。
    bailian_models = {model.id: model for model in by_key["bailian"].models}
    assert bailian_models["kimi/custom-model"].is_default is True
    assert by_key["bailian"].default_model is not None
    deepseek_models = {model.id: model for model in by_key["deepseek"].models}
    assert deepseek_models["deepseek-v4-flash"].is_default is True
    entpilot_models = {model.id: model for model in by_key["entpilot"].models}
    assert entpilot_models["seedance-2.5-720p"].is_default is True
    assert by_key["entpilot"].default_model == "seedance-2.5-720p"


def test_provider_catalog_accepts_explicit_generic_entpilot_key(monkeypatch) -> None:
    """显式使用通用 VIDEO_GEN_API_KEY 时，EntPilot 仍应标记为可用。"""
    from app.core import config
    from app.services.provider_catalog import build_provider_catalog

    monkeypatch.setattr(
        config,
        "settings",
        _settings(
            video_gen_provider="entpilot",
            video_gen_api_key="generic-entpilot-key",
        ),
    )

    catalog = build_provider_catalog()
    entpilot = next(item for item in catalog.providers if item.key == "entpilot")
    assert entpilot.available is True


def test_provider_catalog_uses_explicit_generic_model_with_default_provider(monkeypatch) -> None:
    """仅显式填写通用模型时，目录默认值要与 EntPilot 运行时一致。"""
    from app.core import config
    from app.services.provider_catalog import build_provider_catalog

    monkeypatch.setattr(
        config,
        "settings",
        _settings(video_gen_model="seedance-2.5-720p"),
    )

    catalog = build_provider_catalog()
    entpilot = next(item for item in catalog.providers if item.key == "entpilot")
    assert entpilot.default_model == "seedance-2.5-720p"


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


def test_provider_catalog_normalizes_entpilot_alias_for_default_model(monkeypatch) -> None:
    """VIDEO_GEN_PROVIDER 使用公开别名时，EntPilot 仍应标记通用默认模型。"""
    from app.core import config
    from app.services.provider_catalog import build_provider_catalog

    monkeypatch.setattr(
        config,
        "settings",
        _settings(
            entpilot_api_key="entpilot-key",
            video_gen_provider="ent-pilot",
            video_gen_model="seedance-2.5-720p",
        ),
    )

    catalog = build_provider_catalog()
    entpilot = next(item for item in catalog.providers if item.key == "entpilot")
    models = {item.id: item for item in entpilot.models}
    assert entpilot.default_model == "seedance-2.5-720p"
    assert models["seedance-2.5-720p"].is_default is True


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
