"""应用配置和中间件契约测试。"""

import pytest
from fastapi import HTTPException, Request
from httpx import ASGITransport, AsyncClient, Response
from pydantic import ValidationError
from sqlalchemy import create_engine
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import Settings

PRODUCTION_ENVIRONMENTS = ["prod", "production"]
TRUSTED_ORIGIN = "https://trusted.example"
UNTRUSTED_ORIGIN = "https://untrusted.example"
STRONG_SESSION_SECRET = "MTV_9qL2-vR7!cX4@pN8#sK5$wH1%zF6&dB3"


def test_default_sync_database_driver_is_installed() -> None:
    configured = Settings(_env_file=None)

    engine = create_engine(configured.database_sync_url)
    engine.dispose()


@pytest.mark.parametrize("app_env", PRODUCTION_ENVIRONMENTS)
def test_prod_rejects_default_session_secret(app_env: str) -> None:
    with pytest.raises(ValidationError, match="SESSION_SECRET"):
        Settings(
            app_env=app_env,
            session_secret="dev-only-change-me",
            cors_origins=[TRUSTED_ORIGIN],
            _env_file=None,
        )


@pytest.mark.parametrize("app_env", PRODUCTION_ENVIRONMENTS)
@pytest.mark.parametrize("secret", ["", "   ", "x" * 31])
def test_prod_rejects_short_or_blank_session_secret(app_env: str, secret: str) -> None:
    with pytest.raises(ValidationError, match="SESSION_SECRET"):
        Settings(
            app_env=app_env,
            session_secret=secret,
            cors_origins=[TRUSTED_ORIGIN],
            _env_file=None,
        )


@pytest.mark.parametrize("app_env", PRODUCTION_ENVIRONMENTS)
@pytest.mark.parametrize(
    "secret",
    [
        "x" * 32,
        "01234567" * 4,
        "password" * 4,
    ],
)
def test_prod_rejects_low_strength_session_secret(app_env: str, secret: str) -> None:
    with pytest.raises(ValidationError, match="SESSION_SECRET"):
        Settings(
            app_env=app_env,
            session_secret=secret,
            cors_origins=[TRUSTED_ORIGIN],
            _env_file=None,
        )


@pytest.mark.parametrize("app_env", PRODUCTION_ENVIRONMENTS)
def test_prod_accepts_strong_session_secret(app_env: str) -> None:
    configured = Settings(
        app_env=app_env,
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=[TRUSTED_ORIGIN],
        _env_file=None,
    )

    assert configured.is_prod is True


def test_dev_allows_default_session_secret() -> None:
    configured = Settings(
        app_env="dev",
        session_secret="dev-only-change-me",
        _env_file=None,
    )

    assert configured.is_dev is True


@pytest.mark.parametrize(
    "public_base_url",
    [
        "http://api.example.com",
        "http://localhost:8000",
        "https://localhost:8000",
        "https://api.localhost",
        "https://127.0.0.1",
        "https://10.0.0.1",
        "https://169.254.169.254",
        "https://[::1]",
        "https://[fd00::1]",
        "https://user:password@api.example.com",
        "https://api.example.com/v3api",
        "https://api.example.com?token=value",
        "https://api.example.com#fragment",
    ],
)
def test_reference_image_audit_public_base_url_requires_https_origin(
    public_base_url: str,
) -> None:
    with pytest.raises(ValidationError, match="VOLC_IMAGE_AUDIT_PUBLIC_BASE_URL"):
        Settings(
            volc_image_audit_public_base_url=public_base_url,
            _env_file=None,
        )


def test_reference_image_audit_public_base_url_normalizes_https_origin() -> None:
    configured = Settings(
        volc_image_audit_public_base_url="https://api.example.com/",
        _env_file=None,
    )

    assert configured.volc_image_audit_public_base_url == "https://api.example.com"


@pytest.mark.parametrize(
    ("provider", "provider_settings", "expected_key", "expected_model", "expected_base_url"),
    [
        (
            "deepseek",
            {"deepseek_api_key": "deepseek-key"},
            "deepseek-key",
            "deepseek-v4-flash",
            "",
        ),
        (
            "claude",
            {"anthropic_api_key": "anthropic-key"},
            "anthropic-key",
            "claude-sonnet-5",
            "https://api.anthropic.com",
        ),
        (
            "gemini",
            {"gemini_api_key": "gemini-key"},
            "gemini-key",
            "gemini-2.5-flash",
            "",
        ),
        (
            "bailian",
            {"dashscope_api_key": "dashscope-key"},
            "dashscope-key",
            "qwen-plus",
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
        ),
        (
            "neolink",
            {
                "neolink_api_key": "neolink-key",
                "neolink_base_url": "https://neolink.example/api/",
            },
            "neolink-key",
            "qwen-turbo",
            "https://neolink.example/api",
        ),
        (
            "neo-link",
            {
                "neolink_api_key": "neolink-key",
                "neolink_base_url": "https://neolink.example/api/",
            },
            "neolink-key",
            "qwen-turbo",
            "https://neolink.example/api",
        ),
        (
            "neo_link",
            {
                "neolink_api_key": "neolink-key",
                "neolink_base_url": "https://neolink.example/api/",
            },
            "neolink-key",
            "qwen-turbo",
            "https://neolink.example/api",
        ),
    ],
)
def test_llm_capabilities_use_provider_specific_defaults(
    monkeypatch,
    provider: str,
    provider_settings: dict[str, str],
    expected_key: str,
    expected_model: str,
    expected_base_url: str,
) -> None:
    # 隔离 OS 环境变量（如 CI/harness 注入的 ANTHROPIC_MODEL），仅信任显式传入的参数。
    for env_name in (
        "ANTHROPIC_API_KEY", "ANTHROPIC_MODEL", "ANTHROPIC_BASE_URL",
        "DEEPSEEK_API_KEY", "DEEPSEEK_MODEL",
        "GEMINI_API_KEY", "GEMINI_LLM_MODEL",
        "DASHSCOPE_API_KEY", "DASHSCOPE_BASE_URL", "DASHSCOPE_VIDEO_MODEL",
        "NEOLINK_API_KEY", "NEOLINK_BASE_URL",
        "OPENAI_API_KEY", "OPENAI_LLM_MODEL", "OPENAI_IMAGE_MODEL", "OPENAI_VIDEO_MODEL",
        "TEXT_LLM_PROVIDER", "TEXT_LLM_API_KEY", "TEXT_LLM_MODEL", "TEXT_LLM_BASE_URL",
        "CHAT_LLM_PROVIDER", "CHAT_LLM_API_KEY", "CHAT_LLM_MODEL", "CHAT_LLM_BASE_URL",
        "IMAGE_GEN_PROVIDER", "IMAGE_GEN_API_KEY", "IMAGE_GEN_MODEL", "IMAGE_GEN_BASE_URL",
    ):
        monkeypatch.delenv(env_name, raising=False)
    configured = Settings(
        text_llm_provider=provider,
        chat_llm_provider=provider,
        _env_file=None,
        **provider_settings,
    )

    assert configured.text_llm_api_key == expected_key
    assert configured.chat_llm_api_key == expected_key
    assert configured.text_llm_model == expected_model
    assert configured.chat_llm_model == expected_model
    assert configured.text_llm_base_url == expected_base_url
    assert configured.chat_llm_base_url == expected_base_url


def test_neolink_llm_generic_settings_take_precedence_over_fallbacks() -> None:
    configured = Settings(
        text_llm_provider="neolink",
        text_llm_api_key="text-key",
        text_llm_model="text-model",
        text_llm_base_url="https://text.example/api/",
        chat_llm_provider="neolink",
        chat_llm_api_key="chat-key",
        chat_llm_model="chat-model",
        chat_llm_base_url="https://chat.example/api/",
        neolink_api_key="dedicated-key",
        neolink_base_url="https://dedicated.example/api/",
        image_gen_provider="neolink",
        image_gen_api_key="image-key",
        image_gen_base_url="https://image.example/api/",
        _env_file=None,
    )

    assert (
        configured.text_llm_api_key,
        configured.text_llm_model,
        configured.text_llm_base_url,
    ) == ("text-key", "text-model", "https://text.example/api")
    assert (
        configured.chat_llm_api_key,
        configured.chat_llm_model,
        configured.chat_llm_base_url,
    ) == ("chat-key", "chat-model", "https://chat.example/api")


@pytest.mark.parametrize("image_gen_provider", ["neolink", "neo_link"])
def test_neolink_llm_falls_back_to_explicit_neolink_image_settings(
    monkeypatch,
    image_gen_provider: str,
) -> None:
    for env_name in (
        "NEOLINK_API_KEY",
        "NEOLINK_BASE_URL",
        "TEXT_LLM_API_KEY",
        "TEXT_LLM_BASE_URL",
        "TEXT_LLM_MODEL",
        "CHAT_LLM_API_KEY",
        "CHAT_LLM_BASE_URL",
        "CHAT_LLM_MODEL",
    ):
        monkeypatch.delenv(env_name, raising=False)

    configured = Settings(
        text_llm_provider="neolink",
        chat_llm_provider="neolink",
        image_gen_provider=image_gen_provider,
        image_gen_api_key="image-key",
        image_gen_base_url="https://image.example/api/",
        _env_file=None,
    )

    assert configured.text_llm_api_key == "image-key"
    assert configured.chat_llm_api_key == "image-key"
    assert configured.text_llm_base_url == "https://image.example/api"
    assert configured.chat_llm_base_url == "https://image.example/api"
    assert configured.text_llm_model == "qwen-turbo"
    assert configured.chat_llm_model == "qwen-turbo"


def test_neolink_llm_explicit_image_settings_take_precedence_over_dedicated_fallbacks(
    monkeypatch,
) -> None:
    for env_name in (
        "TEXT_LLM_API_KEY",
        "TEXT_LLM_BASE_URL",
        "CHAT_LLM_API_KEY",
        "CHAT_LLM_BASE_URL",
        "NEOLINK_API_KEY",
        "NEOLINK_BASE_URL",
        "IMAGE_GEN_PROVIDER",
        "IMAGE_GEN_API_KEY",
        "IMAGE_GEN_BASE_URL",
    ):
        monkeypatch.delenv(env_name, raising=False)

    configured = Settings(
        text_llm_provider="neolink",
        chat_llm_provider="neolink",
        neolink_api_key="dedicated-key",
        neolink_base_url="https://neolink.com/api/",
        image_gen_provider="neolink",
        image_gen_api_key="image-key",
        image_gen_base_url="https://image.example/api/",
        _env_file=None,
    )

    assert configured.text_llm_api_key == "image-key"
    assert configured.chat_llm_api_key == "image-key"
    assert configured.text_llm_base_url == "https://image.example/api"
    assert configured.chat_llm_base_url == "https://image.example/api"


def test_neolink_llm_blank_dedicated_key_does_not_bypass_local_configuration_error(
    monkeypatch,
) -> None:
    for env_name in (
        "TEXT_LLM_API_KEY",
        "CHAT_LLM_API_KEY",
        "IMAGE_GEN_PROVIDER",
        "IMAGE_GEN_API_KEY",
    ):
        monkeypatch.delenv(env_name, raising=False)

    configured = Settings(
        text_llm_provider="neolink",
        chat_llm_provider="neolink",
        neolink_api_key=" \t",
        _env_file=None,
    )

    assert configured.text_llm_api_key == ""
    assert configured.chat_llm_api_key == ""


def test_production_alias_enables_secure_session_cookie(monkeypatch) -> None:
    from app import main

    configured = Settings(
        app_env="production",
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=[TRUSTED_ORIGIN],
        _env_file=None,
    )
    monkeypatch.setattr(main, "settings", configured)

    app = main.create_app()
    session_middleware = next(
        middleware
        for middleware in app.user_middleware
        if middleware.cls is SessionMiddleware
    )

    assert session_middleware.kwargs["https_only"] is True


def test_session_cookie_secure_can_be_disabled_for_http_test_environment(monkeypatch) -> None:
    from app import main

    configured = Settings(
        app_env="production",
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=[],
        session_cookie_secure=False,
        _env_file=None,
    )
    monkeypatch.setattr(main, "settings", configured)

    app = main.create_app()
    session_middleware = next(
        middleware
        for middleware in app.user_middleware
        if middleware.cls is SessionMiddleware
    )

    assert configured.session_cookie_secure_enabled is False
    assert session_middleware.kwargs["https_only"] is False


@pytest.mark.parametrize(
    "origins",
    [
        [""],
        ["null"],
        ["file:///tmp/flowapp"],
        ["ftp://trusted.example"],
        ["https://user:password@trusted.example"],
        ["https://trusted.example?tenant=flow"],
        ["https://trusted.example#section"],
        ["https://trusted.example/path"],
        ["trusted.example"],
        ["https://trusted.example:invalid"],
    ],
)
def test_cors_rejects_missing_or_invalid_trusted_origins(
    origins: list[str],
) -> None:
    with pytest.raises(ValidationError, match="CORS_ORIGINS"):
        Settings(cors_origins=origins, _env_file=None)


def test_cors_allows_empty_list_for_same_origin_only() -> None:
    configured = Settings(
        app_env="production",
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=[],
        _env_file=None,
    )

    assert configured.cors_origins == []
    assert configured.cors_allow_credentials is False


def test_cors_allows_wildcard_in_non_production() -> None:
    configured = Settings(
        app_env="dev",
        cors_origins=["*"],
        _env_file=None,
    )

    assert configured.cors_origins == ["*"]
    assert configured.cors_allow_credentials is False


def test_cors_rejects_wildcard_in_production() -> None:
    with pytest.raises(ValidationError, match="CORS_ORIGINS"):
        Settings(
            app_env="prod",
            session_secret=STRONG_SESSION_SECRET,
            cors_origins=["*"],
            _env_file=None,
        )


def test_cors_strips_whitespace_and_normalizes_root_trailing_slash() -> None:
    configured = Settings(
        cors_origins=["  https://trusted.example/  "],
        _env_file=None,
    )

    assert configured.cors_origins == [TRUSTED_ORIGIN]


async def test_cors_preflight_uses_configured_origin(monkeypatch) -> None:
    from app import main

    configured = Settings(
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=["http://localhost:3000"],
        cors_methods=["GET"],
        cors_headers=["Content-Type"],
        _env_file=None,
    )
    monkeypatch.setattr(main, "settings", configured)
    app = main.create_app()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as test_client:
        response = await test_client.options(
            "/v3api/health",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Content-Type",
            },
        )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:3000"
    assert response.headers["access-control-allow-credentials"] == "true"
    assert "GET" in response.headers["access-control-allow-methods"]
    assert "content-type" in response.headers["access-control-allow-headers"].lower()


async def test_cors_empty_list_rejects_cross_origin(monkeypatch) -> None:
    from app import main

    configured = Settings(
        app_env="production",
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=[],
        cors_methods=["GET"],
        cors_headers=["Content-Type"],
        _env_file=None,
    )
    monkeypatch.setattr(main, "settings", configured)
    app = main.create_app()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://api.example"
    ) as test_client:
        response = await test_client.options(
            "/v3api/health",
            headers={
                "Origin": "https://frontend.example",
                "Access-Control-Request-Method": "GET",
            },
        )

    assert response.status_code == 400
    assert "access-control-allow-origin" not in response.headers
    assert "access-control-allow-credentials" not in response.headers


async def test_cors_preflight_allows_any_origin_without_credentials(monkeypatch) -> None:
    from app import main

    configured = Settings(
        app_env="dev",
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=["*"],
        cors_methods=["GET"],
        cors_headers=["Content-Type"],
        _env_file=None,
    )
    monkeypatch.setattr(main, "settings", configured)
    app = main.create_app()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as test_client:
        response = await test_client.options(
            "/v3api/health",
            headers={
                "Origin": "http://localhost:4173",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Content-Type",
            },
        )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"
    assert "access-control-allow-credentials" not in response.headers


async def test_cors_preflight_honors_custom_settings(monkeypatch) -> None:
    from app import main

    custom_settings = Settings(
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=["https://trusted.example"],
        cors_methods=["PATCH"],
        cors_headers=["X-Flow-Request"],
        _env_file=None,
    )
    monkeypatch.setattr(main, "settings", custom_settings)
    app = main.create_app()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as test_client:
        response = await test_client.options(
            "/v3api/health",
            headers={
                "Origin": "https://trusted.example",
                "Access-Control-Request-Method": "PATCH",
                "Access-Control-Request-Headers": "X-Flow-Request",
            },
        )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://trusted.example"
    assert response.headers["access-control-allow-credentials"] == "true"
    assert "PATCH" in response.headers["access-control-allow-methods"]
    assert "x-flow-request" in response.headers[
        "access-control-allow-headers"
    ].lower()


def _create_cors_contract_app(monkeypatch):
    from app import main

    configured = Settings(
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=[TRUSTED_ORIGIN],
        _env_file=None,
    )
    monkeypatch.setattr(main, "settings", configured)
    app = main.create_app()

    @app.get("/__tests__/ok")
    async def ok() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/__tests__/http-error")
    async def http_error() -> None:
        raise HTTPException(status_code=409, detail="测试冲突")

    @app.get("/__tests__/validation-error")
    async def validation_error(value: int) -> dict[str, int]:
        return {"value": value}

    @app.get("/__tests__/unhandled-error")
    async def unhandled_error() -> None:
        raise RuntimeError("不得泄露的测试异常")

    return app


async def _request_contract_app(monkeypatch, path: str, origin: str) -> Response:
    app = _create_cors_contract_app(monkeypatch)

    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as test_client:
        return await test_client.get(path, headers={"Origin": origin})


@pytest.mark.parametrize(
    ("path", "status_code", "body"),
    [
        ("/__tests__/ok", 200, {"status": "ok"}),
        ("/__tests__/http-error", 409, {"detail": "测试冲突"}),
        ("/__tests__/validation-error", 422, {"detail": "字段校验失败"}),
        ("/__tests__/unhandled-error", 500, {"detail": "服务器内部错误"}),
    ],
)
async def test_trusted_origin_is_allowed_on_all_response_types(
    monkeypatch,
    path: str,
    status_code: int,
    body: dict[str, object],
) -> None:
    response = await _request_contract_app(monkeypatch, path, TRUSTED_ORIGIN)

    assert response.status_code == status_code
    assert response.json() == body
    assert response.headers["access-control-allow-origin"] == TRUSTED_ORIGIN
    assert response.headers["access-control-allow-credentials"] == "true"


async def test_untrusted_origin_has_no_allow_origin_header(monkeypatch) -> None:
    response = await _request_contract_app(monkeypatch, "/__tests__/ok", UNTRUSTED_ORIGIN)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert "access-control-allow-origin" not in response.headers


async def test_untrusted_preflight_is_rejected(monkeypatch) -> None:
    app = _create_cors_contract_app(monkeypatch)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as test_client:
        response = await test_client.options(
            "/__tests__/ok",
            headers={
                "Origin": UNTRUSTED_ORIGIN,
                "Access-Control-Request-Method": "GET",
            },
        )

    assert response.status_code == 400
    assert "access-control-allow-origin" not in response.headers


async def test_https_session_request_sets_secure_http_only_lax_cookie(monkeypatch) -> None:
    from app import main

    configured = Settings(
        app_env="prod",
        session_secret=STRONG_SESSION_SECRET,
        cors_origins=[TRUSTED_ORIGIN],
        _env_file=None,
    )
    monkeypatch.setattr(main, "settings", configured)
    app = main.create_app()

    @app.get("/__tests__/session")
    async def write_session(request: Request) -> dict[str, str]:
        request.session["user_id"] = "test-user"
        return {"status": "ok"}

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://test"
    ) as test_client:
        response = await test_client.get("/__tests__/session")

    cookie = response.headers["set-cookie"].lower()
    assert response.status_code == 200
    assert "secure" in cookie
    assert "httponly" in cookie
    assert "samesite=lax" in cookie
