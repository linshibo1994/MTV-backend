"""FastAPI 应用工厂。

- 挂载 SessionMiddleware (httpOnly signed Cookie 会话)
- 挂载 /v3api 业务路由
- 挂载媒体代理路由 (sapi 等价)
- 统一异常处理 {"detail": ...}
"""

from contextlib import asynccontextmanager
from typing import Any, cast

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.types import ASGIApp, HTTPExceptionHandler

from app.api.router import api_router
from app.api.v1 import media
from app.core.config import settings
from app.core.errors import (
    ConflictError,
    ForbiddenError,
    InsufficientCreditsError,
    NotAuthenticatedError,
    http_exception_handler,
    unhandled_exception_handler,
    validation_exception_handler,
)
from app.core.redis import close_redis


class CORSFastAPI(FastAPI):
    """让 CORS 包住 Starlette 的 ServerErrorMiddleware。"""

    def __init__(
        self,
        *,
        cors_origins: list[str],
        cors_methods: list[str],
        cors_headers: list[str],
        cors_allow_credentials: bool,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._cors_origins = cors_origins
        self._cors_methods = cors_methods
        self._cors_headers = cors_headers
        self._cors_allow_credentials = cors_allow_credentials

    def build_middleware_stack(self) -> ASGIApp:
        return CORSMiddleware(
            app=super().build_middleware_stack(),
            allow_origins=self._cors_origins,
            allow_credentials=self._cors_allow_credentials,
            allow_methods=self._cors_methods,
            allow_headers=self._cors_headers,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_redis()


def create_app() -> FastAPI:
    app = CORSFastAPI(
        cors_origins=settings.cors_origins,
        cors_methods=settings.cors_methods,
        cors_headers=settings.cors_headers,
        cors_allow_credentials=settings.cors_allow_credentials,
        title="智绘 Flow App 后端",
        version="0.1.0",
        docs_url="/v3api/docs",
        openapi_url="/v3api/openapi.json",
        lifespan=lifespan,
    )

    # httpOnly 会话 Cookie (SessionMiddleware 默认即 httponly)
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret,
        session_cookie=settings.session_cookie_name,
        max_age=settings.session_max_age,
        same_site="lax",
        https_only=settings.is_prod,
    )
    # 媒体代理是必需能力，导入或依赖损坏时应阻止应用带着残缺路由启动。
    app.include_router(media.router, prefix=settings.media_proxy_prefix, tags=["媒体代理"])

    # 业务路由
    app.include_router(api_router)

    # 异常处理
    http_handler = cast(HTTPExceptionHandler, http_exception_handler)
    validation_handler = cast(HTTPExceptionHandler, validation_exception_handler)
    app.add_exception_handler(HTTPException, http_handler)
    app.add_exception_handler(RequestValidationError, validation_handler)
    app.add_exception_handler(NotAuthenticatedError, http_handler)
    app.add_exception_handler(ForbiddenError, http_handler)
    app.add_exception_handler(ConflictError, http_handler)
    app.add_exception_handler(InsufficientCreditsError, http_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)

    return app


app = create_app()
