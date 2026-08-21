"""统一错误处理 — 所有错误响应体为 `{"detail": "<message>"}`。"""

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_409_CONFLICT,
)


class NotAuthenticatedError(HTTPException):
    """未登录 — 401 {"detail":"未登录"}。"""

    def __init__(self, detail: str = "未登录") -> None:
        super().__init__(status_code=HTTP_401_UNAUTHORIZED, detail=detail)


class BadRequestError(HTTPException):
    """请求参数或业务非法 — 400。"""

    def __init__(self, detail: str = "请求参数错误") -> None:
        super().__init__(status_code=HTTP_400_BAD_REQUEST, detail=detail)


class ForbiddenError(HTTPException):
    """无权限 — 403。"""

    def __init__(self, detail: str = "无权限") -> None:
        super().__init__(status_code=403, detail=detail)


class NotFoundError(HTTPException):
    """资源不存在 — 404。"""

    def __init__(self, detail: str = "资源不存在") -> None:
        super().__init__(status_code=404, detail=detail)


class ConflictError(HTTPException):
    """版本冲突 — 409。"""

    def __init__(self, detail: str = "版本冲突，请刷新后重试") -> None:
        super().__init__(status_code=HTTP_409_CONFLICT, detail=detail)


class InsufficientCreditsError(HTTPException):
    """积分不足 — 402。"""

    def __init__(self, detail: str = "积分不足") -> None:
        super().__init__(status_code=402, detail=detail)


async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


async def validation_exception_handler(
    _: Request, __: RequestValidationError
) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": "字段校验失败"})


async def unhandled_exception_handler(_: Request, __: Exception) -> JSONResponse:
    return JSONResponse(status_code=500, content={"detail": "服务器内部错误"})
