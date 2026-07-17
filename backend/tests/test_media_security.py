"""跨模块生成产物的媒体代理安全契约。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.api.v1 import media
from app.core.errors import NotFoundError


class _Result:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one_or_none(self) -> object:
        return self.value


@pytest.mark.parametrize("status", ["starting", "processing", "failed", "canceled"])
async def test_timeline_media_rejects_every_non_succeeded_render(
    monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    job = SimpleNamespace(
        status=status,
        script_id="script-1",
        output_storage_key="generate_outputs/script-1/episode-1/render-1.mp4",
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=_Result(job)))
    authorize = AsyncMock()
    monkeypatch.setattr(media, "_authorize_script", authorize)

    with pytest.raises(NotFoundError, match="媒体资源不存在"):
        await media._authorize_timeline_media_key(
            db,
            "generate_outputs/script-1/episode-1/render-1.mp4",
            SimpleNamespace(),
            False,
        )

    authorize.assert_not_awaited()


async def test_timeline_media_authorizes_succeeded_render(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = SimpleNamespace(
        status="succeeded",
        script_id="script-1",
        output_storage_key="generate_outputs/script-1/episode-1/render-1.mp4",
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=_Result(job)))
    authorize = AsyncMock()
    monkeypatch.setattr(media, "_authorize_script", authorize)

    await media._authorize_timeline_media_key(
        db,
        "generate_outputs/script-1/episode-1/render-1.mp4",
        SimpleNamespace(),
        False,
    )

    authorize.assert_awaited_once()
