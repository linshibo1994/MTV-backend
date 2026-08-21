"""已下线提示词风格与优化端点的路由契约。"""

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "payload"),
    [
        ("GET", "/v3api/prompt-styles", None),
        ("POST", "/v3api/prompt-optimize", {"prompt": "a quiet street", "target": "image"}),
        (
            "POST",
            "/v3api/prompt-optimize/batch",
            {"items": [{"prompt": "a quiet street", "target": "image"}]},
        ),
    ],
)
async def test_removed_prompt_endpoints_return_not_found(
    client,
    method: str,
    path: str,
    payload: dict | None,
) -> None:
    """静态风格目录和提示词优化接口下线后不得再被路由暴露。"""
    response = await client.request(method, path, json=payload)

    assert response.status_code == 404
