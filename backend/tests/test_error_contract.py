"""统一错误响应契约测试。"""


async def test_validation_error_has_only_detail(client) -> None:
    response = await client.post("/v3api/auth/login", json={})

    assert response.status_code == 422
    assert response.json() == {"detail": "字段校验失败"}
