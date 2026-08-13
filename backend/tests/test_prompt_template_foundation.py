"""PromptTemplate 基础设施专项测试。"""

import asyncio
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from jinja2.sandbox import SecurityError
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.api.v1 import prompt_templates as prompt_template_api
from app.api.v1.prompt_templates import _require_platform_admin
from app.core.errors import BadRequestError, ForbiddenError
from app.models.prompt_template import PromptTemplate
from app.schemas.prompt_template import PromptTemplateCreateIn
from app.services import prompt_template as service


class _ScalarResult:
    def __init__(self, values) -> None:
        self._values = list(values)

    def scalars(self):
        return self

    def all(self):
        return list(self._values)

    def scalar_one_or_none(self):
        return self._values[0] if self._values else None


def _template(
    *,
    key: str = "storyboard.system",
    version: int = 1,
    content: str = "你好 {{ name }}",
    is_active: bool = True,
) -> PromptTemplate:
    now = datetime.now(UTC)
    return PromptTemplate(
        id=f"template-{version}",
        key=key,
        version=version,
        content=content,
        content_sha256=service.prompt_content_sha256(content),
        variables_schema={"name": {"type": "string"}},
        is_active=is_active,
        created_by_user_id="admin-1",
        created_at=now,
        updated_at=now,
    )


def test_prompt_template_model_has_immutable_version_and_single_active_constraints() -> None:
    names = {
        constraint.name
        for constraint in PromptTemplate.__table__.constraints
        if constraint.name is not None
    }
    indexes = {index.name: index for index in PromptTemplate.__table__.indexes}
    assert "uq_prompt_templates_key_version" in names
    assert indexes["uq_prompt_templates_active_key"].unique is True


def test_validate_and_render_use_strict_declared_variables() -> None:
    referenced = service.validate_prompt_template(
        "标题：{{ title }}",
        {"title": {"type": "string"}},
    )
    assert referenced == frozenset({"title"})
    snapshot = service.fallback_snapshot("storyboard.user", "标题：{{ title }}")
    assert service.render_prompt_template(snapshot, {"title": "第一集"}) == "标题：第一集"

    with pytest.raises(service.PromptTemplateValidationError) as exc_info:
        service.validate_prompt_template("{{ private_value }}", {})
    assert exc_info.value.diagnostic == {"version": 1, "category": "undeclared_variable"}

    with pytest.raises(service.PromptTemplateValidationError) as exc_info:
        service.render_prompt_template(snapshot, {})
    assert exc_info.value.diagnostic == {"version": 1, "category": "render_failed"}


def test_sandbox_rejects_unsafe_attribute_without_leaking_template(caplog) -> None:
    sentinel = "PRIVATE_TEMPLATE_BODY_8f0e"
    snapshot = service.fallback_snapshot(
        "storyboard.system",
        f"{sentinel} {{{{ value.__class__.__mro__ }}}}",
    )
    with pytest.raises(service.PromptTemplateValidationError) as exc_info:
        service.render_prompt_template(snapshot, {"value": "x"})
    assert exc_info.value.__cause__ is None
    assert sentinel not in str(exc_info.value)
    assert sentinel not in caplog.text


def test_sandbox_never_invokes_callable_template_variables() -> None:
    invoked = False

    def private_callback() -> str:
        nonlocal invoked
        invoked = True
        return "不应返回"

    snapshot = service.fallback_snapshot("storyboard.system", "{{ callback() }}")
    with pytest.raises(service.PromptTemplateValidationError) as exc_info:
        service.render_prompt_template(snapshot, {"callback": private_callback})
    assert exc_info.value.diagnostic == {"version": 1, "category": "render_failed"}
    assert invoked is False


def test_render_failure_falls_back_and_returns_actual_snapshot() -> None:
    active = service.PromptTemplateSnapshot(
        key="storyboard.system",
        template_id="template-1",
        version=1,
        content_sha256="a" * 64,
        content="{{ missing }}",
        source="database",
    )
    rendered, used = service.render_prompt_template_with_fallback(
        active,
        {"name": "阿明"},
        fallback="你好 {{ name }}",
    )
    assert rendered == "你好 阿明"
    assert used.source == "fallback"
    assert used.version == 0
    assert used.template_id is None


@pytest.mark.asyncio
async def test_resolve_bundle_keeps_database_snapshot_and_fills_fallbacks() -> None:
    template = _template()
    db = SimpleNamespace(execute=AsyncMock(return_value=_ScalarResult([template])))
    bundle = await service.resolve_prompt_bundle(
        db,
        {
            "storyboard.system": "系统兜底",
            "storyboard.user": "用户兜底",
        },
    )
    assert bundle["storyboard.system"].source == "database"
    assert bundle["storyboard.system"].template_id == "template-1"
    assert bundle["storyboard.user"].source == "fallback"
    with pytest.raises(TypeError):
        bundle["other"] = service.fallback_snapshot("other", "x")  # type: ignore[index]


@pytest.mark.asyncio
async def test_resolve_bundle_does_not_validate_unused_fallback() -> None:
    template = _template()
    db = SimpleNamespace(execute=AsyncMock(return_value=_ScalarResult([template])))
    bundle = await service.resolve_prompt_bundle(db, {"storyboard.system": ""})
    assert bundle["storyboard.system"].source == "database"


@pytest.mark.asyncio
async def test_resolve_template_uses_fallback_when_query_fails_without_leaking_error(caplog) -> None:
    private_error = "PRIVATE_DATABASE_MESSAGE_d310"
    db = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError(private_error)))
    snapshot = await service.resolve_prompt_template(
        db,
        "storyboard.system",
        fallback="系统 {{ name }}",
    )
    assert snapshot.source == "fallback"
    assert snapshot.content == "系统 {{ name }}"
    assert private_error not in caplog.text


def test_resolve_template_postgres_statement_failure_keeps_outer_transaction_usable() -> None:
    """真实 PostgreSQL 查询失败只回滚 SAVEPOINT，后续业务落库事务仍可继续。"""

    database_url = os.environ.get("MTV_TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("未配置 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 回退事务测试")
    if make_url(database_url).drivername != "postgresql+asyncpg":
        pytest.fail("MTV_TEST_POSTGRES_URL 必须使用 postgresql+asyncpg 驱动")

    async def scenario() -> None:
        from app.services.storyboard import _selected_scene_audition_block

        engine = create_async_engine(database_url)
        async with engine.connect() as connection:
            transaction = await connection.begin()
            db = AsyncSession(bind=connection, expire_on_commit=False)
            try:
                # 临时搜索路径模拟滚动发布期间 prompt_templates 表尚未迁移。
                await db.execute(text("SET LOCAL search_path TO pg_temp"))
                snapshot = await service.resolve_prompt_template(
                    db,
                    "storyboard.system",
                    fallback="系统回退",
                )
                assert snapshot.source == "fallback"
                assert (await db.execute(text("SELECT 1"))).scalar_one() == 1
                audition_block = await _selected_scene_audition_block(
                    db,
                    script_id="missing-script",
                    episode_id="missing-episode",
                    input_version=1,
                )
                assert audition_block == ""
                assert (await db.execute(text("SELECT 2"))).scalar_one() == 2
            finally:
                await db.close()
                if transaction.is_active:
                    await transaction.rollback()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_create_version_is_append_only_and_can_activate() -> None:
    older = _template(version=2, is_active=True)

    class _DB:
        def __init__(self) -> None:
            self.added = None
            self.execute_count = 0

        async def execute(self, _statement):
            self.execute_count += 1
            return _ScalarResult([older] if self.execute_count == 1 else [])

        def add(self, value) -> None:
            self.added = value

        async def flush(self) -> None:
            if self.added is not None and self.added.id is None:
                self.added.id = "template-3"

    db = _DB()
    created = await service.create_prompt_template_version(
        db,  # type: ignore[arg-type]
        key="storyboard.system",
        content="标题：{{ name }}",
        variables_schema={"name": {"type": "string"}},
        actor_user_id="admin-1",
        activate=True,
    )
    assert created.version == 3
    assert created.is_active is True
    assert older.is_active is False
    assert created.content_sha256 == service.prompt_content_sha256(created.content)


@pytest.mark.asyncio
async def test_create_first_version_maps_unique_race_to_conflict() -> None:
    class _DB:
        async def execute(self, _statement):
            return _ScalarResult([])

        def add(self, _value) -> None:
            return None

        async def flush(self) -> None:
            raise IntegrityError("insert", {}, RuntimeError("unique"))

    with pytest.raises(Exception) as exc_info:
        await service.create_prompt_template_version(
            _DB(),  # type: ignore[arg-type]
            key="storyboard.system",
            content="系统提示词",
            variables_schema={},
            actor_user_id="admin-1",
        )
    assert getattr(exc_info.value, "status_code", None) == 409
    assert "unique" not in str(exc_info.value)


def test_platform_admin_guard_requires_enabled_admin() -> None:
    _require_platform_admin(SimpleNamespace(enabled=True, is_admin=True))
    for user in (
        SimpleNamespace(enabled=True, is_admin=False),
        SimpleNamespace(enabled=False, is_admin=True),
    ):
        with pytest.raises(ForbiddenError):
            _require_platform_admin(user)


def test_create_schema_preserves_template_whitespace_and_rejects_blank() -> None:
    payload = PromptTemplateCreateIn(
        key="storyboard.system",
        content="  标题：{{ title }}\n",
        variables_schema={"title": {"type": "string"}},
    )
    assert payload.content == "  标题：{{ title }}\n"
    with pytest.raises(ValueError):
        PromptTemplateCreateIn(key="storyboard.system", content=" \n\t ")


@pytest.mark.asyncio
async def test_create_api_maps_template_validation_to_safe_bad_request(monkeypatch) -> None:
    private_body = "PRIVATE_TEMPLATE_BODY_1b90"
    monkeypatch.setattr(
        prompt_template_api,
        "create_prompt_template_version",
        AsyncMock(side_effect=service.PromptTemplateValidationError("invalid_syntax")),
    )
    payload = PromptTemplateCreateIn(
        key="storyboard.system",
        content=private_body,
    )
    user = SimpleNamespace(enabled=True, is_admin=True, user_id="admin-1")
    with pytest.raises(BadRequestError) as exc_info:
        await prompt_template_api.create_prompt_template(
            payload,
            SimpleNamespace(),  # type: ignore[arg-type]
            user,  # type: ignore[arg-type]
        )
    assert exc_info.value.detail == "提示词模板定义无效"
    assert private_body not in str(exc_info.value.detail)


def test_prompt_validation_error_never_contains_body(monkeypatch) -> None:
    sentinel = "PRIVATE_TEMPLATE_CONTENT_28aa"
    monkeypatch.setattr(
        service._ENVIRONMENT,
        "parse",
        lambda _content: (_ for _ in ()).throw(SecurityError("private")),
    )
    with pytest.raises(service.PromptTemplateValidationError) as exc_info:
        service.validate_prompt_template(sentinel, {})
    assert sentinel not in str(exc_info.value)
