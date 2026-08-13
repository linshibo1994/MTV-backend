"""提示词模板版本解析、安全渲染与激活服务。"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from jinja2 import StrictUndefined, TemplateError, meta
from jinja2.sandbox import SandboxedEnvironment, SecurityError
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, NotFoundError
from app.models.prompt_template import PromptTemplate
from app.services.optional_query import (
    can_fallback_from_optional_query,
    execute_optional_query,
)

logger = logging.getLogger(__name__)

_MAX_TEMPLATE_CONTENT_LENGTH = 120_000
_MAX_TEMPLATE_RENDER_LENGTH = 240_000


class _PromptSandbox(SandboxedEnvironment):
    """提示词模板只允许消费数据，禁止调用变量携带的任意可调用对象。"""

    def is_safe_callable(self, obj: object) -> bool:
        return False


_ENVIRONMENT = _PromptSandbox(
    autoescape=False,
    undefined=StrictUndefined,
    enable_async=False,
)
_ENVIRONMENT.globals.clear()


class PromptTemplateValidationError(ValueError):
    """模板定义或渲染不满足安全契约；异常不携带模板正文或变量值。"""

    def __init__(self, category: str) -> None:
        super().__init__("提示词模板无效")
        self.diagnostic = {"version": 1, "category": category}


@dataclass(frozen=True)
class PromptTemplateSnapshot:
    """单次业务任务使用的不可变模板快照。"""

    key: str
    template_id: str | None
    version: int
    content_sha256: str
    content: str
    source: str

    def public_metadata(self) -> dict[str, str | int | None]:
        return {
            "key": self.key,
            "template_id": self.template_id,
            "version": self.version,
            "content_sha256": self.content_sha256,
            "source": self.source,
        }


def prompt_content_sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _normalize_key(key: str) -> str:
    normalized = key.strip()
    if not normalized or len(normalized) > 128:
        raise PromptTemplateValidationError("invalid_key")
    return normalized


def validate_prompt_template(content: str, variables_schema: Mapping[str, object]) -> frozenset[str]:
    """在持久化或启用前编译模板并校验声明变量。"""
    if not content.strip() or len(content) > _MAX_TEMPLATE_CONTENT_LENGTH:
        raise PromptTemplateValidationError("invalid_content")
    if any(not isinstance(name, str) or not name for name in variables_schema):
        raise PromptTemplateValidationError("invalid_variables_schema")
    try:
        syntax_tree = _ENVIRONMENT.parse(content)
    except TemplateError:
        raise PromptTemplateValidationError("invalid_syntax") from None
    referenced = frozenset(meta.find_undeclared_variables(syntax_tree))
    if not referenced <= variables_schema.keys():
        raise PromptTemplateValidationError("undeclared_variable")
    return referenced


def snapshot_from_model(template: PromptTemplate) -> PromptTemplateSnapshot:
    return PromptTemplateSnapshot(
        key=template.key,
        template_id=template.id,
        version=template.version,
        content_sha256=template.content_sha256,
        content=template.content,
        source="database",
    )


def fallback_snapshot(key: str, content: str) -> PromptTemplateSnapshot:
    normalized_key = _normalize_key(key)
    if not content.strip():
        raise PromptTemplateValidationError("invalid_fallback")
    return PromptTemplateSnapshot(
        key=normalized_key,
        template_id=None,
        version=0,
        content_sha256=prompt_content_sha256(content),
        content=content,
        source="fallback",
    )


async def resolve_prompt_template(
    db: AsyncSession,
    key: str,
    *,
    fallback: str | None = None,
) -> PromptTemplateSnapshot:
    """解析一个激活版本；未配置时按调用方显式 fallback 降级。"""
    normalized_key = _normalize_key(key)
    try:
        result = await execute_optional_query(
            db,
            select(PromptTemplate).where(
                PromptTemplate.key == normalized_key,
                PromptTemplate.is_active.is_(True),
            )
        )
    except Exception as exc:
        if fallback is None or not can_fallback_from_optional_query(exc):
            raise
        logger.warning("提示词模板查询失败，使用内置回退 key=%s", normalized_key)
        return fallback_snapshot(normalized_key, fallback)
    template = result.scalar_one_or_none()
    if template is not None:
        return snapshot_from_model(template)
    if fallback is not None:
        return fallback_snapshot(normalized_key, fallback)
    raise NotFoundError("提示词模板未配置")


async def resolve_prompt_bundle(
    db: AsyncSession,
    fallbacks: Mapping[str, str],
) -> Mapping[str, PromptTemplateSnapshot]:
    """一次查询解析多个模板，确保同一任务内使用稳定 active 快照。"""
    normalized_fallbacks = {_normalize_key(key): content for key, content in fallbacks.items()}
    if not normalized_fallbacks:
        return MappingProxyType({})
    try:
        result = await execute_optional_query(
            db,
            select(PromptTemplate).where(
                PromptTemplate.key.in_(normalized_fallbacks),
                PromptTemplate.is_active.is_(True),
            )
        )
    except Exception as exc:
        if not can_fallback_from_optional_query(exc):
            raise
        logger.warning("提示词模板批量查询失败，全部使用内置回退 count=%d", len(normalized_fallbacks))
        return MappingProxyType(
            {
                key: fallback_snapshot(key, content)
                for key, content in normalized_fallbacks.items()
            }
        )
    resolved = {
        template.key: snapshot_from_model(template)
        for template in result.scalars().all()
    }
    for key, content in normalized_fallbacks.items():
        if key not in resolved:
            resolved[key] = fallback_snapshot(key, content)
    return MappingProxyType(resolved)


def render_prompt_template(
    snapshot: PromptTemplateSnapshot,
    variables: Mapping[str, object],
) -> str:
    """用沙箱与严格未定义变量策略渲染，不在异常或日志中泄露正文。"""
    try:
        rendered = _ENVIRONMENT.from_string(snapshot.content).render(**variables)
    except (SecurityError, TemplateError, TypeError):
        logger.warning(
            "提示词模板渲染失败 key=%s version=%d source=%s",
            snapshot.key,
            snapshot.version,
            snapshot.source,
        )
        raise PromptTemplateValidationError("render_failed") from None
    if not rendered.strip() or len(rendered) > _MAX_TEMPLATE_RENDER_LENGTH:
        raise PromptTemplateValidationError("invalid_rendered_content")
    return rendered


def render_prompt_template_with_fallback(
    snapshot: PromptTemplateSnapshot,
    variables: Mapping[str, object],
    *,
    fallback: str,
) -> tuple[str, PromptTemplateSnapshot]:
    """渲染激活版本，失败时回退内置模板并返回实际使用的快照。"""
    try:
        return render_prompt_template(snapshot, variables), snapshot
    except PromptTemplateValidationError:
        if snapshot.source == "fallback":
            raise
    fallback_used = fallback_snapshot(snapshot.key, fallback)
    return render_prompt_template(fallback_used, variables), fallback_used


async def create_prompt_template_version(
    db: AsyncSession,
    *,
    key: str,
    content: str,
    variables_schema: Mapping[str, object],
    actor_user_id: str | None,
    activate: bool = False,
) -> PromptTemplate:
    """追加不可变版本；同一 key 的 version 在行锁内单调递增。"""
    normalized_key = _normalize_key(key)
    schema_copy = dict(variables_schema)
    validate_prompt_template(content, schema_copy)
    # 锁定已有 key 的版本行；PostgreSQL 下序列化同 key 版本分配。
    existing_rows = list(
        (
            await db.execute(
                select(PromptTemplate)
                .where(PromptTemplate.key == normalized_key)
                .order_by(PromptTemplate.version.desc())
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    next_version = (existing_rows[0].version if existing_rows else 0) + 1
    if activate:
        # 先在数据库中撤销旧 active，再插入新 active，避免 flush 的 INSERT/UPDATE
        # 排序触发部分唯一索引的瞬时冲突。
        await db.execute(
            update(PromptTemplate)
            .where(
                PromptTemplate.key == normalized_key,
                PromptTemplate.is_active.is_(True),
            )
            .values(is_active=False)
        )
        for existing in existing_rows:
            existing.is_active = False
    template = PromptTemplate(
        key=normalized_key,
        version=next_version,
        content=content,
        content_sha256=prompt_content_sha256(content),
        variables_schema=schema_copy,
        is_active=activate,
        created_by_user_id=actor_user_id,
    )
    db.add(template)
    try:
        await db.flush()
    except IntegrityError as exc:
        # 首个版本尚无同 key 行可锁，唯一约束负责封住并发窗口。
        raise ConflictError("提示词模板版本并发冲突，请重试") from exc
    return template


async def activate_prompt_template_version(
    db: AsyncSession,
    *,
    key: str,
    version: int,
) -> PromptTemplate:
    """原子切换 key 的 active 版本；正文与版本号保持不可变。"""
    normalized_key = _normalize_key(key)
    rows = list(
        (
            await db.execute(
                select(PromptTemplate)
                .where(PromptTemplate.key == normalized_key)
                .order_by(PromptTemplate.version.desc())
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    target = next((row for row in rows if row.version == version), None)
    if target is None:
        raise NotFoundError("提示词模板版本不存在")
    validate_prompt_template(target.content, target.variables_schema or {})
    await db.execute(
        update(PromptTemplate)
        .where(
            PromptTemplate.key == normalized_key,
            PromptTemplate.is_active.is_(True),
            PromptTemplate.id != target.id,
        )
        .values(is_active=False)
    )
    target.is_active = True
    await db.flush()
    return target


async def list_prompt_template_versions(
    db: AsyncSession,
    *,
    key: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[PromptTemplate], int]:
    normalized_key = _normalize_key(key) if key is not None else None
    filters = [PromptTemplate.key == normalized_key] if normalized_key is not None else []
    total = int(
        (
            await db.execute(select(func.count(PromptTemplate.id)).where(*filters))
        ).scalar_one()
    )
    result = await db.execute(
        select(PromptTemplate)
        .where(*filters)
        .order_by(PromptTemplate.key.asc(), PromptTemplate.version.desc())
        .offset(offset)
        .limit(limit)
    )
    return list(result.scalars().all()), total


async def get_active_prompt_template(
    db: AsyncSession,
    *,
    key: str,
) -> PromptTemplate | None:
    return (
        await db.execute(
            select(PromptTemplate).where(
                PromptTemplate.key == _normalize_key(key),
                PromptTemplate.is_active.is_(True),
            )
        )
    ).scalar_one_or_none()


async def get_prompt_template_version(
    db: AsyncSession,
    *,
    key: str,
    version: int,
) -> PromptTemplate:
    template = (
        await db.execute(
            select(PromptTemplate).where(
                PromptTemplate.key == _normalize_key(key),
                PromptTemplate.version == version,
            )
        )
    ).scalar_one_or_none()
    if template is None:
        raise NotFoundError("提示词模板版本不存在")
    return template


def ensure_version_is_immutable(
    existing: PromptTemplate,
    *,
    content: str,
    variables_schema: Mapping[str, object],
) -> None:
    """供集成层显式拒绝对已有版本正文的 PATCH。"""
    if existing.content != content or dict(existing.variables_schema or {}) != dict(variables_schema):
        raise ConflictError("提示词模板版本不可修改，请创建新版本")
