"""幂等 schema 漂移修复、已下线对象清理与团队项目 owner 成员数据回填。

背景
----
线上数据库曾被 ``alembic stamp`` 盖章到基线 ``d4b7e1c9a362``，但 stamp 不会
真正执行迁移的 DDL，因此线上实际 schema 与基线声明不一致（schema 漂移）：

- ``scene_auditions`` 缺 ``attempt_count`` / ``error_code`` / ``error_message``
  / ``started_at`` / ``finished_at`` 五列；
- ``subject_candidates`` 缺 ``resolution_action`` / ``resolution_request_hash``
  两列及对应 CHECK 约束；
- 团队项目 owner 成员数据修复迁移 ``f6c9a31e8b42`` 在 stamp 时未真正执行，
  部分团队项目可能缺少合法 owner 成员行。
- 动态画风下线前已存在的 ``art_styles``、``art_style_extraction_jobs``、旧列和
  生成任务 JSON 快照仍可能遗留在已盖章的数据库中。

由于线上已被 stamp 到基线 ``d4b7e1c9a362``（= head），``alembic upgrade head``
变为 no-op，无法修复上述漂移。本模块在 ``alembic upgrade head`` 之后幂等补齐，
使实际 schema 与模型元数据一致、补回数据修复。

行为
----
- **全新数据库**：``alembic upgrade head`` 已创建全部 schema，本模块全部 no-op。
- **漂移数据库**：补建缺表 / 缺列 / 缺约束，清理已下线对象并回填 owner 成员。
- **已修复数据库**：全部幂等 no-op，可安全地在每次部署时执行。

CLI
---
::

    python -m app.db.drift_repair             # 执行幂等修复
    python -m app.db.drift_repair --check-only  # 仅检测，发现漂移则退出码 1
    python -m app.db.drift_repair --normalize-alembic-revision
                                               # 仅映射已知的错误 revision

``--check-only`` 用于部署门禁：在 ``alembic upgrade head`` 与修复之后运行，
若仍有 schema 漂移或 owner 数据不一致，则阻断应用启动。
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection

from app.core.config import settings
from app.models import Base  # noqa: F401  触发各模型注册到 Base.metadata
from app.models.storage_reference_guard import STORAGE_REFERENCE_GUARD_BYPASS

# ---------------------------------------------------------------------------
# 已知漂移列：基线 create_table 已含这些列，但线上旧表（stamp 前已存在）
# 缺少它们。用 ``ADD COLUMN IF NOT EXISTS`` 幂等补齐。DDL 字符串与模型定义对齐。
# ---------------------------------------------------------------------------
_KNOWN_DRIFT_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("scene_auditions", "attempt_count", "INTEGER NOT NULL DEFAULT 0"),
    ("scene_auditions", "error_code", "VARCHAR(64)"),
    ("scene_auditions", "error_message", "VARCHAR(255)"),
    ("scene_auditions", "started_at", "TIMESTAMP WITH TIME ZONE"),
    ("scene_auditions", "finished_at", "TIMESTAMP WITH TIME ZONE"),
    ("subject_candidates", "resolution_action", "VARCHAR(32)"),
    ("subject_candidates", "resolution_request_hash", "VARCHAR(64)"),
)

# 已知漂移 CHECK 约束：用 DO 块在约束不存在时幂等补建。
_KNOWN_DRIFT_CHECK_CONSTRAINTS: tuple[tuple[str, str, str], ...] = (
    (
        "scene_auditions",
        "ck_scene_auditions_attempt_count_non_negative",
        "CHECK (attempt_count >= 0)",
    ),
    (
        "subject_candidates",
        "ck_subject_candidates_resolution_action",
        "CHECK (resolution_action IS NULL OR resolution_action "
        "IN ('link_existing', 'create_subject', 'ignore'))",
    ),
)

# ---------------------------------------------------------------------------
# 动态画风与生成提示词模板已下线。基线文件只描述最终 schema；已存在且被 stamp
# 到基线的数据库不会重跑基线 upgrade，因此必须在这里幂等收敛。
# ---------------------------------------------------------------------------
_RETIRED_ART_STYLE_TABLES = ("art_style_extraction_jobs", "art_styles")
_RETIRED_ART_STYLE_COLUMNS = (
    ("scripts", "visual_style"),
    ("scripts", "unify_style"),
    ("subjects", "visual_style"),
    ("subjects", "enrich_version"),
)
_LEGACY_ALEMBIC_REVISION_REPLACEMENTS = {"e8a1f2b3c4d5": "d4b7e1c9a362"}

# MGROUTER 已下线。基线迁移不能重写，因此已 stamp 的数据库需要在本模块中
# 幂等地把存量 provider key 迁移到 EntPilot，并替换两张视频表上的 CHECK 约束。
_RETIRED_VIDEO_PROVIDER_KEY = "mgrouter"
_ENTPILOT_VIDEO_PROVIDER_KEY = "entpilot"
_ENTPILOT_DEFAULT_VIDEO_MODEL = "seedance-2.0-720p"
_VIDEO_PROVIDER_KEY_CONSTRAINTS: tuple[tuple[str, str], ...] = (
    ("video_provider_configs", "ck_video_provider_configs_provider_key"),
    ("video_generation_jobs", "ck_video_jobs_provider_key"),
)
_VIDEO_PROVIDER_KEY_CHECK_SQL = (
    "CHECK (provider_key IN "
    "('jianyu', 'modelsai', 'volcengine', 'doubao', 'neolink', 'bailian', "
    "'openai', 'gemini', 'entpilot'))"
)


@dataclass(frozen=True, slots=True)
class _RetiredDataArtifact:
    name: str
    required_tables: frozenset[str]
    exists_sql: str
    cleanup_sql: str


_RETIRED_ART_STYLE_DATA_ARTIFACTS: tuple[_RetiredDataArtifact, ...] = (
    _RetiredDataArtifact(
        name="task_dispatch_outbox.art_style.extract",
        required_tables=frozenset({"task_dispatch_outbox"}),
        exists_sql="SELECT EXISTS (SELECT 1 FROM task_dispatch_outbox WHERE task_type = 'art_style.extract')",
        cleanup_sql="DELETE FROM task_dispatch_outbox WHERE task_type = 'art_style.extract'",
    ),
    _RetiredDataArtifact(
        name="prompt_templates.video.constraint_lock",
        required_tables=frozenset({"prompt_templates"}),
        exists_sql="SELECT EXISTS (SELECT 1 FROM prompt_templates WHERE key = 'video.constraint_lock')",
        cleanup_sql="DELETE FROM prompt_templates WHERE key = 'video.constraint_lock'",
    ),
    _RetiredDataArtifact(
        name="generation_jobs.retired_prompt_fields",
        required_tables=frozenset({"generation_jobs"}),
        exists_sql=(
            "SELECT EXISTS (SELECT 1 FROM generation_jobs "
            "WHERE jsonb_typeof(input_json) = 'object' "
            "AND input_json ?| ARRAY['style_id', 'art_style_id', 'visual_style', 'unify_style', "
            "'resolved_prompts', 'enrich_version'])"
        ),
        cleanup_sql=(
            "UPDATE generation_jobs "
            "SET input_json = input_json - 'style_id' - 'art_style_id' - 'visual_style' - 'unify_style' "
            "- 'resolved_prompts' - 'enrich_version' "
            "WHERE jsonb_typeof(input_json) = 'object' "
            "AND input_json ?| ARRAY['style_id', 'art_style_id', 'visual_style', 'unify_style', "
            "'resolved_prompts', 'enrich_version']"
        ),
    ),
    _RetiredDataArtifact(
        name="video_generation_jobs.retired_prompt_fields",
        required_tables=frozenset({"video_generation_jobs"}),
        exists_sql=(
            "SELECT EXISTS (SELECT 1 FROM video_generation_jobs "
            "WHERE jsonb_typeof(input_json) = 'object' "
            "AND input_json ?| ARRAY['style_id', 'art_style_id', 'visual_style', 'unify_style', "
            "'constraint_prompt_template'])"
        ),
        cleanup_sql=(
            "UPDATE video_generation_jobs "
            "SET input_json = input_json - 'style_id' - 'art_style_id' - 'visual_style' - 'unify_style' "
            "- 'constraint_prompt_template' "
            "WHERE jsonb_typeof(input_json) = 'object' "
            "AND input_json ?| ARRAY['style_id', 'art_style_id', 'visual_style', 'unify_style', "
            "'constraint_prompt_template']"
        ),
    ),
    _RetiredDataArtifact(
        name="pipeline_stages.enrich_prompts",
        required_tables=frozenset({"pipeline_stages"}),
        exists_sql=(
            "SELECT EXISTS (SELECT 1 FROM pipeline_stages "
            "WHERE stage = 'enrich' AND jsonb_typeof(detail) = 'object' AND detail ? 'prompts')"
        ),
        cleanup_sql=(
            "UPDATE pipeline_stages SET detail = detail - 'prompts' "
            "WHERE stage = 'enrich' AND jsonb_typeof(detail) = 'object' AND detail ? 'prompts'"
        ),
    ),
    _RetiredDataArtifact(
        name="pipeline_jobs.enrich_prompts",
        required_tables=frozenset({"pipeline_jobs"}),
        exists_sql=(
            "SELECT EXISTS (SELECT 1 FROM pipeline_jobs "
            "WHERE stage = 'enrich' AND jsonb_typeof(output_json) = 'object' AND output_json ? 'prompts')"
        ),
        cleanup_sql=(
            "UPDATE pipeline_jobs SET output_json = output_json - 'prompts' "
            "WHERE stage = 'enrich' AND jsonb_typeof(output_json) = 'object' AND output_json ? 'prompts'"
        ),
    ),
    _RetiredDataArtifact(
        name="pipeline_jobs.merge_enrich_versions",
        required_tables=frozenset({"pipeline_jobs"}),
        exists_sql=(
            "SELECT EXISTS (SELECT 1 FROM pipeline_jobs "
            "WHERE stage = 'merge' AND jsonb_typeof(output_json) = 'object' AND EXISTS ("
            "SELECT 1 FROM jsonb_array_elements(CASE "
            "WHEN jsonb_typeof(output_json -> 'subjects') = 'array' "
            "THEN output_json -> 'subjects' ELSE '[]'::jsonb END) AS subjects(item) "
            "WHERE jsonb_typeof(item) = 'object' "
            "AND item ?| ARRAY['base_enrich_version', 'enrich_version']))"
        ),
        cleanup_sql=(
            "UPDATE pipeline_jobs SET output_json = jsonb_set("
            "output_json, '{subjects}', (SELECT jsonb_agg(CASE "
            "WHEN jsonb_typeof(item) = 'object' "
            "THEN item - 'base_enrich_version' - 'enrich_version' ELSE item END ORDER BY ordinal) "
            "FROM jsonb_array_elements(CASE WHEN jsonb_typeof(output_json -> 'subjects') = 'array' "
            "THEN output_json -> 'subjects' ELSE '[]'::jsonb END) "
            "WITH ORDINALITY AS subjects(item, ordinal))) "
            "WHERE stage = 'merge' AND jsonb_typeof(output_json) = 'object' AND EXISTS ("
            "SELECT 1 FROM jsonb_array_elements(CASE "
            "WHEN jsonb_typeof(output_json -> 'subjects') = 'array' "
            "THEN output_json -> 'subjects' ELSE '[]'::jsonb END) AS subjects(item) "
            "WHERE jsonb_typeof(item) = 'object' "
            "AND item ?| ARRAY['base_enrich_version', 'enrich_version'])"
        ),
    ),
)


@dataclass(slots=True)
class DriftReport:
    """漂移检测报告，``--check-only`` 模式据此决定退出码。"""

    missing_tables: list[str]
    missing_columns: list[str]
    missing_constraints: list[str]
    retired_tables: list[str]
    retired_columns: list[str]
    retired_data: list[str]
    legacy_alembic_revisions: list[str]
    owner_data_inconsistent: bool

    @property
    def has_drift(self) -> bool:
        return bool(
            self.missing_tables
            or self.missing_columns
            or self.missing_constraints
            or self.retired_tables
            or self.retired_columns
            or self.retired_data
            or self.legacy_alembic_revisions
            or self.owner_data_inconsistent
        )

    def render(self) -> str:
        lines: list[str] = ["检测到 schema 漂移："]
        if self.missing_tables:
            lines.append("  缺表: " + ", ".join(self.missing_tables))
        if self.missing_columns:
            lines.append("  缺列: " + "; ".join(self.missing_columns))
        if self.missing_constraints:
            lines.append("  缺约束: " + ", ".join(self.missing_constraints))
        if self.retired_tables:
            lines.append("  待清理旧表: " + ", ".join(self.retired_tables))
        if self.retired_columns:
            lines.append("  待清理旧列: " + "; ".join(self.retired_columns))
        if self.retired_data:
            lines.append("  待清理旧数据: " + "; ".join(self.retired_data))
        if self.legacy_alembic_revisions:
            lines.append("  待回正 Alembic revision: " + ", ".join(self.legacy_alembic_revisions))
        if self.owner_data_inconsistent:
            lines.append("  团队项目 owner 成员数据不一致（存在无 owner 或多 owner 的团队项目）")
        return "\n".join(lines)


def _sync_url() -> str:
    """从异步 DATABASE_URL 派生同步连接串，避免依赖未配置的 DATABASE_SYNC_URL。"""
    url = settings.database_url
    if "+asyncpg" in url:
        return url.replace("+asyncpg", "+psycopg2", 1)
    if url.startswith("postgres://"):  # Heroku 风格
        return "postgresql+psycopg2://" + url[len("postgres://"):]
    if url.startswith("postgresql+psycopg2://") or url.startswith("postgresql+psycopg://"):
        return url  # 已是同步驱动
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return settings.database_sync_url



def _existing_tables(conn: Connection) -> set[str]:
    return set(inspect(conn).get_table_names())


def _existing_columns(conn: Connection, table: str) -> set[str]:
    return {col["name"] for col in inspect(conn).get_columns(table)}


def _existing_constraints(conn: Connection, table: str) -> set[str]:
    return {
        name
        for constraint in inspect(conn).get_check_constraints(table)
        if isinstance(name := constraint.get("name"), str)
    }


def _existing_constraint_definitions(conn: Connection, table: str) -> dict[str, str]:
    """返回 CHECK 约束定义，供已发布基线的约束内容收敛使用。"""

    return {
        name: str(constraint.get("sqltext") or "")
        for constraint in inspect(conn).get_check_constraints(table)
        if isinstance(name := constraint.get("name"), str)
    }


def _provider_key_constraint_is_current(definition: str | None) -> bool:
    """判断 provider key 约束是否已移除下线的 MGROUTER 并包含 EntPilot。"""

    normalized = " ".join((definition or "").lower().split())
    return (
        bool(normalized)
        and _RETIRED_VIDEO_PROVIDER_KEY not in normalized
        and _ENTPILOT_VIDEO_PROVIDER_KEY in normalized
    )


def _detect_video_provider_constraint_drift(
    conn: Connection,
    existing: set[str],
) -> list[str]:
    """检测两张视频表上的 provider key 约束是否仍是旧基线定义。"""

    missing: list[str] = []
    for table, constraint_name in _VIDEO_PROVIDER_KEY_CONSTRAINTS:
        if table not in existing:
            continue
        definitions = _existing_constraint_definitions(conn, table)
        if not _provider_key_constraint_is_current(definitions.get(constraint_name)):
            missing.append(constraint_name)
    return missing


def _migrate_retired_video_provider_data(conn: Connection, existing: set[str]) -> None:
    """迁移存量 MGROUTER 配置/任务，避免新约束拒绝旧行。"""

    if "video_provider_configs" in existing:
        # 若 EntPilot 行已由人工配置创建，先合并 active 状态并删除旧行，
        # 避免 provider_key + job_type 唯一约束冲突；没有目标行时再直接改 key。
        conn.execute(
            text(
                """
                UPDATE video_provider_configs AS target
                SET is_active = target.is_active OR EXISTS (
                    SELECT 1
                    FROM video_provider_configs AS legacy
                    WHERE legacy.provider_key = :legacy_key
                      AND legacy.job_type = target.job_type
                      AND legacy.is_active = TRUE
                )
                WHERE target.provider_key = :new_key
                """
            ),
            {
                "legacy_key": _RETIRED_VIDEO_PROVIDER_KEY,
                "new_key": _ENTPILOT_VIDEO_PROVIDER_KEY,
            },
        )
        conn.execute(
            text(
                """
                DELETE FROM video_provider_configs AS legacy
                WHERE legacy.provider_key = :legacy_key
                  AND EXISTS (
                      SELECT 1
                      FROM video_provider_configs AS target
                      WHERE target.provider_key = :new_key
                        AND target.job_type = legacy.job_type
                  )
                """
            ),
            {
                "legacy_key": _RETIRED_VIDEO_PROVIDER_KEY,
                "new_key": _ENTPILOT_VIDEO_PROVIDER_KEY,
            },
        )
        conn.execute(
            text(
                """
                UPDATE video_provider_configs
                SET provider_key = :new_key,
                    -- MGROUTER 的模型命名空间与 EntPilot 不兼容；旧行即使
                    -- 使用了自定义模型，也必须落到新供应商的合法模型清单。
                    model = :default_model
                WHERE provider_key = :legacy_key
                """
            ),
            {
                "legacy_key": _RETIRED_VIDEO_PROVIDER_KEY,
                "new_key": _ENTPILOT_VIDEO_PROVIDER_KEY,
                "default_model": _ENTPILOT_DEFAULT_VIDEO_MODEL,
            },
        )

    if "video_generation_jobs" not in existing:
        return

    # 任务 JSON 是恢复/审计时的快照；只迁移明确指向旧供应商的路由字段，
    # 不改写提示词、媒体引用或已完成任务的结果地址。先按外层 provider_key
    # 归一化模型，再改写外层 key，避免丢失“这是旧任务”的判定依据。
    for column in ("input_json", "output_json", "metrics"):
        conn.execute(
            text(
                f"""
                UPDATE video_generation_jobs
                SET {column} = jsonb_set(
                    {column}, '{{model}}', '"seedance-2.0-720p"'::jsonb, TRUE
                )
                WHERE jsonb_typeof({column}) = 'object'
                  AND (
                      provider_key = :legacy_key
                      OR {column}->>'provider' IN ('mgrouter', 'aicc')
                      OR {column}->>'provider_key' IN ('mgrouter', 'aicc')
                  )
                """
            ),
            {"legacy_key": _RETIRED_VIDEO_PROVIDER_KEY},
        )
        conn.execute(
            text(
                f"""
                UPDATE video_generation_jobs
                SET {column} = jsonb_set(
                    {column}, '{{provider}}', '"entpilot"'::jsonb, TRUE
                )
                WHERE jsonb_typeof({column}) = 'object'
                  AND {column}->>'provider' IN ('mgrouter', 'aicc')
                """
            )
        )
        conn.execute(
            text(
                f"""
                UPDATE video_generation_jobs
                SET {column} = jsonb_set(
                    {column}, '{{provider_key}}', '"entpilot"'::jsonb, TRUE
                )
                WHERE jsonb_typeof({column}) = 'object'
                  AND {column}->>'provider_key' IN ('mgrouter', 'aicc')
                """
            )
        )

    conn.execute(
        text(
            """
            UPDATE video_generation_jobs
            SET provider_key = :new_key
            WHERE provider_key = :legacy_key
            """
        ),
        {
            "legacy_key": _RETIRED_VIDEO_PROVIDER_KEY,
            "new_key": _ENTPILOT_VIDEO_PROVIDER_KEY,
        },
    )


def _repair_video_provider_key_constraints(
    conn: Connection,
    existing: set[str],
) -> list[str]:
    """先解除旧约束，再迁移数据，最后幂等补回新的 CHECK 约束。

    PostgreSQL 会在 UPDATE 语句执行时立即校验 CHECK 约束；因此不能在旧约束
    仍拒绝 ``entpilot`` 的窗口内直接把 ``mgrouter`` 行改名。
    """

    constraints_to_rebuild: list[tuple[str, str]] = []
    for table, constraint_name in _VIDEO_PROVIDER_KEY_CONSTRAINTS:
        if table not in existing:
            continue
        definitions = _existing_constraint_definitions(conn, table)
        if _provider_key_constraint_is_current(definitions.get(constraint_name)):
            continue
        # DROP 与后续数据迁移处于同一事务；若任一步失败，事务回滚后仍保留旧约束。
        conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint_name}"))
        constraints_to_rebuild.append((table, constraint_name))

    _migrate_retired_video_provider_data(conn, existing)

    still_missing: list[str] = []
    for table, constraint_name in constraints_to_rebuild:
        conn.execute(
            text(
                f"ALTER TABLE {table} ADD CONSTRAINT {constraint_name} "
                f"{_VIDEO_PROVIDER_KEY_CHECK_SQL}"
            )
        )
        if constraint_name not in _existing_constraints(conn, table):
            still_missing.append(constraint_name)
    return still_missing


def _detect_missing_tables(conn: Connection, existing: set[str]) -> list[str]:
    """列出模型元数据中、数据库里缺失的表（遗留表 meizi_storage_objects 无模型，不会出现）。"""
    model_tables = set(Base.metadata.tables.keys())
    return sorted(model_tables - existing)


def _repair_missing_tables(conn: Connection, missing: list[str]) -> None:
    """用模型元数据幂等补建缺表（checkfirst 跳过已存在的表与列级对象）。"""
    if not missing:
        return
    # 仅创建缺失的表，避免对已存在表重复创建列/约束；checkfirst 保证幂等。
    tables_to_create = [Base.metadata.tables[name] for name in missing]
    Base.metadata.create_all(conn, checkfirst=True, tables=tables_to_create)


def _repair_known_drift_columns(conn: Connection) -> list[str]:
    """幂等补齐已知漂移列，返回仍缺失（理论上不会发生）的列名。"""
    still_missing: list[str] = []
    for table, column, ddl in _KNOWN_DRIFT_COLUMNS:
        existing = _existing_columns(conn, table)
        if column not in existing:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {ddl}"))
    # 重新检测以确认
    for table, column, _ in _KNOWN_DRIFT_COLUMNS:
        if column not in _existing_columns(conn, table):
            still_missing.append(f"{table}.{column}")
    return still_missing


def _repair_known_drift_constraints(conn: Connection) -> list[str]:
    """幂等补齐已知漂移 CHECK 约束，返回仍缺失的约束名。"""
    still_missing: list[str] = []
    for table, constraint_name, check_sql in _KNOWN_DRIFT_CHECK_CONSTRAINTS:
        existing = _existing_constraints(conn, table)
        if constraint_name not in existing:
            conn.execute(
                text(
                    "DO $do$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint "
                    f"WHERE conname = '{constraint_name}') THEN "
                    f"ALTER TABLE {table} ADD CONSTRAINT {constraint_name} {check_sql}; "
                    "END IF; END $do$"
                )
            )
        if constraint_name not in _existing_constraints(conn, table):
            still_missing.append(constraint_name)
    return still_missing


def _detect_retired_art_style_tables(existing: set[str]) -> list[str]:
    return [table for table in _RETIRED_ART_STYLE_TABLES if table in existing]


def _detect_retired_art_style_columns(conn: Connection, existing: set[str]) -> list[str]:
    retired_columns: list[str] = []
    for table, column in _RETIRED_ART_STYLE_COLUMNS:
        if table in existing and column in _existing_columns(conn, table):
            retired_columns.append(f"{table}.{column}")
    return retired_columns


def _detect_retired_art_style_data(conn: Connection, existing: set[str]) -> list[str]:
    retired_data: list[str] = []
    for artifact in _RETIRED_ART_STYLE_DATA_ARTIFACTS:
        if artifact.required_tables.issubset(existing):
            if bool(conn.execute(text(artifact.exists_sql)).scalar()):
                retired_data.append(artifact.name)
    return retired_data


def _repair_retired_art_style_artifacts(conn: Connection, existing: set[str]) -> None:
    """幂等清理已下线的动态画风及生成提示词模板遗留对象。"""
    for artifact in _RETIRED_ART_STYLE_DATA_ARTIFACTS:
        if artifact.required_tables.issubset(existing):
            conn.execute(text(artifact.cleanup_sql))

    # 子表先于父表，且不用 CASCADE，避免意外删除未在本次清理范围中的依赖对象。
    if "art_style_extraction_jobs" in existing:
        conn.execute(text("DELETE FROM art_style_extraction_jobs"))
        conn.execute(text("DROP TABLE IF EXISTS art_style_extraction_jobs"))
    if "art_styles" in existing:
        conn.execute(text("DELETE FROM art_styles"))
        conn.execute(text("DROP TABLE IF EXISTS art_styles"))

    for table, column in _RETIRED_ART_STYLE_COLUMNS:
        if table in existing:
            conn.execute(text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {column}"))


def _legacy_alembic_revisions(conn: Connection, existing: set[str]) -> list[str]:
    if "alembic_version" not in existing:
        return []
    revisions = set(conn.execute(text("SELECT version_num FROM alembic_version")).scalars())
    return sorted(revisions & _LEGACY_ALEMBIC_REVISION_REPLACEMENTS.keys())


def _resolve_legacy_revision_replacement(revisions: set[str]) -> str | None:
    """只允许单一、明确已知的错误 revision 回正到唯一基线。"""
    legacy_revisions = revisions & _LEGACY_ALEMBIC_REVISION_REPLACEMENTS.keys()
    if not legacy_revisions:
        return None
    if len(revisions) != 1 or len(legacy_revisions) != 1:
        raise RuntimeError(
            "Alembic revision 包含未知或混合值，拒绝自动回正；"
            f"请先人工核对 alembic_version：{', '.join(sorted(revisions))}"
        )
    return _LEGACY_ALEMBIC_REVISION_REPLACEMENTS[legacy_revisions.pop()]


def normalize_legacy_alembic_revision() -> bool:
    """把已知错误 revision 回正，令 Alembic 可继续加载唯一基线。"""
    engine = create_engine(_sync_url(), future=True)
    try:
        with engine.begin() as conn:
            existing = _existing_tables(conn)
            if "alembic_version" not in existing:
                return False
            revisions = set(
                conn.execute(text("SELECT version_num FROM alembic_version FOR UPDATE")).scalars()
            )
            replacement = _resolve_legacy_revision_replacement(revisions)
            if replacement is None:
                return False
            legacy_revision = next(iter(revisions))
            conn.execute(
                text(
                    "UPDATE alembic_version SET version_num = :replacement "
                    "WHERE version_num = :legacy_revision"
                ),
                {"replacement": replacement, "legacy_revision": legacy_revision},
            )
            return True
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# 团队项目 owner 成员数据回填（原迁移 f6c9a31e8b42，幂等）
# 规则：每个 team scope 的 script 必须恰好有一个 owner 成员 = script.owner_user_id。
# ---------------------------------------------------------------------------
_OWNER_BACKFILL_STATEMENTS: tuple[str, ...] = (
    # 非真实所有者（user_id != script.owner_user_id）却挂着 owner 角色，降为 manager。
    """
    UPDATE project_members AS member
    SET project_role = 'manager'
    FROM scripts AS script
    WHERE script.id = member.script_id
      AND script.scope = 'team'
      AND member.project_role = 'owner'
      AND member.user_id <> script.owner_user_id
    """,
    # 真实所有者已在成员表但角色不是 owner，恢复为 owner。
    """
    UPDATE project_members AS member
    SET project_role = 'owner'
    FROM scripts AS script
    WHERE script.id = member.script_id
      AND script.scope = 'team'
      AND member.user_id = script.owner_user_id
      AND member.project_role <> 'owner'
    """,
    # 缺失成员行时用确定性 ID 幂等插入。JOIN users 过滤 owner_user_id 指向已删除
    # 用户的孤儿团队（scripts.owner_user_id 无 FK，可能悬空），避免触发
    # project_members.user_id 的外键违约；孤儿团队由 _owner_data_inconsistent 报告。
    """
    INSERT INTO project_members (id, script_id, user_id, project_role)
    SELECT
        'own-' || md5('mtv-team-owner:' || script.id),
        script.id,
        script.owner_user_id,
        'owner'
    FROM scripts AS script
    JOIN users AS owner_user ON owner_user.user_id = script.owner_user_id
    LEFT JOIN project_members AS actual_owner
      ON actual_owner.script_id = script.id
     AND actual_owner.user_id = script.owner_user_id
    WHERE script.scope = 'team'
      AND actual_owner.id IS NULL
    ON CONFLICT (id) DO NOTHING
    """,
)


def _owner_data_inconsistent(conn: Connection) -> bool:
    """检测是否存在无 owner、多 owner，或 owner_user_id 指向不存在用户的团队项目。"""
    result = conn.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM scripts AS script
                WHERE script.scope = 'team'
                  AND (
                      NOT EXISTS (
                          SELECT 1 FROM users AS u
                          WHERE u.user_id = script.owner_user_id
                      )
                      OR NOT EXISTS (
                          SELECT 1
                          FROM project_members AS actual_owner
                          WHERE actual_owner.script_id = script.id
                            AND actual_owner.user_id = script.owner_user_id
                            AND actual_owner.project_role = 'owner'
                      )
                      OR (
                          SELECT count(*)
                          FROM project_members AS owner_member
                          WHERE owner_member.script_id = script.id
                            AND owner_member.project_role = 'owner'
                      ) <> 1
                  )
            )
            """
        )
    )
    return bool(result.scalar())


def _repair_owner_data(conn: Connection) -> None:
    """幂等执行 owner 成员回填。先加表锁串行化，避免与业务并发写竞争。"""
    conn.execute(
        text("LOCK TABLE users, scripts, project_members IN SHARE ROW EXCLUSIVE MODE")
    )
    for stmt in _OWNER_BACKFILL_STATEMENTS:
        conn.execute(text(stmt))


def _detect_general_missing_columns(conn: Connection, existing: set[str]) -> list[str]:
    """通用列差异：对每张已存在的模型表，比对模型列名与数据库实际列名，报告缺失列。

    已知漂移清单只决定“如何修复”，不应限定“是否检测”；本函数使 ``--check-only``
    成为真正的 schema-vs-model 门禁，覆盖清单之外的漂移。
    """
    missing: list[str] = []
    for table_name, table in Base.metadata.tables.items():
        if table_name not in existing:
            continue  # 缺表由 missing_tables 报告
        model_cols = {c.name for c in table.columns}
        actual_cols = _existing_columns(conn, table_name)
        for col in sorted(model_cols - actual_cols):
            missing.append(f"{table_name}.{col}")
    return missing


def _detect(conn: Connection) -> DriftReport:
    existing = _existing_tables(conn)
    missing_tables = _detect_missing_tables(conn, existing)

    missing_columns: list[str] = []
    for table, column, _ in _KNOWN_DRIFT_COLUMNS:
        if table in existing and column not in _existing_columns(conn, table):
            missing_columns.append(f"{table}.{column}")

    missing_constraints: list[str] = []
    for table, constraint_name, _ in _KNOWN_DRIFT_CHECK_CONSTRAINTS:
        if table in existing and constraint_name not in _existing_constraints(conn, table):
            missing_constraints.append(constraint_name)

    # 通用列差异：捕获已知清单之外的列级漂移（如未来新迁移 stamp 未跑 DDL）。
    general_missing = _detect_general_missing_columns(conn, existing)
    # 去重（已知清单可能与通用检测重叠）
    for item in general_missing:
        if item not in missing_columns:
            missing_columns.append(item)

    for constraint_name in _detect_video_provider_constraint_drift(conn, existing):
        if constraint_name not in missing_constraints:
            missing_constraints.append(constraint_name)

    retired_tables = _detect_retired_art_style_tables(existing)
    retired_columns = _detect_retired_art_style_columns(conn, existing)
    retired_data = _detect_retired_art_style_data(conn, existing)
    legacy_alembic_revisions = _legacy_alembic_revisions(conn, existing)
    owner_inconsistent = _owner_data_inconsistent(conn) if "project_members" in existing else False

    return DriftReport(
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        missing_constraints=missing_constraints,
        retired_tables=retired_tables,
        retired_columns=retired_columns,
        retired_data=retired_data,
        legacy_alembic_revisions=legacy_alembic_revisions,
        owner_data_inconsistent=owner_inconsistent,
    )


def run_repair() -> DriftReport:
    """执行幂等修复，返回修复后的检测报告（应为无漂移）。

    schema DDL 修复与 owner 数据回填分属独立事务：owner 回填若因孤儿 owner_user_id
    等数据问题失败，不会回滚已完成的 schema 修复，使修复可逐步收敛。
    """
    engine = create_engine(_sync_url(), future=True)
    try:
        with engine.begin() as raw_conn:
            # 本事务仅执行受控的 schema/历史数据收敛；与 Alembic 环境一致地显式
            # 标记，避免存储引用写入保护误拦截对已下线 JSON 字段的删除操作。
            conn = raw_conn.execution_options(**{STORAGE_REFERENCE_GUARD_BYPASS: True})
            missing_tables = _detect_missing_tables(conn, _existing_tables(conn))
            _repair_missing_tables(conn, missing_tables)
            _repair_known_drift_columns(conn)
            _repair_known_drift_constraints(conn)
            _repair_video_provider_key_constraints(conn, _existing_tables(conn))
            _repair_retired_art_style_artifacts(conn, _existing_tables(conn))
        # owner 数据回填独立事务：DDL 已提交，回填失败不回滚 schema 修复。
        if "project_members" in _existing_tables_from_engine(engine):
            with engine.begin() as conn:
                _repair_owner_data(conn)
        with engine.begin() as conn:
            report = _detect(conn)
        return report
    finally:
        engine.dispose()


def _existing_tables_from_engine(engine) -> set[str]:
    with engine.connect() as conn:
        return set(inspect(conn).get_table_names())


def run_check_only() -> DriftReport:
    """仅检测不修复，返回当前漂移报告。"""
    engine = create_engine(_sync_url(), future=True)
    try:
        with engine.begin() as conn:
            return _detect(conn)
    finally:
        engine.dispose()


def main(argv: list[str]) -> int:
    if "--normalize-alembic-revision" in argv:
        changed = normalize_legacy_alembic_revision()
        if changed:
            print("已将已知错误 Alembic revision 回正到 d4b7e1c9a362。")
        else:
            print("Alembic revision 无需回正。")
        return 0

    check_only = "--check-only" in argv
    if check_only:
        report = run_check_only()
        if report.has_drift:
            print(report.render(), file=sys.stderr)
            return 1
        print("schema 漂移检测通过：实际 schema 与模型元数据一致。")
        return 0

    report = run_repair()
    if report.has_drift:
        # 修复后仍残留漂移，说明存在本模块未覆盖的差异，需人工排查。
        print(report.render(), file=sys.stderr)
        return 1
    print("schema 漂移修复完成：实际 schema 与模型元数据一致。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
