"""幂等 schema 漂移修复与团队项目 owner 成员数据回填。

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

由于线上已被 stamp 到基线 ``d4b7e1c9a362``（= head），``alembic upgrade head``
变为 no-op，无法修复上述漂移。本模块在 ``alembic upgrade head`` 之后幂等补齐，
使实际 schema 与模型元数据一致、补回数据修复。

行为
----
- **全新数据库**：``alembic upgrade head`` 已创建全部 schema，本模块全部 no-op。
- **漂移数据库**：补建缺表 / 缺列 / 缺约束，回填 owner 成员。
- **已修复数据库**：全部幂等 no-op，可安全地在每次部署时执行。

CLI
---
::

    python -m app.db.drift_repair             # 执行幂等修复
    python -m app.db.drift_repair --check-only  # 仅检测，发现漂移则退出码 1

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


@dataclass(slots=True)
class DriftReport:
    """漂移检测报告，``--check-only`` 模式据此决定退出码。"""

    missing_tables: list[str]
    missing_columns: list[str]
    missing_constraints: list[str]
    owner_data_inconsistent: bool

    @property
    def has_drift(self) -> bool:
        return bool(
            self.missing_tables
            or self.missing_columns
            or self.missing_constraints
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

    owner_inconsistent = _owner_data_inconsistent(conn) if "project_members" in existing else False

    return DriftReport(
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        missing_constraints=missing_constraints,
        owner_data_inconsistent=owner_inconsistent,
    )


def run_repair() -> DriftReport:
    """执行幂等修复，返回修复后的检测报告（应为无漂移）。

    schema DDL 修复与 owner 数据回填分属独立事务：owner 回填若因孤儿 owner_user_id
    等数据问题失败，不会回滚已完成的 schema 修复，使修复可逐步收敛。
    """
    engine = create_engine(_sync_url(), future=True)
    try:
        with engine.begin() as conn:
            missing_tables = _detect_missing_tables(conn, _existing_tables(conn))
            _repair_missing_tables(conn, missing_tables)
            _repair_known_drift_columns(conn)
            _repair_known_drift_constraints(conn)
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
