"""动态画风下线的唯一基线与漂移修复契约。"""

from __future__ import annotations

import importlib.util
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

import pytest

_VERSIONS_DIR = Path(__file__).resolve().parents[1] / "alembic" / "versions"
_BASELINE_PATH = _VERSIONS_DIR / "d4b7e1c9a362_当前数据库基线.py"


class _OperationRecorder:
    def __init__(self) -> None:
        self.tables: dict[str, tuple[str, ...]] = {}

    def create_table(self, table_name: str, *columns: object, **_kwargs: object) -> None:
        self.tables[table_name] = tuple(
            column.name for column in columns if isinstance(getattr(column, "name", None), str)
        )

    def create_index(self, *_args: object, **_kwargs: object) -> None:
        return None

    def bulk_insert(self, *_args: object, **_kwargs: object) -> None:
        return None

    def inline_literal(self, value: str) -> str:
        return value

    def create_foreign_key(self, *_args: object, **_kwargs: object) -> None:
        return None

    def f(self, name: str) -> str:
        return name


class _ConnectionRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object) -> None:
        self.statements.append(str(statement))


class _ScalarResult:
    def __init__(self, value: bool) -> None:
        self._value = value

    def scalar(self) -> bool:
        return self._value


def _load_baseline() -> ModuleType:
    assert _BASELINE_PATH.is_file(), "缺少唯一数据库基线"
    spec = importlib.util.spec_from_file_location("current_database_baseline", _BASELINE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_baseline_is_the_only_migration_and_creates_final_schema() -> None:
    """新库只创建最终 schema，不能再经由额外迁移删除旧对象。"""
    assert {path.name for path in _VERSIONS_DIR.glob("*.py")} == {_BASELINE_PATH.name}

    baseline = _load_baseline()
    recorder = _OperationRecorder()
    baseline.op = recorder

    baseline.upgrade()

    assert {"art_styles", "art_style_extraction_jobs"}.isdisjoint(recorder.tables)
    assert {"visual_style", "unify_style"}.isdisjoint(recorder.tables["scripts"])
    assert {"visual_style", "enrich_version"}.isdisjoint(recorder.tables["subjects"])
    assert "video.constraint_lock" not in {key for key, _content, _variables in baseline._DEFAULT_PROMPT_TEMPLATES}


def test_drift_repair_removes_retired_art_style_objects_idempotently() -> None:
    """已 stamp 的旧库由 drift repair 清除下线对象，重复部署保持无害。"""
    from app.db import drift_repair

    report = drift_repair.DriftReport(
        missing_tables=[],
        missing_columns=[],
        missing_constraints=[],
        retired_tables=["art_styles"],
        retired_columns=["scripts.visual_style"],
        retired_data=["prompt_templates.video.constraint_lock"],
        legacy_alembic_revisions=[],
        owner_data_inconsistent=False,
    )
    assert report.has_drift
    assert "待清理旧表: art_styles" in report.render()

    recorder = _ConnectionRecorder()
    drift_repair._repair_retired_art_style_artifacts(
        recorder,
        {
            "art_style_extraction_jobs",
            "art_styles",
            "generation_jobs",
            "video_generation_jobs",
            "pipeline_jobs",
            "pipeline_stages",
            "prompt_templates",
            "scripts",
            "subjects",
            "task_dispatch_outbox",
        },
    )

    statements = "\n".join(recorder.statements)
    assert "DELETE FROM task_dispatch_outbox WHERE task_type = 'art_style.extract'" in statements
    assert "DELETE FROM prompt_templates WHERE key = 'video.constraint_lock'" in statements
    assert "UPDATE generation_jobs" in statements
    assert "UPDATE video_generation_jobs" in statements
    assert "UPDATE pipeline_stages" in statements
    assert "UPDATE pipeline_jobs" in statements
    assert statements.index("DROP TABLE IF EXISTS art_style_extraction_jobs") < statements.index(
        "DROP TABLE IF EXISTS art_styles"
    )
    assert "ALTER TABLE scripts DROP COLUMN IF EXISTS unify_style" in statements
    assert "ALTER TABLE subjects DROP COLUMN IF EXISTS enrich_version" in statements


def test_check_only_detects_retired_art_style_objects(monkeypatch) -> None:
    """只读门禁必须拒绝仍保留已下线表、列和模板数据的旧库。"""
    from app.db import drift_repair

    existing = {"art_styles", "prompt_templates", "scripts", "subjects"}
    column_names = {
        "scripts": {"visual_style"},
        "subjects": {"enrich_version"},
    }

    monkeypatch.setattr(drift_repair, "_existing_tables", lambda _conn: existing)
    monkeypatch.setattr(drift_repair, "_detect_missing_tables", lambda _conn, _tables: [])
    monkeypatch.setattr(
        drift_repair,
        "_existing_columns",
        lambda _conn, table: column_names.get(table, set()),
    )
    monkeypatch.setattr(drift_repair, "_existing_constraints", lambda _conn, _table: set())
    monkeypatch.setattr(drift_repair, "_detect_general_missing_columns", lambda _conn, _tables: [])

    class _DetectionConnection:
        def execute(self, statement: object) -> _ScalarResult:
            return _ScalarResult("video.constraint_lock" in str(statement))

    report = drift_repair._detect(_DetectionConnection())

    assert report.retired_tables == ["art_styles"]
    assert report.retired_columns == ["scripts.visual_style", "subjects.enrich_version"]
    assert report.retired_data == ["prompt_templates.video.constraint_lock"]
    assert report.has_drift


def test_repair_uses_guarded_connection_for_retired_json_cleanup(monkeypatch) -> None:
    """运维清理受存储引用保护的 JSON 列时必须显式使用受控 bypass。"""
    from app.db import drift_repair

    class _Connection:
        def __init__(self) -> None:
            self.execution_options_values: dict[str, bool] = {}

        def execution_options(self, **values: bool) -> _Connection:
            self.execution_options_values.update(values)
            return self

    class _Engine:
        def __init__(self) -> None:
            self.connection = _Connection()
            self.disposed = False

        @contextmanager
        def begin(self):
            yield self.connection

        def dispose(self) -> None:
            self.disposed = True

    engine = _Engine()
    monkeypatch.setattr(drift_repair, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(drift_repair, "_existing_tables", lambda _conn: set())
    monkeypatch.setattr(drift_repair, "_detect_missing_tables", lambda _conn, _tables: [])
    monkeypatch.setattr(drift_repair, "_repair_missing_tables", lambda _conn, _missing: None)
    monkeypatch.setattr(drift_repair, "_repair_known_drift_columns", lambda _conn: [])
    monkeypatch.setattr(drift_repair, "_repair_known_drift_constraints", lambda _conn: [])
    monkeypatch.setattr(drift_repair, "_repair_retired_art_style_artifacts", lambda _conn, _tables: None)
    monkeypatch.setattr(drift_repair, "_existing_tables_from_engine", lambda _engine: set())
    monkeypatch.setattr(
        drift_repair,
        "_detect",
        lambda _conn: drift_repair.DriftReport([], [], [], [], [], [], [], False),
    )

    report = drift_repair.run_repair()

    assert not report.has_drift
    assert engine.connection.execution_options_values == {
        drift_repair.STORAGE_REFERENCE_GUARD_BYPASS: True
    }
    assert engine.disposed


def test_only_known_retired_alembic_revision_can_be_mapped_to_baseline() -> None:
    """删除错误迁移后，只允许把明确已知的旧 revision 回正到唯一基线。"""
    from app.db import drift_repair

    assert drift_repair._resolve_legacy_revision_replacement({"e8a1f2b3c4d5"}) == "d4b7e1c9a362"
    assert drift_repair._resolve_legacy_revision_replacement({"d4b7e1c9a362"}) is None
    with pytest.raises(RuntimeError, match="未知或混合"):
        drift_repair._resolve_legacy_revision_replacement({"e8a1f2b3c4d5", "unknown"})


def test_migrate_services_normalize_known_revision_before_alembic_upgrade() -> None:
    """远端 deploy 使用的 migrate 服务必须先完成受控 revision 映射。"""
    backend_dir = _VERSIONS_DIR.parents[1]
    expected = "python -m app.db.drift_repair --normalize-alembic-revision && alembic upgrade head"

    for compose_path in (
        backend_dir / "docker-compose.production.yml",
        backend_dir / "docker-compose.test.yml",
    ):
        assert expected in compose_path.read_text(encoding="utf-8")
