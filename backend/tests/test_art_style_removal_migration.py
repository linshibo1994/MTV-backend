"""项目动态画风下线迁移契约。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "e8a1f2b3c4d5_下线项目动态画风.py"
)


class _OperationRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def execute(self, statement: object) -> None:
        self.calls.append(("execute", str(statement)))

    def f(self, name: str) -> str:
        return name

    def drop_table(self, table_name: str) -> None:
        self.calls.append(("drop_table", table_name))

    def drop_column(self, table_name: str, column_name: str) -> None:
        self.calls.append(("drop_column", f"{table_name}.{column_name}"))

    def create_table(self, table_name: str, *_columns: object, **_kwargs: object) -> None:
        self.calls.append(("create_table", table_name))

    def create_index(self, _name: str, table_name: str, *_columns: object, **_kwargs: object) -> None:
        self.calls.append(("create_index", table_name))

    def add_column(self, table_name: str, column: object) -> None:
        self.calls.append(("add_column", f"{table_name}.{getattr(column, 'name', '')}"))

    def alter_column(self, table_name: str, column_name: str, **_kwargs: object) -> None:
        self.calls.append(("alter_column", f"{table_name}.{column_name}"))


def _load_migration() -> ModuleType:
    assert _MIGRATION_PATH.is_file(), "缺少项目动态画风下线迁移"
    spec = importlib.util.spec_from_file_location("art_style_removal_migration", _MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_art_style_removal_migration_cleans_legacy_data_and_recreates_schema() -> None:
    """升级销毁旧画风数据；降级只恢复空 schema，不能恢复已删除数据。"""
    migration = _load_migration()
    recorder = _OperationRecorder()
    migration.op = recorder

    migration.upgrade()

    assert ("execute", "DELETE FROM task_dispatch_outbox WHERE task_type = 'art_style.extract'") in (
        recorder.calls
    )
    assert (
        "execute",
        "UPDATE generation_jobs "
        "SET input_json = input_json - 'style_id' - 'art_style_id' - 'visual_style' - 'unify_style' "
        "- 'resolved_prompts' - 'enrich_version' "
        "WHERE input_json ?| ARRAY['style_id', 'art_style_id', 'visual_style', 'unify_style', "
        "'resolved_prompts', 'enrich_version']",
    ) in recorder.calls
    assert (
        "execute",
        "UPDATE video_generation_jobs "
        "SET input_json = input_json - 'style_id' - 'art_style_id' - 'visual_style' - 'unify_style' "
        "- 'constraint_prompt_template' "
        "WHERE input_json ?| ARRAY['style_id', 'art_style_id', 'visual_style', 'unify_style', "
        "'constraint_prompt_template']",
    ) in recorder.calls
    assert ("execute", "DELETE FROM prompt_templates WHERE key = 'video.constraint_lock'") in recorder.calls
    assert (
        "execute",
        "UPDATE pipeline_stages "
        "SET detail = detail - 'prompts' "
        "WHERE stage = 'enrich' AND detail ? 'prompts'",
    ) in recorder.calls
    assert (
        "execute",
        "UPDATE pipeline_jobs "
        "SET output_json = output_json - 'prompts' "
        "WHERE stage = 'enrich' AND output_json ? 'prompts'",
    ) in recorder.calls
    assert (
        "execute",
        "UPDATE pipeline_jobs "
        "SET output_json = jsonb_set("
        "output_json, "
        "'{subjects}', "
        "(SELECT jsonb_agg(item - 'base_enrich_version' - 'enrich_version' ORDER BY ordinal) "
        "FROM jsonb_array_elements(output_json -> 'subjects') WITH ORDINALITY AS subjects(item, ordinal))"
        ") "
        "WHERE stage = 'merge' "
        "AND jsonb_typeof(output_json -> 'subjects') = 'array' "
        "AND EXISTS ("
        "SELECT 1 FROM jsonb_array_elements(output_json -> 'subjects') AS subjects(item) "
        "WHERE item ?| ARRAY['base_enrich_version', 'enrich_version']"
        ")",
    ) in recorder.calls
    assert ("execute", "DELETE FROM art_style_extraction_jobs") in recorder.calls
    assert ("execute", "DELETE FROM art_styles") in recorder.calls
    assert recorder.calls.index(("execute", "DELETE FROM art_style_extraction_jobs")) < recorder.calls.index(
        ("execute", "DELETE FROM art_styles")
    )
    assert recorder.calls.index(("drop_table", "art_style_extraction_jobs")) < recorder.calls.index(
        ("drop_table", "art_styles")
    )
    assert ("drop_column", "subjects.visual_style") in recorder.calls
    assert ("drop_column", "subjects.enrich_version") in recorder.calls
    assert ("drop_column", "scripts.visual_style") in recorder.calls
    assert ("drop_column", "scripts.unify_style") in recorder.calls

    recorder.calls.clear()
    migration.downgrade()

    assert ("create_table", "art_styles") in recorder.calls
    assert ("create_table", "art_style_extraction_jobs") in recorder.calls
    assert ("add_column", "scripts.visual_style") in recorder.calls
    assert ("add_column", "scripts.unify_style") in recorder.calls
    assert ("add_column", "subjects.visual_style") in recorder.calls
    assert ("add_column", "subjects.enrich_version") in recorder.calls
    assert ("alter_column", "subjects.enrich_version") in recorder.calls
