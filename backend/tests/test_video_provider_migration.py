"""视频供应商下线替换的数据库漂移修复契约测试。"""

from __future__ import annotations


class _ConnectionRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.statements.append(str(statement))


def test_provider_constraint_rebuild_migrates_rows_between_drop_and_add(monkeypatch) -> None:
    """旧 CHECK 不接受 entpilot 时，数据 UPDATE 必须发生在 DROP 之后。"""

    from app.db import drift_repair

    recorder = _ConnectionRecorder()
    events: list[str] = []
    old_definitions = {
        "video_provider_configs": "CHECK (provider_key IN ('mgrouter'))",
        "video_generation_jobs": "CHECK (provider_key IN ('mgrouter'))",
    }
    constraint_names = {
        "ck_video_provider_configs_provider_key",
        "ck_video_jobs_provider_key",
    }

    def old_constraint_definition(_conn, table: str) -> dict[str, str]:
        constraint_name = next(
            name
            for item, name in drift_repair._VIDEO_PROVIDER_KEY_CONSTRAINTS
            if item == table
        )
        return {constraint_name: old_definitions[table]}

    monkeypatch.setattr(
        drift_repair,
        "_existing_constraint_definitions",
        old_constraint_definition,
    )
    monkeypatch.setattr(
        drift_repair,
        "_migrate_retired_video_provider_data",
        lambda _conn, _existing: events.append("migrate"),
    )
    monkeypatch.setattr(
        drift_repair,
        "_existing_constraints",
        lambda _conn, _table: constraint_names,
    )

    assert drift_repair._repair_video_provider_key_constraints(
        recorder,
        set(old_definitions),
    ) == []

    assert events == ["migrate"]
    drop_indexes = [
        index
        for index, statement in enumerate(recorder.statements)
        if "DROP CONSTRAINT" in statement
    ]
    add_indexes = [
        index
        for index, statement in enumerate(recorder.statements)
        if "ADD CONSTRAINT" in statement
    ]
    assert drop_indexes
    assert add_indexes
    assert max(drop_indexes) < min(add_indexes)


def test_provider_constraint_rebuild_is_idempotent_when_definition_is_current(monkeypatch) -> None:
    """已完成替换的数据库不应重复执行约束 DDL。"""

    from app.db import drift_repair

    recorder = _ConnectionRecorder()
    calls: list[str] = []
    current_definition = (
        "CHECK (provider_key IN ('volcengine', 'neolink', 'entpilot'))"
    )

    monkeypatch.setattr(
        drift_repair,
        "_existing_constraint_definitions",
        lambda _conn, _table: {
            "ck_video_provider_configs_provider_key": current_definition,
            "ck_video_jobs_provider_key": current_definition,
        },
    )
    monkeypatch.setattr(
        drift_repair,
        "_migrate_retired_video_provider_data",
        lambda _conn, _existing: calls.append("migrate"),
    )

    assert drift_repair._repair_video_provider_key_constraints(
        recorder,
        {"video_provider_configs", "video_generation_jobs"},
    ) == []
    assert calls == ["migrate"]
    assert recorder.statements == []


def test_retired_provider_data_normalizes_all_legacy_models_before_key_update() -> None:
    """旧供应商的自定义模型也必须在切换前归一为合法 EntPilot 模型。"""

    from app.db import drift_repair

    recorder = _ConnectionRecorder()
    drift_repair._migrate_retired_video_provider_data(
        recorder,
        {"video_provider_configs", "video_generation_jobs"},
    )

    statements = recorder.statements
    config_update = next(
        statement
        for statement in statements
        if "UPDATE video_provider_configs" in statement
        and "SET provider_key = :new_key" in statement
    )
    assert "model = :default_model" in config_update
    assert "lower(model) LIKE" not in config_update

    model_updates = [
        (index, statement)
        for index, statement in enumerate(statements)
        if "'{model}'" in statement
    ]
    provider_key_update = next(
        index
        for index, statement in enumerate(statements)
        if "UPDATE video_generation_jobs" in statement
        and "SET provider_key = :new_key" in statement
    )
    assert len(model_updates) == 3
    assert all(index < provider_key_update for index, _ in model_updates)
    assert all("provider_key = :legacy_key" in statement for _, statement in model_updates)
