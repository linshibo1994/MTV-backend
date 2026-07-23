"""流水线轮询查询与独立进度检查点专项测试。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from sqlalchemy.dialects import postgresql

from app.services.pipeline import commit_stage_progress
from app.services.pipeline import database_core as pipeline_database
from app.services.pipeline.catalog import get_subject_catalog_state
from app.services.pipeline.orchestrator import get_pipeline_status


class _RowsResult:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def all(self) -> list[object]:
        return self._rows


class _ScalarResult:
    def __init__(self, value: object) -> None:
        self._value = value

    def scalar(self) -> object:
        return self._value


async def test_pipeline_status_uses_one_lightweight_column_query() -> None:
    row = SimpleNamespace(
        stage="extract",
        status="running",
        detail={"count": 1},
        current=1,
        total=2,
        error=None,
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=_RowsResult([row])))

    status = await get_pipeline_status(db, "script-1")

    assert db.execute.await_count == 1
    statement = db.execute.await_args.args[0]
    selected_keys = set(statement.selected_columns.keys())
    assert selected_keys == {"stage", "status", "detail", "current", "total", "error"}
    assert status["running"] is True
    assert status["stages"][1]["current"] == 1
    assert status["stages"][0]["status"] == "pending"


async def test_catalog_query_extracts_publish_flag_without_selecting_full_output() -> None:
    statements: list[object] = []

    async def execute(statement):
        statements.append(statement)
        if len(statements) == 1:
            return _RowsResult([])
        return _ScalarResult(0)

    result = await get_subject_catalog_state(
        SimpleNamespace(execute=execute),
        "script-1",
    )

    assert result["state"] == "untracked"
    assert len(statements) == 2
    history_query = statements[0]
    selected_keys = set(history_query.selected_columns.keys())
    assert "output_json" not in selected_keys
    assert "subjects_published" in selected_keys
    sql = str(
        history_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "pipeline_jobs.output_json ->> 'subjects_published'" in sql
    assert "SELECT pipeline_jobs.output_json," not in sql


async def test_commit_stage_progress_uses_independent_monotonic_update(monkeypatch) -> None:
    executed: list[object] = []

    class _UpdateResult:
        rowcount = 1

    class _ProgressSession:
        commit = AsyncMock()

        async def execute(self, statement):
            executed.append(statement)
            return _UpdateResult()

    progress_session = _ProgressSession()

    class _SessionContext:
        async def __aenter__(self):
            return progress_session

        async def __aexit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(
        pipeline_database,
        "async_session_factory",
        lambda: _SessionContext(),
    )

    updated = await commit_stage_progress(
        "script-1",
        "enrich",
        status="running",
        current=3,
        total=10,
        detail={
            "count": 3,
            "input_version": 7,
            "input_client_save_seq": 11,
        },
    )

    assert updated is True
    progress_session.commit.assert_awaited_once()
    assert len(executed) == 1
    compiled = executed[0].compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "CASE WHEN" in sql
    assert "pipeline_stages.current <" in sql
    assert "pipeline_stages.total <" in sql
    assert "pipeline_stages.status =" in sql
    assert "input_version" in compiled.params.values()
    assert "input_client_save_seq" in compiled.params.values()
