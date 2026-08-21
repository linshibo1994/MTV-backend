"""AC-SU05：人工锁字段与流水线差异建议专项测试。"""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from app.api.v1 import asset_library
from app.core.errors import ConflictError
from app.crud import asset as asset_crud
from app.models.subject import Subject
from app.models.subject_change_suggestion import SubjectChangeSuggestion
from app.schemas.asset import SubjectSuggestionResolveIn, SubjectUpdateIn
from app.services.pipeline import merge


class _Rows:
    def __init__(self, rows=None, scalar=None) -> None:
        self.rows = list(rows or [])
        self.scalar = scalar

    def scalars(self):
        return self

    def all(self):
        return self.rows

    def scalar_one_or_none(self):
        return self.scalar


class _QueueDB:
    def __init__(self, *results) -> None:
        self.results = list(results)
        self.added: list[object] = []
        self.flushes = 0

    async def execute(self, _statement):
        return self.results.pop(0)

    def add(self, row) -> None:
        self.added.append(row)

    async def flush(self) -> None:
        self.flushes += 1


def _subject(**overrides) -> Subject:
    values = {
        "id": "subject-1",
        "script_id": "script-1",
        "type": "character",
        "name": "人工名称",
        "pipeline_name": "模型新名称",
        "description": "人工描述",
        "subject_version": 3,
        "name_locked": True,
        "description_locked": True,
        "is_child": True,
        "is_parent": False,
        "episode_nos": [1],
        "is_active": True,
        "review_status": "not_designed",
    }
    values.update(overrides)
    return Subject(**values)


class _ConcurrentIdentityDB:
    """用父行 asyncio.Lock 模拟 PostgreSQL 事务级 FOR UPDATE。"""

    def __init__(self, subject: Subject, subjects: list[Subject], script_lock: asyncio.Lock) -> None:
        self.subject = subject
        self.subjects = subjects
        self.script_lock = script_lock
        self.owns_script_lock = False
        self.sql: list[str] = []

    async def execute(self, statement):
        sql = str(statement.compile(dialect=postgresql.dialect()))
        self.sql.append(sql)
        if "FROM scripts" in sql:
            await self.script_lock.acquire()
            self.owns_script_lock = True
            return _Rows(scalar="script-1")
        if "FROM subjects" in sql and "FOR UPDATE" in sql:
            return _Rows(scalar=self.subject)
        if "FROM subjects" in sql:
            duplicate = next(
                (
                    item.id
                    for item in self.subjects
                    if item.id != self.subject.id
                    and item.type == self.subject.type
                    and item.name == "统一名称"
                ),
                None,
            )
            return _Rows(scalar=duplicate)
        raise AssertionError(f"未预期 SQL: {sql}")

    async def flush(self) -> None:
        return None

    async def commit(self) -> None:
        if self.owns_script_lock:
            self.owns_script_lock = False
            self.script_lock.release()

    async def rollback(self) -> None:
        await self.commit()


async def test_manual_edit_auto_locks_changed_fields_and_checks_version() -> None:
    subject = _subject(
        name="旧名称",
        description="旧描述",
        subject_version=1,
        name_locked=False,
        description_locked=False,
    )
    db = _QueueDB(_Rows(scalar=None))

    await asset_crud.update_subject(
        db,
        subject,
        {"name": "新名称", "description": "新描述"},
        expected_subject_version=1,
    )

    assert subject.name == "新名称"
    assert subject.description == "新描述"
    assert subject.name_locked is True
    assert subject.description_locked is True
    assert subject.subject_version == 2
    assert not hasattr(subject, "enrich_version")

    with pytest.raises(ConflictError, match="主体版本已变化"):
        await asset_crud.update_subject(
            db,
            subject,
            {"name": "过期写入"},
            expected_subject_version=1,
        )


async def test_script_then_subject_lock_serializes_concurrent_identity_updates() -> None:
    subjects = [
        _subject(id="subject-1", name="角色甲", pipeline_name="模型甲"),
        _subject(id="subject-2", name="角色乙", pipeline_name="模型乙"),
    ]
    script_lock = asyncio.Lock()
    sessions = [
        _ConcurrentIdentityDB(subject, subjects, script_lock) for subject in subjects
    ]

    async def update_identity(db: _ConcurrentIdentityDB) -> str:
        try:
            subject = await asset_crud.get_subject_for_identity_update(
                db, "script-1", db.subject.id
            )
            assert subject is db.subject
            await asset_crud.update_subject(db, subject, {"name": "统一名称"})
            await db.commit()
            return "200"
        except ConflictError as exc:
            await db.rollback()
            return str(exc.status_code)

    results = await asyncio.gather(*(update_identity(db) for db in sessions))

    assert sorted(results) == ["200", "409"]
    assert [subject.name for subject in subjects].count("统一名称") == 1
    for db in sessions:
        lock_sql = [sql for sql in db.sql if "FOR UPDATE" in sql]
        assert "FROM scripts" in lock_sql[0]
        assert "FROM subjects" in lock_sql[1]


async def test_merge_preserves_locked_values_and_stages_differences() -> None:
    subject = _subject()
    db = _QueueDB(_Rows([subject]))

    staged = await merge._prepare_staged_subjects(
        db,
        "script-1",
        [
            {
                "type": "character",
                "name": "模型新名称",
                "description": "模型新描述",
                "episode_nos": [1, 2],
                "is_child": True,
            }
        ],
    )

    assert len(staged) == 1
    item = staged[0]
    assert item["id"] == "subject-1"
    assert item["name"] == "人工名称"
    assert item["description"] == "人工描述"
    assert item["episode_nos"] == [1, 2]
    assert item["subject_version"] == 4
    assert item["field_suggestions"] == [
        {
            "field": "name",
            "current_value": "人工名称",
            "suggested_value": "模型新名称",
        },
        {
            "field": "description",
            "current_value": "人工描述",
            "suggested_value": "模型新描述",
        },
    ]


async def test_merge_uses_stable_pipeline_names_for_same_type_same_episode() -> None:
    first = _subject(
        id="subject-1",
        name="人工甲",
        pipeline_name="模型甲",
        episode_nos=[1],
    )
    second = _subject(
        id="subject-2",
        name="人工乙",
        pipeline_name="模型乙",
        episode_nos=[1],
    )
    db = _QueueDB(_Rows([first, second]))

    staged = await merge._prepare_staged_subjects(
        db,
        "script-1",
        [
            {
                "type": "character",
                "name": "模型甲",
                "description": "甲的新描述",
                "episode_nos": [1],
                "is_child": True,
            },
            {
                "type": "character",
                "name": "模型乙",
                "description": "乙的新描述",
                "episode_nos": [1],
                "is_child": True,
            },
        ],
    )

    assert [item["id"] for item in staged] == ["subject-1", "subject-2"]
    assert [item["name"] for item in staged] == ["人工甲", "人工乙"]
    assert [item["field_suggestions"][0]["suggested_value"] for item in staged] == [
        "模型甲",
        "模型乙",
    ]


async def test_ambiguous_merge_keeps_unmatched_locked_subjects() -> None:
    first = _subject(id="subject-1", name="甲", episode_nos=[1])
    second = _subject(id="subject-2", name="乙", episode_nos=[1])
    db = _QueueDB(_Rows([first, second]))

    staged = await merge._prepare_staged_subjects(
        db,
        "script-1",
        [
            {
                "type": "character",
                "name": "无法唯一匹配",
                "description": "模型描述",
                "episode_nos": [1],
                "is_child": True,
            }
        ],
    )

    assert {item["name"] for item in staged} == {"无法唯一匹配", "甲", "乙"}
    assert {item["id"] for item in staged if item["name"] in {"甲", "乙"}} == {
        "subject-1",
        "subject-2",
    }


async def test_persist_suggestions_is_idempotent_and_uses_published_version() -> None:
    db = _QueueDB(_Rows(scalar=None))
    staged = [
        {
            "id": "subject-1",
            "base_subject_version": 3,
            "subject_version": 4,
            "field_suggestions": [
                {
                    "field": "name",
                    "current_value": "人工名称",
                    "suggested_value": "模型名称",
                }
            ],
        }
    ]

    created = await asset_crud.persist_subject_suggestions(
        db,
        "script-1",
        staged,
        source_run_id="run-1",
    )

    assert len(created) == 1
    assert created[0].base_subject_version == 4
    assert created[0].status == "pending"
    assert db.added == created

    existing = created[0]
    duplicate_db = _QueueDB(_Rows([existing]))
    assert not await asset_crud.persist_subject_suggestions(
        duplicate_db,
        "script-1",
        staged,
        source_run_id="run-2",
    )
    assert duplicate_db.added == []


async def test_new_subject_version_supersedes_old_pending_same_value() -> None:
    old = SubjectChangeSuggestion(
        id="suggestion-old",
        script_id="script-1",
        subject_id="subject-1",
        field="name",
        current_value="人工名称",
        suggested_value="模型名称",
        base_subject_version=4,
        status="pending",
    )
    db = _QueueDB(_Rows([old]))

    created = await asset_crud.persist_subject_suggestions(
        db,
        "script-1",
        [
            {
                "id": "subject-1",
                "subject_version": 5,
                "field_suggestions": [
                    {
                        "field": "name",
                        "current_value": "人工名称",
                        "suggested_value": "模型名称",
                    }
                ],
            }
        ],
        source_run_id="run-new",
    )

    assert old.status == "ignored"
    assert old.resolved_at is not None
    assert len(created) == 1
    assert created[0].base_subject_version == 5
    assert created[0].status == "pending"


async def test_accept_suggestion_updates_locked_field_and_version() -> None:
    subject = _subject(subject_version=4)
    suggestion = SubjectChangeSuggestion(
        id="suggestion-1",
        script_id="script-1",
        subject_id="subject-1",
        field="name",
        current_value="人工名称",
        suggested_value="接受后的名称",
        base_subject_version=4,
        status="pending",
    )
    db = _QueueDB(_Rows(scalar=suggestion), _Rows(scalar=None))

    result = await asset_crud.resolve_subject_suggestion(
        db,
        subject,
        "suggestion-1",
        action="accept",
        expected_subject_version=4,
        actor_user_id="user-1",
    )

    assert result.status == "accepted"
    assert result.resolved_by_user_id == "user-1"
    assert result.resolved_at is not None
    assert subject.name == "接受后的名称"
    assert subject.name_locked is True
    assert subject.subject_version == 5
    assert not hasattr(subject, "enrich_version")


async def test_stale_suggestion_cannot_overwrite_new_manual_value() -> None:
    subject = _subject(subject_version=5, name="更新后的人工名称")
    suggestion = SubjectChangeSuggestion(
        id="suggestion-1",
        script_id="script-1",
        subject_id="subject-1",
        field="name",
        current_value="人工名称",
        suggested_value="旧模型建议",
        base_subject_version=4,
        status="pending",
    )
    db = _QueueDB(_Rows(scalar=suggestion))

    with pytest.raises(ConflictError, match="差异建议已过期"):
        await asset_crud.resolve_subject_suggestion(
            db,
            subject,
            "suggestion-1",
            action="accept",
            expected_subject_version=5,
            actor_user_id="user-1",
        )
    assert subject.name == "更新后的人工名称"
    assert suggestion.status == "pending"


async def test_resolve_api_requires_edit_and_writes_audit(monkeypatch) -> None:
    user = SimpleNamespace(user_id="user-1", username="alice")
    subject = _subject(subject_version=4)
    now = datetime.now(UTC)
    suggestion = SubjectChangeSuggestion(
        id="suggestion-1",
        script_id="script-1",
        subject_id="subject-1",
        field="description",
        current_value="人工描述",
        suggested_value="模型描述",
        base_subject_version=4,
        status="ignored",
        created_at=now,
        updated_at=now,
    )
    db = SimpleNamespace(commit=AsyncMock())
    load_script = AsyncMock()
    audit = AsyncMock()
    monkeypatch.setattr(asset_library, "_load_script_or_403", load_script)
    monkeypatch.setattr(
        asset_library.asset_crud,
        "get_subject_for_identity_update",
        AsyncMock(return_value=subject),
    )
    monkeypatch.setattr(
        asset_library.asset_crud,
        "resolve_subject_suggestion",
        AsyncMock(return_value=suggestion),
    )
    monkeypatch.setattr(asset_library, "audit_log", audit)

    await asset_library.resolve_subject_suggestion(
        "script-1",
        "subject-1",
        "suggestion-1",
        SubjectSuggestionResolveIn(action="ignore", expected_subject_version=4),
        db=db,
        user=user,
    )

    assert load_script.await_args.kwargs["action"].value == "edit"
    assert audit.await_args.kwargs["action"] == "subject_suggestion_ignore"
    assert audit.await_args.kwargs["resource_id"] == "suggestion-1"
    db.commit.assert_awaited_once()


async def test_manual_update_audits_lock_and_version_change(monkeypatch) -> None:
    user = SimpleNamespace(user_id="user-1", username="alice")
    subject = _subject(
        name="旧名称",
        subject_version=1,
        name_locked=False,
    )
    db = SimpleNamespace(commit=AsyncMock())
    audit = AsyncMock()
    monkeypatch.setattr(asset_library, "_load_script_or_403", AsyncMock())
    monkeypatch.setattr(
        asset_library.asset_crud,
        "get_subject_for_identity_update",
        AsyncMock(return_value=subject),
    )

    async def update(_db, target, data, *, expected_subject_version):
        assert expected_subject_version == 1
        target.name = data["name"]
        target.name_locked = True
        target.subject_version = 2
        return target

    monkeypatch.setattr(asset_library.asset_crud, "update_subject", update)
    monkeypatch.setattr(
        asset_library.asset_crud,
        "get_subject",
        AsyncMock(return_value=subject),
    )
    monkeypatch.setattr(
        asset_library.asset_crud,
        "get_episode_id_map",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        asset_library.asset_crud,
        "serialize_subject",
        lambda *_args: SimpleNamespace(subject_name=subject.name),
    )
    monkeypatch.setattr(asset_library, "audit_log", audit)

    result = await asset_library.update_subject(
        "script-1",
        "subject-1",
        SubjectUpdateIn(name="新名称", expected_subject_version=1),
        db=db,
        user=user,
    )

    assert result.subject_name == "新名称"
    payload = audit.await_args.kwargs
    assert payload["action"] == "subject_update"
    assert payload["before"]["name_locked"] is False
    assert payload["after"]["name_locked"] is True
    assert payload["after"]["subject_version"] == 2


def test_subject_update_schema_rejects_blank_locked_text() -> None:
    with pytest.raises(ValueError):
        SubjectUpdateIn(name="   ", expected_subject_version=1)
