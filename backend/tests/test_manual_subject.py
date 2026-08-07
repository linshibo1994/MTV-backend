"""手动新增资产主体的专项测试。"""

from collections.abc import AsyncIterator
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.api.v1 import asset_library
from app.core.errors import ForbiddenError
from app.core.permissions import Action
from app.models.asset_entry import AssetEntry
from app.models.audit_event import AuditEvent
from app.models.base import Base
from app.models.episode import Episode
from app.models.materials import Materials
from app.models.parent_subject import ParentSubject
from app.models.pipeline_stage import PipelineStage
from app.models.scene import Scene
from app.models.script import Script
from app.models.subject import Subject
from app.models.user import User
from app.schemas.asset import SubjectCreateIn
from app.schemas.materials import build_subject_prompt


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncSessionAdapter:
    """以真实 SQLite 事务承载路由与 CRUD 的异步调用。"""

    def __init__(self, session: Session) -> None:
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.session.commit()

    async def rollback(self) -> None:
        self.session.rollback()

    def add(self, row) -> None:
        self.session.add(row)


def _create_tables(engine) -> None:
    Base.metadata.create_all(
        engine,
        tables=[
            Script.__table__,
            Episode.__table__,
            Scene.__table__,
            ParentSubject.__table__,
            Subject.__table__,
            PipelineStage.__table__,
            AssetEntry.__table__,
            Materials.__table__,
            AuditEvent.__table__,
        ],
    )


def _script() -> Script:
    return Script(
        id="script-1",
        title="剧本",
        scope="personal",
        owner_user_id="user-1",
        created_by_user_id="user-1",
    )


def _episode(episode_no: int, episode_id: str) -> Episode:
    return Episode(
        episode_id=episode_id,
        script_id="script-1",
        episode_no=episode_no,
        title=f"第{episode_no}集",
    )


def _owner() -> User:
    return User(
        user_id="user-1",
        username="owner",
        password_hash="test",
        enabled=True,
    )


def _app(db: _AsyncSessionAdapter, user: User) -> FastAPI:
    app = FastAPI()
    app.include_router(asset_library.router)

    async def _db_override() -> AsyncIterator[_AsyncSessionAdapter]:
        yield db

    app.dependency_overrides[asset_library.get_db] = _db_override
    app.dependency_overrides[asset_library.get_current_user] = lambda: user
    return app


@pytest.mark.asyncio
async def test_create_background_subject_normalizes_and_writes_audit() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.add_all([_script(), _episode(1, "episode-1"), _episode(2, "episode-2")])
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async with AsyncClient(
            transport=ASGITransport(app=_app(db, _owner())), base_url="http://test"
        ) as client:
            response = await client.post(
                "/scripts/script-1/asset-library/subjects",
                json={
                    "type": "background",
                    "name": "  雨夜码头  ",
                    "description": "  雨夜的旧码头，蓝灰色调  ",
                    "episode_nos": [2, 1],
                },
            )
            listed = await client.get(
                "/scripts/script-1/asset-library/subjects",
                params={"limit": 30, "offset": 0},
            )

        assert response.status_code == 201
        body = response.json()
        assert body["subject_name"] == "雨夜码头"
        assert body["subject_type"] == "background"
        assert body["episode_nos"] == [1, 2]
        assert body["episode_ids"] == ["episode-1", "episode-2"]
        assert body["review_status"] == "not_designed"
        assert body["design_prompt_source"] == "template_fallback"
        assert "【2 空间布局】" in body["design_prompt"]
        assert listed.status_code == 200
        assert listed.json()["items"][0]["subject_id"] == body["subject_id"]

        subject = raw.scalar(select(Subject))
        assert subject is not None
        assert subject.type == "scene"
        assert subject.description == "雨夜的旧码头，蓝灰色调"
        assert subject.is_child is True
        assert subject.is_active is True
        assert subject.image_entry_id is None

        event = raw.scalar(select(AuditEvent).where(AuditEvent.action == "subject_create"))
        assert event is not None
        assert event.actor_user_id == "user-1"
        assert event.resource_id == subject.id
        assert event.before is None
        assert event.after == {
            "script_id": "script-1",
            "type": "scene",
            "name": "雨夜码头",
            "description": "雨夜的旧码头，蓝灰色调",
            "episode_nos": [1, 2],
            "review_status": "not_designed",
        }


@pytest.mark.asyncio
async def test_subject_list_prefers_current_enrich_prompt_and_rejects_stale_entry() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        episode = _episode(1, "episode-1")
        subject = Subject(
            id="subject-1",
            script_id="script-1",
            type="character",
            name="老农",
            description="老年男性，满脸皱纹，穿粗布衣",
            episode_nos=[1],
            subject_version=2,
            enrich_version=3,
            review_status="not_designed",
        )
        stage = PipelineStage(
            id="stage-enrich",
            script_id="script-1",
            stage="enrich",
            status="done",
            detail={
                "prompts": {
                    subject.id: {
                        "subject_version": 2,
                        "enrich_version": 3,
                        "prompt_template_version": 2,
                        "resolved_prompts": {
                            "final": "服务端生成的老农设定提示词。\n"
                            + build_subject_prompt(
                                "老农",
                                "character",
                                "年迈农民，稳定面容，穿粗布衣与草帽",
                            )
                        },
                        "episode_snapshot": {
                            "1": {
                                "episode_id": episode.episode_id,
                                "episode_version": 1,
                                "client_save_seq": 0,
                            }
                        },
                        "source": "generated",
                    }
                }
            },
        )
        raw.add_all([_script(), episode, subject, stage])
        raw.commit()

        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _owner())),
            base_url="http://test",
        ) as client:
            current = await client.get("/scripts/script-1/asset-library/subjects")

            stage.detail = {
                **stage.detail,
                "prompts": {
                    subject.id: {
                        **stage.detail["prompts"][subject.id],
                        "prompt_template_version": 1,
                    }
                },
            }
            raw.commit()
            old_template = await client.get("/scripts/script-1/asset-library/subjects")

            stage.detail = {
                **stage.detail,
                "prompts": {
                    subject.id: {
                        **stage.detail["prompts"][subject.id],
                        "prompt_template_version": 2,
                        "subject_version": 1,
                    }
                },
            }
            raw.commit()
            stale = await client.get("/scripts/script-1/asset-library/subjects")

        assert current.status_code == 200
        current_item = current.json()["items"][0]
        assert "服务端生成的老农设定提示词" in current_item["design_prompt"]
        assert "【视觉资产设定提示词】" in current_item["design_prompt"]
        assert "【人物设定图构图硬约束】" in current_item["design_prompt"]
        assert current_item["design_prompt_source"] == "generated"
        assert current_item["design_prompt_version"] == 3
        assert current_item["design_prompt_template_version"] == 2

        assert old_template.status_code == 200
        old_template_item = old_template.json()["items"][0]
        assert old_template_item["design_prompt_source"] == "template_fallback"
        assert "服务端生成的老农设定提示词" not in old_template_item["design_prompt"]

        assert stale.status_code == 200
        stale_item = stale.json()["items"][0]
        assert stale_item["design_prompt_source"] == "template_fallback"
        assert "请为以下角色制作" in stale_item["design_prompt"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("input_type", "stored_type", "output_type"),
    [
        ("character", "character", "character"),
        ("scene", "scene", "background"),
        ("prop", "prop", "prop"),
        ("sound", "sound", "sound"),
    ],
)
async def test_create_subject_supports_every_documented_type(
    input_type: str,
    stored_type: str,
    output_type: str,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.add_all([_script(), _episode(1, "episode-1")])
        raw.commit()

        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _owner())),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/scripts/script-1/asset-library/subjects",
                json={
                    "type": input_type,
                    "name": f"测试{input_type}",
                    "description": f"{input_type} 类型的完整描述",
                    "episode_nos": [1],
                },
            )

        assert response.status_code == 201
        body = response.json()
        assert body["subject_type"] == output_type
        if output_type == "sound":
            assert body["design_prompt"] is None
            assert body["design_prompt_source"] is None
        else:
            assert body["design_prompt"]
            assert body["design_prompt_source"] == "template_fallback"
        subject = raw.scalar(select(Subject))
        assert subject is not None
        assert subject.type == stored_type
        assert subject.review_status == "not_designed"


@pytest.mark.asyncio
async def test_scene_and_background_names_share_conflict_domain() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.add_all(
            [
                _script(),
                _episode(1, "episode-1"),
                Subject(
                    id="subject-existing",
                    script_id="script-1",
                    type="scene",
                    name="大堂",
                    description="大堂全景",
                    episode_nos=[1],
                    review_status="not_designed",
                ),
            ]
        )
        raw.commit()

        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _owner())),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/scripts/script-1/asset-library/subjects",
                json={
                    "type": "background",
                    "name": "大堂",
                    "description": "重复场景",
                    "episode_nos": [1],
                },
            )

        assert response.status_code == 409
        assert response.json() == {"detail": "同剧本下已存在同名同类主体"}
        assert raw.query(Subject).count() == 1
        assert raw.query(AuditEvent).count() == 0


@pytest.mark.asyncio
async def test_create_subject_rejects_episode_from_outside_script() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.add_all([_script(), _episode(1, "episode-1")])
        raw.commit()

        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _owner())),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/scripts/script-1/asset-library/subjects",
                json={
                    "type": "prop",
                    "name": "旧手表",
                    "description": "磨损的机械手表",
                    "episode_nos": [1, 3],
                },
            )

        assert response.status_code == 400
        assert response.json() == {"detail": "所属集号不存在: 3"}
        assert raw.query(Subject).count() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {
            "type": "vehicle",
            "name": "车",
            "description": "一辆车",
            "episode_nos": [1],
        },
        {
            "type": "character",
            "name": "   ",
            "description": "主角",
            "episode_nos": [1],
        },
        {
            "type": "sound",
            "name": "雨声",
            "description": "   ",
            "episode_nos": [1],
        },
        {
            "type": "prop",
            "name": "手表",
            "description": "机械手表",
            "episode_nos": [1, 1],
        },
        {
            "type": "prop",
            "name": "手表",
            "description": "机械手表",
            "episode_nos": [0],
        },
        {
            "type": "prop",
            "name": "手表",
            "description": "机械手表",
            "episode_nos": [1.0],
        },
        {
            "type": "prop",
            "name": "手表",
            "description": "机械手表",
            "episode_nos": [True],
        },
    ],
)
async def test_create_subject_validates_payload(payload: dict) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.add_all([_script(), _episode(1, "episode-1")])
        raw.commit()

        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _owner())),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/scripts/script-1/asset-library/subjects",
                json=payload,
            )

        assert response.status_code == 422
        assert raw.query(Subject).count() == 0


@pytest.mark.asyncio
async def test_create_subject_requires_edit_permission(monkeypatch) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.add_all([_script(), _episode(1, "episode-1")])
        raw.commit()
        checked_actions: list[Action] = []

        async def _deny(_db, _user, _script, action: Action) -> None:
            checked_actions.append(action)
            raise ForbiddenError("无编辑权限")

        monkeypatch.setattr(asset_library, "check_script_access", _deny)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _owner())),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/scripts/script-1/asset-library/subjects",
                json={
                    "type": "character",
                    "name": "林舟",
                    "description": "青年男性主角",
                    "episode_nos": [1],
                },
            )

        assert response.status_code == 403
        assert checked_actions == [Action.EDIT]
        assert raw.query(Subject).count() == 0
        assert raw.query(AuditEvent).count() == 0


def test_openapi_exposes_manual_subject_operation() -> None:
    from app.main import app

    operation = app.openapi()["paths"][
        "/v3api/scripts/{script_id}/asset-library/subjects"
    ]
    assert "post" in operation
    assert operation["post"]["responses"]["201"]["description"] == "Successful Response"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "episode_nos",
    ([1, 1], [0], [1.0], [True], []),
)
async def test_update_subject_reuses_strict_episode_number_validation(
    episode_nos: list,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.add_all(
            [
                _script(),
                _episode(1, "episode-1"),
                Subject(
                    id="subject-1",
                    script_id="script-1",
                    type="prop",
                    name="手表",
                    pipeline_name="手表",
                    description="机械手表",
                    episode_nos=[1],
                ),
            ]
        )
        raw.commit()

        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _owner())),
            base_url="http://test",
        ) as client:
            response = await client.patch(
                "/scripts/script-1/asset-library/subjects/subject-1",
                json={"episode_nos": episode_nos},
            )

        assert response.status_code == 422
        assert raw.get(Subject, "subject-1").episode_nos == [1]


@pytest.mark.asyncio
async def test_update_subject_rejects_unknown_episode_and_name_type_conflict() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.add_all(
            [
                _script(),
                _episode(1, "episode-1"),
                Subject(
                    id="subject-1",
                    script_id="script-1",
                    type="prop",
                    name="手表",
                    pipeline_name="手表",
                    description="机械手表",
                    episode_nos=[1],
                ),
                Subject(
                    id="subject-2",
                    script_id="script-1",
                    type="prop",
                    name="钥匙",
                    pipeline_name="钥匙",
                    description="黄铜钥匙",
                    episode_nos=[1],
                ),
            ]
        )
        raw.commit()

        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _owner())),
            base_url="http://test",
        ) as client:
            missing_episode = await client.patch(
                "/scripts/script-1/asset-library/subjects/subject-1",
                json={"episode_nos": [2]},
            )
            raw.rollback()
            duplicate = await client.patch(
                "/scripts/script-1/asset-library/subjects/subject-1",
                json={"name": "钥匙"},
            )

        assert missing_episode.status_code == 400
        assert missing_episode.json() == {"detail": "所属集号不存在: 2"}
        assert duplicate.status_code == 409
        assert duplicate.json() == {"detail": "同剧本下已存在同名同类主体"}


@pytest.mark.asyncio
async def test_update_subject_type_rejects_stable_pipeline_identity_conflict() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.add_all(
            [
                _script(),
                _episode(1, "episode-1"),
                Subject(
                    id="subject-character",
                    script_id="script-1",
                    type="character",
                    name="人工角色名",
                    pipeline_name="稳定身份",
                    description="角色",
                    episode_nos=[1],
                ),
                Subject(
                    id="subject-prop",
                    script_id="script-1",
                    type="prop",
                    name="道具名",
                    pipeline_name="稳定身份",
                    description="道具",
                    episode_nos=[1],
                ),
            ]
        )
        raw.commit()

        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _owner())),
            base_url="http://test",
        ) as client:
            response = await client.patch(
                "/scripts/script-1/asset-library/subjects/subject-character",
                json={"type": "prop"},
            )

        assert response.status_code == 409
        assert response.json() == {"detail": "同剧本下已存在同名同类主体"}
        assert raw.get(Subject, "subject-character").type == "character"


@pytest.mark.asyncio
async def test_create_subject_and_audit_share_one_transaction(monkeypatch) -> None:
    order: list[str] = []
    subject = Subject(
        id="subject-1",
        script_id="script-1",
        type="character",
        name="主角",
        pipeline_name="主角",
        description="主角描述",
        episode_nos=[1],
        review_status="not_designed",
    )
    db = SimpleNamespace(commit=AsyncMock(side_effect=lambda: order.append("commit")))

    async def create(*args, **kwargs):
        order.append("create")
        return subject

    async def audit(*args, **kwargs):
        order.append("audit")

    monkeypatch.setattr(asset_library, "_load_script_or_403", AsyncMock())
    monkeypatch.setattr(asset_library.asset_crud, "create_subject", create)
    monkeypatch.setattr(asset_library, "strict_audit_log", audit)
    monkeypatch.setattr(
        asset_library.asset_crud,
        "get_subject",
        AsyncMock(return_value=subject),
    )
    monkeypatch.setattr(
        asset_library.asset_crud,
        "get_episode_id_map",
        AsyncMock(return_value={1: "episode-1"}),
    )
    monkeypatch.setattr(
        asset_library.asset_crud,
        "serialize_subject",
        lambda *_args: SimpleNamespace(subject_id=subject.id),
    )

    result = await asset_library.create_subject(
        "script-1",
        SubjectCreateIn(
            type="character",
            name="主角",
            description="主角描述",
            episode_nos=[1],
        ),
        db=db,
        user=_owner(),
    )

    assert result.subject_id == "subject-1"
    assert order == ["create", "audit", "commit"]
    db.commit.assert_awaited_once()
