"""T5 画布版本、节点与数据库事务约束测试。"""

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.core.errors import ConflictError, ForbiddenError, NotFoundError
from app.crud import materials as materials_crud
from app.models.asset_entry import AssetEntry
from app.models.base import Base
from app.models.canvas_edge import CanvasEdge
from app.models.canvas_node import CanvasNode
from app.models.episode import Episode
from app.models.materials import Materials
from app.models.parent_subject import ParentSubject
from app.models.script import Script
from app.models.subject import Subject
from app.schemas.materials import CanvasDeltaIn, CanvasNodeOut, NodeUpsertIn


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncSessionAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    async def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.session.commit()

    async def rollback(self) -> None:
        self.session.rollback()

    def begin_nested(self):
        return _AsyncNestedTransaction(self.session.begin_nested())

    def add(self, row) -> None:
        self.session.add(row)


class _NoAutoflushAsyncSessionAdapter(_AsyncSessionAdapter):
    async def execute(self, statement):
        with self.session.no_autoflush:
            return self.session.execute(statement)


class _CommitCountingAsyncSessionAdapter(_NoAutoflushAsyncSessionAdapter):
    def __init__(self, session: Session) -> None:
        super().__init__(session)
        self.commit_count = 0

    async def commit(self) -> None:
        self.commit_count += 1
        await super().commit()


class _AsyncNestedTransaction:
    """为同步 SQLite 会话提供与 AsyncSession 相同的保存点接口。"""

    def __init__(self, transaction) -> None:
        self.transaction = transaction

    async def __aenter__(self):
        self.transaction.__enter__()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self.transaction.__exit__(exc_type, exc, traceback)


class _NoopAsyncNestedTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None


def _canvas_tables(engine) -> None:
    Base.metadata.create_all(
        engine,
        tables=[
            Script.__table__,
            Episode.__table__,
            ParentSubject.__table__,
            Subject.__table__,
            AssetEntry.__table__,
            Materials.__table__,
            CanvasNode.__table__,
            CanvasEdge.__table__,
        ],
    )


def test_materials_has_monotonic_version_column():
    assert "version" in Materials.__table__.c
    assert Materials.__table__.c.version.default.arg == 0


def test_task_node_data_round_trips_without_dropping_fields():
    data = {
        "subjectId": "subject-1",
        "assetId": "image-1",
        "prompt": "参考图",
        "negative_prompt": "模糊",
        "seed": 42,
        "width": 1024,
        "height": 1024,
        "status": "pending",
    }
    node = CanvasNodeOut(id="node-1", type="imageUpload", position={"x": 1, "y": 2}, data=data)

    assert node.data == data
    assert CanvasDeltaIn(
        version=0,
        upsert_nodes=[NodeUpsertIn(id="node-1", type="imageUpload", data=data)],
    ).upsert_nodes[0].data == data


def test_node_schema_rejects_unknown_type_and_builds_prompt_from_subject_data():
    with pytest.raises(ValueError):
        NodeUpsertIn(id="node-1", type="untyped", data={"title": "x"})

    node = NodeUpsertIn(
        id="node-1",
        type="imageUpload",
        data={
            "title": "主角设计图",
            "variant": "generated",
            "subjectId": "subject-1",
            "subjectName": "主角",
            "subjectType": "character",
            "subjectDescription": "蓝色外套",
            "model": "banana-2",
            "size": "2k",
            "aspect": "16:9",
            "imageOutputs": [{"status": "succeeded", "assetId": "image-1", "storageKey": "/a.png"}],
        },
    )

    assert "主体名称：主角" in materials_crud.build_prompt_from_node_data(node.data)
    assert node.data["imageOutputs"][0]["assetId"] == "image-1"


@pytest.mark.asyncio
async def test_delta_rejects_stale_material_version_before_mutation(monkeypatch):
    material = SimpleNamespace(id="material-1", version=7, updated_at=None)

    async def _get_materials(_db, _material_id):
        return material

    monkeypatch.setattr(materials_crud, "get_materials", _get_materials)

    class _Db:
        async def execute(self, _statement):
            return SimpleNamespace(rowcount=0)

        def begin_nested(self):
            return _NoopAsyncNestedTransaction()

    with pytest.raises(ConflictError):
        await materials_crud.apply_delta(
            _Db(),
            material.id,
            CanvasDeltaIn(version=6),
        )


def test_invalid_edge_rolls_back_already_executed_canvas_cas():
    """真实事务中，边校验失败后调用者仍提交时不能落下版本更新。"""
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _canvas_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                CanvasNode(id="node-1", material_id="material-1", type="imageUpload", data={"title": "图"}),
            ])
            raw.commit()
            db = _AsyncSessionAdapter(raw)
            with pytest.raises(NotFoundError):
                await materials_crud.apply_delta(
                    db,
                    "material-1",
                    CanvasDeltaIn(
                        version=0,
                        upsert_edges=[{"source": "node-1", "target": "missing"}],
                    ),
                )
            raw.commit()

        with Session(engine) as verify:
            assert verify.get(Materials, "material-1").version == 0

    asyncio.run(scenario())


def test_complete_image_node_data_round_trips_through_real_canvas_transaction():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _canvas_tables(engine)
        data = {
            "title": "主角设计图",
            "variant": "generated",
            "imageUrl": None,
            "model": "banana-2",
            "size": "2k",
            "aspect": "16:9",
            "isGenerating": False,
            "generationJobId": "job-1",
            "subjectId": "subject-1",
            "subjectName": "主角",
            "subjectType": "character",
            "subjectDescription": "蓝色外套",
            "subjectEpisodeIds": ["episode-1"],
            "subjectEpisodeNos": [1],
            "subjectDesignStatus": "pending",
            "promptVariationIndex": 0,
            "imagesExpanded": True,
            "generatingStartedAt": 1784015617046,
            "assetId": "asset-1",
            "imageOutputs": [
                {"status": "succeeded", "assetId": "asset-1", "storageKey": "/image.png"}
            ],
            "primaryImageIndex": 0,
        }
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                Subject(
                    id="subject-1",
                    script_id="script-1",
                    type="character",
                    name="主角",
                    description="蓝色外套",
                ),
            ])
            raw.commit()
            saved = await materials_crud.apply_delta(
                _AsyncSessionAdapter(raw),
                "material-1",
                CanvasDeltaIn(
                    version=0,
                    upsert_nodes=[
                        NodeUpsertIn(
                            id="node-1", type="imageUpload", position={"x": 1, "y": 2}, data=data
                        )
                    ],
                ),
            )
            assert saved.nodes[0].data == {**data, "prompt": materials_crud.build_prompt_from_node_data(data)}

    asyncio.run(scenario())


def test_image_upload_prompt_is_built_from_persisted_subject_not_client_fields():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _canvas_tables(engine)
        data = {
            "title": "主角设计图",
            "variant": "generated",
            "subjectId": "subject-1",
            "subjectName": "伪造名称",
            "subjectType": "sound",
            "subjectDescription": "伪造描述",
            "prompt": "客户端伪造 prompt",
            "model": "banana-2",
            "size": "2k",
            "aspect": "16:9",
            "imageOutputs": [{"status": "pending"}],
        }
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                Subject(
                    id="subject-1",
                    script_id="script-1",
                    type="character",
                    name="真实主角",
                    description="真实描述",
                ),
            ])
            raw.commit()

            saved = await materials_crud.apply_delta(
                _AsyncSessionAdapter(raw),
                "material-1",
                CanvasDeltaIn(
                    version=0,
                    upsert_nodes=[
                        NodeUpsertIn(
                            id="node-1",
                            type="imageUpload",
                            position={"x": 1, "y": 2},
                            data=data,
                        )
                    ],
                ),
            )

            node_data = saved.nodes[0].data
            assert node_data["subjectName"] == "真实主角"
            assert node_data["subjectType"] == "character"
            assert node_data["subjectDescription"] == "真实描述"
            assert "主体名称：真实主角" in node_data["prompt"]
            assert "客户端伪造" not in node_data["prompt"]

    asyncio.run(scenario())


def test_image_upload_prompt_helper_failure_rolls_back_canvas_cas_and_node(monkeypatch):
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _canvas_tables(engine)
        data = {
            "title": "主角设计图",
            "variant": "generated",
            "subjectId": "subject-1",
            "subjectName": "主角",
            "subjectType": "character",
            "subjectDescription": "蓝色外套",
            "model": "banana-2",
            "size": "2k",
            "aspect": "16:9",
            "imageOutputs": [{"status": "pending"}],
        }

        async def _boom(_db, _material, _data):
            raise RuntimeError("helper failed")

        monkeypatch.setattr(materials_crud, "_trusted_image_upload_data", _boom)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
            ])
            raw.commit()

            with pytest.raises(RuntimeError, match="helper failed"):
                await materials_crud.apply_delta(
                    _AsyncSessionAdapter(raw),
                    "material-1",
                    CanvasDeltaIn(
                        version=0,
                        upsert_nodes=[
                            NodeUpsertIn(
                                id="node-1",
                                type="imageUpload",
                                position={"x": 1, "y": 2},
                                data=data,
                            )
                        ],
                    ),
                )
            raw.commit()

        with Session(engine) as verify:
            assert verify.get(Materials, "material-1").version == 0
            assert verify.get(CanvasNode, "node-1") is None

    asyncio.run(scenario())


def test_edge_endpoints_must_belong_to_the_same_material():
    nodes = {
        "node-a": "material-1",
        "node-b": "material-2",
    }

    with pytest.raises(NotFoundError):
        materials_crud.validate_edge_endpoints(nodes, "node-a", "missing", "material-1")
    with pytest.raises(ForbiddenError):
        materials_crud.validate_edge_endpoints(nodes, "node-a", "node-b", "material-1")


def test_canvas_edge_declares_material_scoped_endpoint_foreign_keys():
    constraints = list(CanvasEdge.__table__.foreign_key_constraints)
    endpoint_constraints = [
        constraint
        for constraint in constraints
        if {column.name for column in constraint.columns} in ({"material_id", "source"}, {"material_id", "target"})
    ]
    assert len(endpoint_constraints) == 2


def test_database_rejects_cross_material_edge_endpoints():
    engine = create_engine("sqlite+pysqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(connection, _record) -> None:
        connection.execute("PRAGMA foreign_keys=ON")

    _canvas_tables(engine)
    with Session(engine) as session:
        session.add_all([
            Script(
                id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                created_by_user_id="user-1",
            ),
            Script(
                id="script-2", title="剧本二", scope="personal", owner_user_id="user-1",
                created_by_user_id="user-1",
            ),
            Materials(id="material-1", name="画布一", script_id="script-1"),
            Materials(id="material-2", name="画布二", script_id="script-2"),
            CanvasNode(id="node-a", material_id="material-1", type="imageUpload"),
            CanvasNode(id="node-b", material_id="material-2", type="imageUpload"),
        ])
        session.commit()
        session.add(
            CanvasEdge(
                id="edge-1", material_id="material-1", source="node-a", target="node-b"
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_delta_success_keeps_outer_transaction_uncommitted():
    """画布保存只能由路由拥有最终提交，不能偷提交调用方的无关写入。"""

    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _canvas_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
            ])
            raw.commit()

            # 该无关写入与 delta 同属调用方事务，外层 rollback 必须同时撤销两者。
            raw.add(
                Script(
                    id="script-unrelated", title="无关", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                )
            )
            await materials_crud.apply_delta(
                _AsyncSessionAdapter(raw),
                "material-1",
                CanvasDeltaIn(
                    version=0,
                    upsert_nodes=[{"id": "node-1", "type": "text", "data": {"content": "草稿"}}],
                ),
            )
            raw.rollback()

        with Session(engine) as verify:
            assert verify.scalar(
                select(Script.id).where(Script.id == "script-unrelated")
            ) is None
            assert verify.get(CanvasNode, "node-1") is None

    asyncio.run(scenario())


def test_delta_failure_keeps_prior_outer_transaction_writes():
    """delta 校验失败只能回滚其保存点，不能抹掉调用方既有写入。"""

    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _canvas_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                CanvasNode(id="node-1", material_id="material-1", type="text"),
            ])
            raw.commit()

            raw.add(
                Script(
                    id="script-unrelated", title="无关", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                )
            )
            with pytest.raises(NotFoundError):
                await materials_crud.apply_delta(
                    _AsyncSessionAdapter(raw),
                    "material-1",
                    CanvasDeltaIn(
                        version=0,
                        upsert_edges=[{"source": "node-1", "target": "missing"}],
                    ),
                )
            raw.commit()

        with Session(engine) as verify:
            assert verify.scalar(
                select(Script.id).where(Script.id == "script-unrelated")
            ) == "script-unrelated"
            assert verify.get(Materials, "material-1").version == 0

    asyncio.run(scenario())


def test_delta_deletes_referencing_edges_before_nodes_with_foreign_keys_enabled():
    """PostgreSQL 的即时 FK 要求先删边，并允许客户端同时上报节点与边删除。"""

    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        _canvas_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                CanvasNode(id="node-a", material_id="material-1", type="text"),
                CanvasNode(id="node-b", material_id="material-1", type="text"),
            ])
            raw.commit()
            raw.add(CanvasEdge(id="edge-a-b", material_id="material-1", source="node-a", target="node-b"))
            raw.commit()

            saved = await materials_crud.apply_delta(
                _AsyncSessionAdapter(raw),
                "material-1",
                CanvasDeltaIn(
                    version=0,
                    delete_nodes=["node-a"],
                    delete_edges=["edge-a-b"],
                ),
            )
            raw.commit()

        assert saved.version == 1
        assert [node.id for node in saved.nodes] == ["node-b"]
        assert saved.edges == []

    asyncio.run(scenario())


def test_delta_response_includes_new_nodes_and_edges_with_autoflush_disabled():
    """生产 AsyncSession 关闭 autoflush，保存点内返回前必须显式 flush。"""

    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        _canvas_tables(engine)
        with Session(engine, expire_on_commit=False, autoflush=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
            ])
            raw.commit()

            saved = await materials_crud.apply_delta(
                _NoAutoflushAsyncSessionAdapter(raw),
                "material-1",
                CanvasDeltaIn(
                    version=0,
                    upsert_nodes=[
                        {"id": "node-a", "type": "text", "data": {"content": "A"}},
                        {"id": "node-b", "type": "text", "data": {"content": "B"}},
                    ],
                    upsert_edges=[
                        {"id": "edge-a-b", "source": "node-a", "target": "node-b"}
                    ],
                ),
            )

            assert {node.id for node in saved.nodes} == {"node-a", "node-b"}
            assert [edge.id for edge in saved.edges] == ["edge-a-b"]

    asyncio.run(scenario())


def test_canvas_delta_http_commits_successful_write(monkeypatch, tmp_path):
    """API 成功返回后必须提交，否则请求结束后刷新画布会丢失修改。"""

    async def scenario() -> None:
        from app.api.v1 import materials as materials_api

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'canvas.db'}")
        _canvas_tables(engine)
        with Session(engine, expire_on_commit=False, autoflush=False) as seed:
            seed.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                CanvasNode(
                    id="node-1",
                    material_id="material-1",
                    type="text",
                    position={"x": 0, "y": 0},
                    data={"content": "草稿"},
                ),
            ])
            seed.commit()

        app = FastAPI()
        app.include_router(materials_api.router)
        adapters: list[_CommitCountingAsyncSessionAdapter] = []

        async def _db_override():
            with Session(engine, expire_on_commit=False, autoflush=False) as raw:
                adapter = _CommitCountingAsyncSessionAdapter(raw)
                adapters.append(adapter)
                yield adapter

        app.dependency_overrides[materials_api.get_db] = _db_override
        app.dependency_overrides[materials_api.get_current_user] = lambda: SimpleNamespace()
        monkeypatch.setattr(materials_api, "check_script_access", _allow)

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/materials/material-1/canvas/delta",
                json={
                    "version": 0,
                    "move_nodes": [
                        {"id": "node-1", "position": {"x": 9, "y": 7}}
                    ],
                },
            )

        assert response.status_code == 200
        assert adapters[0].commit_count == 1
        with Session(engine) as verify:
            assert verify.get(CanvasNode, "node-1").position == {"x": 9, "y": 7}

    asyncio.run(scenario())


def test_canvas_delta_http_maps_missing_cross_scope_and_stale_version(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import materials as materials_api

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _canvas_tables(engine)
        with Session(engine, expire_on_commit=False, autoflush=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Script(
                    id="script-2", title="剧本二", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="画布一", script_id="script-1"),
                Materials(id="material-2", name="画布二", script_id="script-2"),
                CanvasNode(id="node-1", material_id="material-1", type="text", data={"content": "A"}),
                CanvasNode(id="other-node", material_id="material-2", type="text", data={"content": "B"}),
            ])
            raw.commit()
            app = FastAPI()
            app.include_router(materials_api.router)

            async def _db_override():
                yield _NoAutoflushAsyncSessionAdapter(raw)

            app.dependency_overrides[materials_api.get_db] = _db_override
            app.dependency_overrides[materials_api.get_current_user] = lambda: SimpleNamespace()
            monkeypatch.setattr(materials_api, "check_script_access", _allow)

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                missing = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={"version": 0, "move_nodes": [{"id": "missing", "position": {"x": 1}}]},
                )
                cross = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={"version": 0, "move_nodes": [{"id": "other-node", "position": {"x": 1}}]},
                )
                stale = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={"version": 99, "move_nodes": [{"id": "node-1", "position": {"x": 1}}]},
                )

            assert missing.status_code == 404
            assert cross.status_code == 403
            assert stale.status_code == 409

    asyncio.run(scenario())


def test_canvas_delta_http_maps_upsert_delete_and_edge_boundaries(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import materials as materials_api

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _canvas_tables(engine)
        with Session(engine, expire_on_commit=False, autoflush=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Script(
                    id="script-2", title="剧本二", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="画布一", script_id="script-1"),
                Materials(id="material-2", name="画布二", script_id="script-2"),
                CanvasNode(id="node-1", material_id="material-1", type="text", data={"content": "A"}),
                CanvasNode(id="node-2", material_id="material-1", type="text", data={"content": "B"}),
                CanvasNode(id="other-node", material_id="material-2", type="text", data={"content": "C"}),
                CanvasEdge(id="edge-1", material_id="material-1", source="node-1", target="node-2"),
                CanvasEdge(id="other-edge", material_id="material-2", source="other-node", target="other-node"),
            ])
            raw.commit()
            app = FastAPI()
            app.include_router(materials_api.router)

            async def _db_override():
                yield _NoAutoflushAsyncSessionAdapter(raw)

            app.dependency_overrides[materials_api.get_db] = _db_override
            app.dependency_overrides[materials_api.get_current_user] = lambda: SimpleNamespace()
            monkeypatch.setattr(materials_api, "check_script_access", _allow)

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                upsert_cross = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={
                        "version": 0,
                        "upsert_nodes": [
                            {"id": "other-node", "type": "text", "data": {"content": "x"}}
                        ],
                    },
                )
                delete_missing = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={"version": 0, "delete_nodes": ["missing"]},
                )
                edge_missing = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={
                        "version": 0,
                        "upsert_edges": [{"source": "node-1", "target": "missing"}],
                    },
                )
                edge_cross = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={
                        "version": 0,
                        "upsert_edges": [{"source": "node-1", "target": "other-node"}],
                    },
                )
                delete_edge_cross = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={"version": 0, "delete_edges": ["other-edge"]},
                )

            assert upsert_cross.status_code == 403
            assert delete_missing.status_code == 404
            assert edge_missing.status_code == 404
            assert edge_cross.status_code == 403
            assert delete_edge_cross.status_code == 403

    asyncio.run(scenario())


def test_canvas_delta_http_rejects_missing_null_and_negative_version_without_mutation(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import materials as materials_api

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _canvas_tables(engine)
        with Session(engine, expire_on_commit=False, autoflush=False) as raw:
            raw.add_all([
                Script(
                    id="script-1", title="剧本", scope="personal", owner_user_id="user-1",
                    created_by_user_id="user-1",
                ),
                Materials(id="material-1", name="画布一", script_id="script-1"),
            ])
            raw.commit()
            app = FastAPI()
            app.include_router(materials_api.router)

            async def _db_override():
                yield _NoAutoflushAsyncSessionAdapter(raw)

            app.dependency_overrides[materials_api.get_db] = _db_override
            app.dependency_overrides[materials_api.get_current_user] = lambda: SimpleNamespace()
            monkeypatch.setattr(materials_api, "check_script_access", _allow)

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                missing = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={
                        "upsert_nodes": [
                            {"id": "node-a", "type": "text", "data": {"content": "A"}}
                        ],
                    },
                )
                null = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={
                        "version": None,
                        "upsert_nodes": [
                            {"id": "node-b", "type": "text", "data": {"content": "B"}}
                        ],
                    },
                )
                negative = await client.post(
                    "/materials/material-1/canvas/delta",
                    json={
                        "version": -1,
                        "upsert_nodes": [
                            {"id": "node-c", "type": "text", "data": {"content": "C"}}
                        ],
                    },
                )

            assert [missing.status_code, null.status_code, negative.status_code] == [422, 422, 422]
            assert raw.get(Materials, "material-1").version == 0
            assert raw.scalars(select(CanvasNode)).all() == []

    asyncio.run(scenario())


def test_node_data_preserves_nested_explicit_null_fields():
    data = {
        "prompt": "主体参考图",
        "imageOutputs": [
            {"status": "succeeded", "assetId": None, "storageKey": None}
        ]
    }

    node = NodeUpsertIn(id="node-1", type="imageUpload", data=data)

    assert node.data == data


@pytest.mark.parametrize(
    ("node_type", "data"),
    [
        ("text", {}),
        ("storyboard", {"shotGroupId": "group-1"}),
        ("generated", {"prompt": "主体参考图"}),
        ("uploaded", {"title": "上传素材"}),
        ("imageUpload", {"title": "主体设计图"}),
    ],
)
def test_node_data_requires_type_specific_contract_fields(node_type: str, data: dict):
    with pytest.raises(ValueError):
        NodeUpsertIn(id="node-1", type=node_type, data=data)


def test_node_data_keeps_unknown_extensions_under_explicit_round_trip_policy():
    node = NodeUpsertIn(
        id="node-1",
        type="text",
        data={"content": "字幕", "frontendExtension": {"color": "red"}},
    )

    assert node.data == {"content": "字幕", "frontendExtension": {"color": "red"}}


@pytest.mark.parametrize("node_type", ["imageUpload", "text", "generated", "uploaded", "storyboard"])
def test_upsert_node_requires_data_for_all_persisted_node_types(node_type: str):
    with pytest.raises(ValueError):
        NodeUpsertIn(id="node-1", type=node_type, data=None)
