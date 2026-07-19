"""画布模块数据访问层 —— Materials / CanvasNode / CanvasEdge / ImageAsset。

apply_delta 增量 upsert/delete nodes 与 edges，支持节点移动/增删/参数变更。
"""

from __future__ import annotations

from typing import Any, cast

from sqlalchemy import delete, func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, ForbiddenError, NotFoundError
from app.core.ids import new_id
from app.models.asset_entry import AssetEntry
from app.models.canvas_edge import CanvasEdge
from app.models.canvas_node import CanvasNode
from app.models.materials import Materials
from app.models.subject import Subject
from app.providers.storage import validate_key_prefix
from app.schemas.materials import (
    CanvasDeltaIn,
    CanvasEdgeOut,
    CanvasNodeOut,
    CanvasNodeType,
    CanvasStateOut,
    ImageAssetOut,
    MaterialsOut,
    build_prompt_from_node_data,
    build_subject_prompt,
)

__all__ = ["build_prompt_from_node_data"]


async def _load_node_for_delta(
    db: AsyncSession, node_id: str, material_id: str
) -> CanvasNode:
    node = await db.get(CanvasNode, node_id)
    if node is None:
        raise NotFoundError("画布节点不存在")
    if node.material_id != material_id:
        raise ForbiddenError("画布节点不属于当前画布")
    return node


async def _load_edge_for_delta(
    db: AsyncSession, edge_id: str, material_id: str
) -> CanvasEdge:
    edge = await db.get(CanvasEdge, edge_id)
    if edge is None:
        raise NotFoundError("画布边不存在")
    if edge.material_id != material_id:
        raise ForbiddenError("画布边不属于当前画布")
    return edge


async def get_materials(db: AsyncSession, material_id: str) -> Materials:
    """获取画布项目，不存在抛 404。"""
    m = await db.get(Materials, material_id)
    if m is None:
        raise NotFoundError("画布项目不存在")
    return m


async def to_materials_out(m: Materials) -> MaterialsOut:
    return MaterialsOut(
        id=m.id,
        name=m.name,
        script_id=m.script_id,
        version=m.version,
        created_at=m.created_at.isoformat() if m.created_at else None,
        updated_at=m.updated_at.isoformat() if m.updated_at else None,
    )


async def get_canvas(db: AsyncSession, material_id: str) -> CanvasStateOut:
    """返回画布完整状态 {material_id, nodes[], edges[]}。"""
    material = await get_materials(db, material_id)

    nodes = (
        await db.execute(
            select(CanvasNode)
            .where(CanvasNode.material_id == material.id)
            .order_by(CanvasNode.created_at)
        )
    ).scalars().all()

    edges = (
        await db.execute(
            select(CanvasEdge)
            .where(CanvasEdge.material_id == material.id)
            .order_by(CanvasEdge.created_at)
        )
    ).scalars().all()

    return CanvasStateOut(
        material_id=material.id,
        nodes=[
            CanvasNodeOut(
                id=n.id,
                type=cast(CanvasNodeType, n.type),
                position=n.position,
                data=n.data,
            )
            for n in nodes
        ],
        edges=[
            CanvasEdgeOut(id=e.id, source=e.source, target=e.target, data=e.data)
            for e in edges
        ],
        version=material.version,
    )


def validate_edge_endpoints(
    node_materials: dict[str, str | None], source: str, target: str, material_id: str
) -> None:
    """拒绝孤儿边与跨画布边。"""
    for endpoint in (source, target):
        endpoint_material_id = node_materials.get(endpoint)
        if endpoint_material_id is None:
            raise NotFoundError("边的 source/target 节点不存在")
        if endpoint_material_id != material_id:
            raise ForbiddenError("边的 source/target 必须属于当前画布")


async def _trusted_image_upload_data(
    db: AsyncSession, material: Materials, node_data: dict
) -> dict:
    subject_id = node_data.get("subjectId")
    if not isinstance(subject_id, str) or not subject_id.strip():
        raise NotFoundError("主体不存在")
    subject = await db.get(Subject, subject_id)
    if subject is None:
        raise NotFoundError("主体不存在")
    if subject.script_id != material.script_id:
        raise ForbiddenError("主体不属于当前剧本")
    description = subject.description or ""
    return {
        **node_data,
        "subjectId": subject.id,
        "subjectName": subject.name,
        "subjectType": subject.type,
        "subjectDescription": description,
        "prompt": build_subject_prompt(subject.name, subject.type, description),
    }


async def apply_delta(db: AsyncSession, material_id: str, delta: CanvasDeltaIn) -> CanvasStateOut:
    """增量 upsert/delete nodes 与 edges。

    - move_nodes: 仅更新 position
    - upsert_nodes: 存在则更新 type/position/data，否则新增
    - delete_nodes: 删除节点 (并清理引用该节点的边)
    - upsert_edges: 存在则更新 source/target/data，否则新增
    - delete_edges: 按 id 删除
    """
    material = await get_materials(db, material_id)
    # delta 是调用方事务的一部分：保存点确保失败只撤销本次变更，最终 commit
    # 必须由 API/service 的外层事务拥有。
    async with db.begin_nested():
        # 材料行版本 CAS 是并发正确性的唯一依据，不能用秒级时间戳代替。
        cas = cast(
            CursorResult[Any],
            await db.execute(
                update(Materials)
                .where(Materials.id == material.id, Materials.version == delta.version)
                .values(version=Materials.version + 1)
            ),
        )
        if cas.rowcount != 1:
            raise ConflictError("画布已被他人修改，请刷新后重试")
        material.version = delta.version + 1

        node_materials: dict[str, str | None] = {}
        for mv in delta.move_nodes:
            node = await db.get(CanvasNode, mv.id)
            if node is None:
                raise NotFoundError("画布节点不存在")
            if node.material_id != material.id:
                raise ForbiddenError("画布节点不属于当前画布")
            node.position = mv.position
            node_materials[node.id] = material.id

        for up in delta.upsert_nodes:
            node = await db.get(CanvasNode, up.id)
            if node is not None and node.material_id != material.id:
                raise ForbiddenError("节点不属于当前画布")
            node_data = up.data
            if node_data is not None and up.type == "imageUpload":
                node_data = await _trusted_image_upload_data(db, material, node_data)
            if node is not None:
                node.type = up.type
                if up.position is not None:
                    node.position = up.position
                if node_data is not None:
                    node.data = node_data
                node_materials[node.id] = material.id
            else:
                new_node = CanvasNode(
                    id=up.id,
                    material_id=material.id,
                    type=up.type,
                    position=up.position,
                    data=node_data,
                )
                db.add(new_node)
                node_materials[new_node.id] = material.id

        # PostgreSQL 的即时复合外键要求新节点先落库，同一 delta 才能安全创建连线。
        if delta.upsert_nodes:
            await db.flush()

        # 前端删除节点时通常会同时上报关联边；须在节点清理边之前完成存在性校验。
        if delta.delete_edges:
            for edge_id in delta.delete_edges:
                await _load_edge_for_delta(db, edge_id, material.id)

        if delta.delete_nodes:
            for node_id in delta.delete_nodes:
                await _load_node_for_delta(db, node_id, material.id)
            await db.execute(
                delete(CanvasEdge).where(
                    CanvasEdge.material_id == material.id,
                    (CanvasEdge.source.in_(delta.delete_nodes))
                    | (CanvasEdge.target.in_(delta.delete_nodes)),
                )
            )
            for node_id in delta.delete_nodes:
                node_materials.pop(node_id, None)
            await db.execute(
                delete(CanvasNode).where(
                    CanvasNode.material_id == material.id,
                    CanvasNode.id.in_(delta.delete_nodes),
                )
            )

        for ue in delta.upsert_edges:
            edge = await db.get(CanvasEdge, ue.id) if ue.id else None
            if edge is not None and edge.material_id != material.id:
                raise ForbiddenError("边不属于当前画布")
            for endpoint in (ue.source, ue.target):
                if endpoint not in node_materials:
                    endpoint_node = await db.get(CanvasNode, endpoint)
                    node_materials[endpoint] = (
                        endpoint_node.material_id if endpoint_node is not None else None
                    )
            validate_edge_endpoints(node_materials, ue.source, ue.target, material.id)
            if edge is not None:
                edge.source = ue.source
                edge.target = ue.target
                if ue.data is not None:
                    edge.data = ue.data
            else:
                db.add(
                    CanvasEdge(
                        id=ue.id or new_id(),
                        material_id=material.id,
                        source=ue.source,
                        target=ue.target,
                        data=ue.data,
                    )
                )

        if delta.delete_edges:
            await db.execute(
                delete(CanvasEdge).where(
                    CanvasEdge.material_id == material.id,
                    CanvasEdge.id.in_(delta.delete_edges),
                )
            )

        material.updated_at = func.now()
        await db.flush()
        return await get_canvas(db, material_id)


async def get_image_asset(db: AsyncSession, material_id: str, image_id: str) -> ImageAssetOut:
    """获取图片资产元数据。

    image_id 对应 AssetEntry.id (kind=image)，需校验归属 material 的 script_id。
    """
    material = await get_materials(db, material_id)
    entry = (
        await db.execute(
            select(AssetEntry).where(
                AssetEntry.id == image_id,
                AssetEntry.kind == "image",
            )
        )
    ).scalar_one_or_none()
    if entry is None:
        raise NotFoundError("图片资产不存在")
    if entry.script_id != material.script_id:
        raise ForbiddenError("图片资产不属于当前剧本")
    return ImageAssetOut(
        image_id=entry.id,
        storage_key=f"/{validate_key_prefix(entry.storage_key)}",
        mime_type=entry.mime_type,
        size_bytes=entry.size_bytes,
        source=entry.source,
    )
