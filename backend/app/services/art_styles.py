"""动态画风提取与持久化服务。"""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import BadRequestError, ConflictError, NotFoundError
from app.core.ids import new_id
from app.models.art_style import ArtStyle
from app.models.asset_entry import AssetEntry
from app.providers.storage import StorageProvider, get_storage, validate_key_prefix
from app.providers.vision_style import VisionStyleProvider, get_vision_style_provider

_MAX_STYLE_IMAGE_BYTES = 20 * 1024 * 1024


async def list_art_styles(
    db: AsyncSession,
    *,
    script_id: str,
    include_inactive: bool = False,
) -> tuple[list[ArtStyle], int]:
    conditions = [ArtStyle.script_id == script_id]
    if not include_inactive:
        conditions.append(ArtStyle.is_active.is_(True))
    total = int(
        (await db.execute(select(func.count()).select_from(ArtStyle).where(*conditions))).scalar_one()
    )
    rows = (
        await db.execute(
            select(ArtStyle)
            .where(*conditions)
            .order_by(ArtStyle.updated_at.desc(), ArtStyle.id.asc())
        )
    ).scalars().all()
    return list(rows), total


async def extract_art_style(
    db: AsyncSession,
    *,
    script_id: str,
    name: str,
    asset_ids: list[str],
    actor_user_id: str,
    provider: VisionStyleProvider | None = None,
    storage: StorageProvider | None = None,
) -> ArtStyle:
    assets = list(
        (
            await db.execute(
                select(AssetEntry).where(
                    AssetEntry.id.in_(asset_ids),
                    AssetEntry.script_id == script_id,
                    AssetEntry.kind == "image",
                    AssetEntry.status.in_(["candidate", "approved"]),
                )
            )
        ).scalars().all()
    )
    by_id = {asset.id: asset for asset in assets}
    if set(asset_ids) != set(by_id):
        raise BadRequestError("参考图不存在、不属于当前剧本或不是有效图片")
    existing_name = (
        await db.execute(
            select(ArtStyle.id).where(ArtStyle.script_id == script_id, ArtStyle.name == name)
        )
    ).scalar_one_or_none()
    if existing_name is not None:
        raise ConflictError("同名画风已存在")

    image_inputs: list[tuple[bytes, str]] = []
    resolved_storage = storage or get_storage()
    for asset_id in asset_ids:
        asset = by_id[asset_id]
        if not asset.mime_type.startswith("image/") or asset.size_bytes > _MAX_STYLE_IMAGE_BYTES:
            raise BadRequestError("参考图格式或大小不符合画风提取要求")
        data = await resolved_storage.get(validate_key_prefix(asset.storage_key))
        if not data or len(data) > _MAX_STYLE_IMAGE_BYTES:
            raise BadRequestError("参考图内容无效或过大")
        image_inputs.append((data, asset.mime_type))

    vision = provider or get_vision_style_provider()
    extracted = await vision.extract_style(image_inputs)
    prompt_zh = extracted.get("prompt_zh")
    prompt_en = extracted.get("prompt_en")
    negative_prompt = extracted.get("negative_prompt", "")
    visual_style = extracted.get("visual_style")
    if not isinstance(prompt_zh, str) or not prompt_zh.strip():
        raise BadRequestError("画风提取结果缺少中文提示词")
    if not isinstance(prompt_en, str) or not prompt_en.strip():
        raise BadRequestError("画风提取结果缺少英文提示词")
    if not isinstance(negative_prompt, str):
        raise BadRequestError("画风提取结果的负面提示词无效")
    if visual_style not in {None, "live_action", "anime"}:
        raise BadRequestError("画风提取结果的媒介类型无效")
    first = by_id[asset_ids[0]]
    row = ArtStyle(
        id=new_id(),
        script_id=script_id,
        name=name,
        prompt_zh=prompt_zh.strip(),
        prompt_en=prompt_en.strip(),
        negative_prompt=negative_prompt.strip(),
        visual_style=visual_style,
        source_asset_ids=asset_ids,
        source_hashes=[by_id[asset_id].sha256 or "" for asset_id in asset_ids],
        thumbnail_storage_key=first.storage_key,
        extraction_provider=vision.provider_name,
        extraction_model=vision.model_name,
        extraction_version=1,
        is_active=True,
        created_by_user_id=actor_user_id,
        updated_by_user_id=actor_user_id,
    )
    db.add(row)
    await db.flush()
    return row


async def update_art_style(
    db: AsyncSession,
    *,
    script_id: str,
    art_style_id: str,
    updates: dict,
    actor_user_id: str,
) -> ArtStyle:
    row = (
        await db.execute(
            select(ArtStyle)
            .where(ArtStyle.id == art_style_id, ArtStyle.script_id == script_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        raise NotFoundError("画风不存在")
    if "name" in updates and updates["name"] != row.name:
        duplicate = (
            await db.execute(
                select(ArtStyle.id).where(
                    ArtStyle.script_id == script_id,
                    ArtStyle.name == updates["name"],
                    ArtStyle.id != art_style_id,
                )
            )
        ).scalar_one_or_none()
        if duplicate is not None:
            raise ConflictError("同名画风已存在")
    for key, value in updates.items():
        if hasattr(row, key):
            setattr(row, key, value)
    row.updated_by_user_id = actor_user_id
    await db.flush()
    return row


async def get_art_style_definition(
    db: AsyncSession,
    *,
    script_id: str,
    art_style_id: str,
) -> dict | None:
    row = (
        await db.execute(
            select(ArtStyle).where(
                ArtStyle.id == art_style_id,
                ArtStyle.script_id == script_id,
                ArtStyle.is_active.is_(True),
            )
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    return {
        "id": row.id,
        "name_zh": row.name,
        "target": ["image", "video"],
        "prompt_template": f"{row.prompt_en}, {{prompt}}",
        "negative_prompt": row.negative_prompt,
        "default_prompt": row.prompt_en,
        "best_for": ["项目自定义画风"],
        "visual_style": row.visual_style,
        "source": "art_style",
    }

