"""资产 enrich 设计提示词下线后的边界契约。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.crud import asset as asset_crud
from app.schemas import pipeline_contracts
from app.schemas.asset import SubjectAssetOut
from app.schemas.script import PipelineStageOut
from app.services.pipeline import STAGE_LABELS, enrich


def _subject() -> SimpleNamespace:
    return SimpleNamespace(
        id="subject-1",
        name="主角",
        type="character",
        description="前端自行组织的最终生图提示词来源",
        parent_id=None,
        parent_subject=None,
        is_parent=False,
        review_status="not_designed",
        image_entry_id=None,
        image_entry=None,
        asset_entries=[],
        episode_nos=[1],
        whitelist_status=None,
        created_at=None,
        updated_at=None,
        subject_version=1,
        name_locked=False,
        description_locked=False,
    )


def test_subject_assets_do_not_expose_server_design_prompt() -> None:
    """资产列表不得向前端提供服务端生成或模板回退的提示词。"""
    removed_fields = {
        "design_prompt",
        "design_prompt_source",
        "design_prompt_version",
        "design_prompt_template_version",
    }

    assert removed_fields.isdisjoint(SubjectAssetOut.model_fields)
    assert removed_fields.isdisjoint(asset_crud.serialize_subject(_subject()).model_dump())


@pytest.mark.asyncio
async def test_enrich_stage_keeps_pipeline_barrier_without_prompt_generation(monkeypatch) -> None:
    """enrich 作为兼容叶子阶段完成，但不能访问模型或产出提示词。"""
    set_stage = AsyncMock()
    provider = SimpleNamespace(complete_json=AsyncMock())

    monkeypatch.setattr(enrich, "input_episode_is_current", AsyncMock(return_value=True))
    monkeypatch.setattr(enrich, "set_stage", set_stage)

    result = await enrich.run_enrich(
        SimpleNamespace(),
        "script-1",
        episode_id="episode-1",
        input_version=3,
        input_client_save_seq=2,
        run_id="run-1",
        provider=provider,
    )

    assert provider.complete_json.await_count == 0
    assert result == {
        "count": 0,
        "input_version": 3,
        "input_client_save_seq": 2,
    }
    assert set_stage.await_args.kwargs["detail"] == result
    assert set_stage.await_args.kwargs["total"] == 0
    assert "prompts" not in result
    assert "resolved_prompts" not in result


def test_pipeline_contracts_do_not_export_enrich_prompt_output() -> None:
    """LLM enrich 输出合约下线，避免其他调用方继续依赖它。"""
    assert not hasattr(pipeline_contracts, "EnrichOutput")
    assert not hasattr(pipeline_contracts, "ResolvedPrompts")


def test_enrich_pipeline_status_uses_neutral_compatibility_sync_copy() -> None:
    """已下线的 enrich 阶段不能在状态接口中暗示提示词或线索增强。"""
    done_stage = PipelineStageOut(
        stage="enrich",
        label=STAGE_LABELS["enrich"],
        status="done",
        detail={"count": 0},
    )
    running_stage = PipelineStageOut(
        stage="enrich",
        label=STAGE_LABELS["enrich"],
        status="running",
        detail="0 条线索增强",
    )

    assert done_stage.label == "兼容同步"
    assert done_stage.detail == "已同步"
    assert running_stage.detail == ""


@pytest.mark.asyncio
async def test_episode_context_maps_do_not_load_retired_enrich_snapshot() -> None:
    """资产序列化只需分集号到分集 ID 的映射，不应加载已下线的版本快照。"""
    seen_columns: list[str] = []

    class _Rows:
        def all(self):
            return [(1, "episode-1"), (2, "episode-2")]

    class _Db:
        async def execute(self, statement):
            seen_columns.extend(column.name for column in statement.selected_columns)
            return _Rows()

    episode_map, context_map = await asset_crud.get_episode_context_maps(_Db(), "script-1")

    assert seen_columns == ["episode_no", "episode_id"]
    assert episode_map == {1: "episode-1", 2: "episode-2"}
    assert context_map == {}
