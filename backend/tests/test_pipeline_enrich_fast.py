"""Enrich 快速路径：宽松契约、缓存、有界并发与单项降级。"""

import asyncio
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from app.models.subject import Subject
from app.schemas.pipeline_contracts import EnrichOutput
from app.services.pipeline import enrich


class _Rows:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def scalars(self) -> "_Rows":
        return self

    def all(self) -> list[object]:
        return self.rows

    def scalar_one_or_none(self) -> object | None:
        return self.rows[0] if self.rows else None


class _Session:
    def __init__(
        self,
        subjects: list[Subject],
        *,
        stage_detail: dict | None = None,
        job_outputs: list[dict] | None = None,
    ) -> None:
        self.subjects = subjects
        self.stage_detail = stage_detail
        self.job_outputs = job_outputs or []
        self.commits = 0

    async def execute(self, statement) -> _Rows:
        sql = str(statement)
        if "FROM subjects" in sql:
            return _Rows(self.subjects)
        if "FROM episodes" in sql:
            return _Rows([])
        if "pipeline_stages.detail" in sql:
            return _Rows([self.stage_detail] if self.stage_detail is not None else [])
        if "pipeline_jobs.output_json" in sql:
            return _Rows(self.job_outputs)
        raise AssertionError(f"未覆盖的查询：{sql}")

    async def commit(self) -> None:
        self.commits += 1


class _DelayProvider:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls = 0
        self.active = 0
        self.max_active = 0
        self.fail = fail

    async def complete_json(self, system: str, user: str) -> dict:
        del system, user
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        if self.fail:
            raise ConnectionError("测试供应商不可用")
        return {
            "schema_version": "pipeline.v1",
            "resolved_prompts": {"final": "自由排版的摄影提示词，无需十二段标题。"},
        }


def _subject(index: int, *, enrich_version: int = 1) -> Subject:
    return Subject(
        id=f"subject-{index}",
        script_id="script-1",
        name=f"主体{index}",
        type="character",
        description=f"主体{index}的稳定视觉描述",
        episode_nos=[1],
        subject_version=1,
        enrich_version=enrich_version,
        is_active=True,
        is_child=True,
        is_parent=False,
    )


def test_enrich_contract_only_requires_safe_nonempty_json_shape() -> None:
    payload = {
        "schema_version": "pipeline.v1",
        "resolved_prompts": {
            "final": "这里是模型添加的引导语。\n只有自由格式正文，也可以用于生图。"
        },
    }
    contract = EnrichOutput.model_validate(payload)
    assert contract.resolved_prompts.final.startswith("这里是模型添加的引导语")

    with pytest.raises(ValidationError):
        EnrichOutput.model_validate(
            {
                "schema_version": "pipeline.v1",
                "resolved_prompts": {"final": "  "},
            }
        )


def test_enrich_uses_bounded_concurrency_and_only_calls_each_subject_once(
    monkeypatch,
) -> None:
    subjects = [_subject(index) for index in range(12)]
    provider = _DelayProvider()
    stage = AsyncMock()
    monkeypatch.setattr(enrich, "set_stage", stage)

    result = asyncio.run(
        enrich.run_enrich(
            _Session(subjects),
            "script-1",
            provider=provider,
            max_concurrency=3,
        )
    )

    assert result == 12
    assert provider.calls == 12
    assert provider.max_active == 3
    detail = stage.await_args.kwargs["detail"]
    assert detail["provider_call_count"] == 12
    assert detail["fallback_count"] == 0


def test_enrich_reuses_matching_version_from_pipeline_job_history(monkeypatch) -> None:
    subjects = [_subject(1, enrich_version=4), _subject(2, enrich_version=4)]
    history = {
        "prompts": {
            "subject-1": {
                "enrich_version": 4,
                "resolved_prompts": {"final": "历史摄影提示词"},
            }
        }
    }
    provider = _DelayProvider()
    stage = AsyncMock()
    monkeypatch.setattr(enrich, "set_stage", stage)

    result = asyncio.run(
        enrich.run_enrich(
            _Session(subjects, job_outputs=[history]),
            "script-1",
            provider=provider,
        )
    )

    assert result == 2
    assert provider.calls == 1
    detail = stage.await_args.kwargs["detail"]
    assert detail["reused_count"] == 1
    assert detail["provider_call_count"] == 1
    assert detail["prompts"]["subject-1"]["source"] == "reused"
    assert detail["prompts"]["subject-1"]["enrich_version"] == 4
    assert detail["prompts"]["subject-2"]["enrich_version"] == 5


def test_enrich_single_subject_failures_fall_back_without_batch_retry(monkeypatch) -> None:
    subjects = [_subject(index) for index in range(4)]
    provider = _DelayProvider(fail=True)
    stage = AsyncMock()
    monkeypatch.setattr(enrich, "set_stage", stage)

    result = asyncio.run(
        enrich.run_enrich(
            _Session(subjects),
            "script-1",
            provider=provider,
            max_concurrency=2,
        )
    )

    assert result == 4
    assert provider.calls == 4
    detail = stage.await_args.kwargs["detail"]
    assert detail["fallback_count"] == 4
    assert detail["provider_call_count"] == 4
    assert all(
        item["source"] == "base_fallback"
        and item["resolved_prompts"]["final"].startswith("为以下主体设计")
        for item in detail["prompts"].values()
    )
