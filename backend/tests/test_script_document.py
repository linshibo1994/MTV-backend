"""剧本文档解析与上传入口测试。"""

from __future__ import annotations

import os
import zipfile
from datetime import UTC, datetime
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.api.v1 import analysis_jobs, scripts
from app.core.errors import ConflictError, NotFoundError
from app.schemas.script import ScriptUploadOut
from app.services.script_document import (
    StagedScriptDocument,
    parse_script_document,
    stage_script_document,
)


def _docx_bytes(paragraphs: list[str]) -> bytes:
    body = "".join(f"<w:p><w:r><w:t>{value}</w:t></w:r></w:p>" for value in paragraphs)
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{body}</w:body></w:document>"
    )
    output = BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", xml)
    return output.getvalue()


async def test_stage_txt_document_decodes_utf8_bom_and_cleans_temp_file() -> None:
    upload = UploadFile(BytesIO("\ufeff第一幕\n第二幕".encode()), filename="剧本.txt")
    document = await stage_script_document(upload, max_bytes=1024)
    try:
        assert document.script_text == "第一幕\n第二幕"
        assert document.suffix == ".txt"
        assert os.path.exists(document.temp_path)
    finally:
        os.unlink(document.temp_path)


async def test_stage_docx_document_extracts_paragraphs() -> None:
    upload = UploadFile(BytesIO(_docx_bytes(["第一幕", "第二幕"])), filename="剧本.docx")
    document = await stage_script_document(upload, max_bytes=1024 * 1024)
    try:
        assert document.script_text == "第一幕\n第二幕"
    finally:
        os.unlink(document.temp_path)


def test_parse_docx_rejects_zip_bomb(tmp_path) -> None:
    path = tmp_path / "bomb.docx"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", "0" * (1024 * 1024))
    with pytest.raises(HTTPException) as exc_info:
        parse_script_document(str(path), ".docx")
    assert exc_info.value.status_code == 400


def test_parse_txt_rejects_content_that_exceeds_script_limit(tmp_path) -> None:
    path = tmp_path / "too-long.txt"
    path.write_bytes(b"a" * (8 * 1024 * 1024 + 1))
    with pytest.raises(HTTPException) as exc_info:
        parse_script_document(str(path), ".txt")
    assert exc_info.value.status_code == 413


async def test_stage_rejects_unsupported_extension() -> None:
    upload = UploadFile(BytesIO(b"legacy"), filename="剧本.doc")
    with pytest.raises(HTTPException) as exc_info:
        await stage_script_document(upload, max_bytes=1024)
    assert exc_info.value.status_code == 415


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _Rows:
    def __init__(self, values):
        self.values = values

    def scalars(self):
        return self

    def all(self):
        return self.values


async def test_upload_endpoint_creates_source_episode_stores_file_and_triggers_pipeline(monkeypatch) -> None:
    from app.providers import storage as storage_module

    monkeypatch.setattr(
        storage_module,
        "settings",
        SimpleNamespace(
            storage_driver="oss",
            aliyun_oss_public_url="https://cdn-ai-public.commind.com",
        ),
    )
    document = StagedScriptDocument(
        original_filename="完整剧本.txt",
        suffix=".txt",
        content_type="text/plain",
        size_bytes=12,
        sha256="a" * 64,
        script_text="第一幕：相遇",
        temp_path="/tmp/already-cleaned-script.txt",
    )
    storage = SimpleNamespace(put_file=AsyncMock(), delete=AsyncMock())
    now = datetime.now(UTC)
    script = SimpleNamespace(id="script-1", owner_user_id="user-1", updated_at=now)
    db = SimpleNamespace(
        execute=AsyncMock(return_value=_Result(None)),
        add=lambda _: None,
        flush=AsyncMock(),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    monkeypatch.setattr(scripts.script_crud, "get_script", AsyncMock(return_value=script))
    monkeypatch.setattr(scripts.script_crud, "get_script_for_update", AsyncMock(return_value=script))
    monkeypatch.setattr(scripts, "check_script_access", AsyncMock())
    monkeypatch.setattr(scripts, "stage_script_document", AsyncMock(return_value=document))
    monkeypatch.setattr(scripts, "cleanup_staged_script_document", lambda _: None)
    monkeypatch.setattr(scripts, "get_storage", lambda: storage)
    monkeypatch.setattr(scripts, "audit_log", AsyncMock())
    trigger = AsyncMock(return_value={"run_id": "run-1", "dispatch_id": "dispatch-1"})
    monkeypatch.setattr(scripts, "trigger_pipeline", trigger)

    result = await scripts.upload_and_split_script(
        "script-1",
        UploadFile(BytesIO(b"ignored"), filename="完整剧本.txt"),
        True,
        provider=None,
        model=None,
        db=db,
        user=SimpleNamespace(user_id="user-1"),
    )

    assert isinstance(result, ScriptUploadOut)
    assert result.analysis_job_id == "run-1"
    assert result.status == "queued"
    assert result.script_file_storage_key.startswith("MTV/upload_scripts/script-1/")
    assert result.script_file_public_url == (
        f"https://cdn-ai-public.commind.com/{result.script_file_storage_key}"
    )
    storage.put_file.assert_awaited_once()
    trigger.assert_awaited_once_with(
        db,
        "script-1",
        result.source_episode_id,
        llm_provider=None,
        llm_model=None,
    )
    assert db.rollback.await_count == 0


async def test_upload_endpoint_rejects_existing_episodes_and_does_not_store(monkeypatch) -> None:
    document = StagedScriptDocument(
        original_filename="完整剧本.txt",
        suffix=".txt",
        content_type="text/plain",
        size_bytes=12,
        sha256="a" * 64,
        script_text="第一幕：相遇",
        temp_path="/tmp/already-cleaned-script.txt",
    )
    storage = SimpleNamespace(put_file=AsyncMock(), delete=AsyncMock())
    script = SimpleNamespace(id="script-1", owner_user_id="user-1")
    db = SimpleNamespace(
        execute=AsyncMock(return_value=_Result("episode-existing")),
        add=lambda _: None,
        flush=AsyncMock(),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    monkeypatch.setattr(scripts.script_crud, "get_script", AsyncMock(return_value=script))
    monkeypatch.setattr(scripts.script_crud, "get_script_for_update", AsyncMock(return_value=script))
    monkeypatch.setattr(scripts, "check_script_access", AsyncMock())
    monkeypatch.setattr(scripts, "stage_script_document", AsyncMock(return_value=document))
    monkeypatch.setattr(scripts, "cleanup_staged_script_document", lambda _: None)
    monkeypatch.setattr(scripts, "get_storage", lambda: storage)

    with pytest.raises(ConflictError, match="已有分集"):
        await scripts.upload_and_split_script(
            "script-1",
            UploadFile(BytesIO(b"ignored"), filename="完整剧本.txt"),
            True,
            provider=None,
            model=None,
            db=db,
            user=SimpleNamespace(user_id="user-1"),
        )
    storage.put_file.assert_not_awaited()
    storage.delete.assert_not_awaited()


async def test_upload_endpoint_persists_cleanup_when_remote_delete_fails(monkeypatch) -> None:
    document = StagedScriptDocument(
        original_filename="完整剧本.txt",
        suffix=".txt",
        content_type="text/plain",
        size_bytes=12,
        sha256="a" * 64,
        script_text="第一幕：相遇",
        temp_path="/tmp/already-cleaned-script.txt",
    )
    storage = SimpleNamespace(
        put_file=AsyncMock(),
        delete=AsyncMock(side_effect=RuntimeError("remote-delete-failed")),
    )
    script = SimpleNamespace(id="script-1", owner_user_id="user-1")
    db = SimpleNamespace(
        execute=AsyncMock(return_value=_Result(None)),
        add=lambda _: None,
        flush=AsyncMock(side_effect=RuntimeError("db-write-failed")),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    enqueue_delete = AsyncMock()
    monkeypatch.setattr(scripts.script_crud, "get_script", AsyncMock(return_value=script))
    monkeypatch.setattr(scripts.script_crud, "get_script_for_update", AsyncMock(return_value=script))
    monkeypatch.setattr(scripts, "check_script_access", AsyncMock())
    monkeypatch.setattr(scripts, "stage_script_document", AsyncMock(return_value=document))
    monkeypatch.setattr(scripts, "cleanup_staged_script_document", lambda _: None)
    monkeypatch.setattr(scripts, "get_storage", lambda: storage)
    monkeypatch.setattr(scripts.media_storage_jobs, "enqueue_delete", enqueue_delete)

    with pytest.raises(RuntimeError, match="db-write-failed"):
        await scripts.upload_and_split_script(
            "script-1",
            UploadFile(BytesIO(b"ignored"), filename="完整剧本.txt"),
            True,
            provider=None,
            model=None,
            db=db,
            user=SimpleNamespace(user_id="user-1"),
        )

    storage.put_file.assert_awaited_once()
    storage.delete.assert_awaited_once()
    enqueue_delete.assert_awaited_once()
    assert enqueue_delete.await_args.kwargs["source_key"].startswith(
        "upload_scripts/script-1/"
    )
    db.commit.assert_awaited_once()


async def test_analysis_job_uses_latest_attempt_for_each_pipeline_stage(monkeypatch) -> None:
    now = datetime.now(UTC)
    jobs = [
        SimpleNamespace(
            run_id="run-1",
            script_id="script-1",
            episode_id="episode-1",
            stage=stage,
            status="done",
            attempt=2 if stage == "split" else 1,
            error_type=None,
            created_at=now,
            updated_at=now,
        )
        for stage in ("split", "extract", "merge", "enrich", "parents")
    ]
    # 旧失败 attempt 不应使已完成的当前运行显示为 failed。
    jobs.append(
        SimpleNamespace(
            run_id="run-1",
            script_id="script-1",
            episode_id="episode-1",
            stage="split",
            status="failed",
            attempt=1,
            error_type="provider_timeout",
            created_at=now,
            updated_at=now,
        )
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=_Rows(jobs)))
    script = SimpleNamespace(id="script-1")
    monkeypatch.setattr(
        analysis_jobs,
        "_load_access_script",
        AsyncMock(return_value=script),
    )
    monkeypatch.setattr(analysis_jobs, "check_script_access", AsyncMock())

    result = await analysis_jobs.get_analysis_job(
        "run-1", db, SimpleNamespace(user_id="user-1")
    )

    assert result.status == "succeeded"
    assert result.current_stage is None
    assert [stage.status for stage in result.stages] == ["done"] * 5


async def test_analysis_job_rejects_unknown_run() -> None:
    db = SimpleNamespace(execute=AsyncMock(return_value=_Rows([])))
    with pytest.raises(NotFoundError, match="分析任务不存在"):
        await analysis_jobs.get_analysis_job("missing", db, SimpleNamespace(user_id="user-1"))
