from __future__ import annotations

import ast
import asyncio
import base64
import binascii
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import stat
import subprocess
import tempfile
import time
import uuid
import wave
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx
from redis import Redis
from sqlalchemy import create_engine, func, or_, select, text
from sqlalchemy.engine import make_url

BASE_URL = "http://127.0.0.1:18080"
TERMINAL = {"succeeded", "failed", "canceled"}
SETTLED_REMOTE_OUTCOMES = frozenset({"canceled", "deleted", "not_found", "confirmed_absent"})
SELF_TEST_MODE = os.sys.argv[1:] == ["--self-test"]
RUN_ID_PATTERN = re.compile(
    r"20\d{6}-[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)
VALIDATION_TIMEZONE = ZoneInfo("Asia/Shanghai")
EXPECTED_PATH_COUNT = 65
EXPECTED_OPERATION_COUNT = 79
EXPECTED_DATABASE_HOST = "127.0.0.1"
EXPECTED_DATABASE_PORT = 55438
EXPECTED_DATABASE = "mtv_validation"
EXPECTED_DATABASE_USER = "mtv_validation"
EXPECTED_REDIS_HOST = "127.0.0.1"
EXPECTED_REDIS_PORT = 56380
EXPECTED_CELERY_APP = "app.workers.celery_app"
MIN_HTTP_STABLE_WINDOW_SECONDS = 5.0
MAX_HTTP_PREFLIGHT_AGE_SECONDS = 15 * 60
MAX_WORKER_START_AUTH_AGE_SECONDS = 5 * 60
REDIS_STABLE_WINDOW_SECONDS = 1.0
MAX_REMOTE_DELETE_ATTEMPTS = 3
MAX_REMOTE_DELETE_SECONDS = 90.0


def require_validation_run_id(value: str | None, *, now: datetime | None = None) -> str:
    assert isinstance(value, str) and RUN_ID_PATTERN.fullmatch(value), (
        "MTV_VALIDATION_RUN_ID 必须是 YYYYMMDD-UUID（小写十六进制）"
    )
    try:
        run_date = datetime.strptime(value[:8], "%Y%m%d").date()
    except ValueError:
        raise AssertionError("MTV_VALIDATION_RUN_ID 日期部分非法") from None
    reference = now or datetime.now(VALIDATION_TIMEZONE)
    assert reference.tzinfo is not None, "run_id 日期比较必须使用带时区时间"
    assert run_date == reference.astimezone(VALIDATION_TIMEZONE).date(), (
        "MTV_VALIDATION_RUN_ID 日期必须是 Asia/Shanghai 当日"
    )
    return value


def stable_run_hash(run_id: str) -> str:
    return hashlib.sha256(run_id.encode("ascii")).hexdigest()[:12]


def build_seed_ids(run_id: str) -> dict[str, Any]:
    suffix = stable_run_hash(run_id)
    organizations = {
        "primary": f"org-val-{suffix}",
        "foreign": f"org-val-x-{suffix}",
    }
    return {
        "run_hash": suffix,
        "organizations": organizations,
        "users": {
            "owner": {
                "user_id": f"usr-val-o-{suffix}",
                "username": f"mtv_val_owner_{suffix}",
                "org_id": organizations["primary"],
            },
            "member": {
                "user_id": f"usr-val-m-{suffix}",
                "username": f"mtv_val_member_{suffix}",
                "org_id": organizations["primary"],
            },
            "admin": {
                "user_id": f"usr-val-a-{suffix}",
                "username": f"mtv_val_admin_{suffix}",
                "org_id": organizations["primary"],
            },
            "foreign": {
                "user_id": f"usr-val-f-{suffix}",
                "username": f"mtv_val_foreign_{suffix}",
                "org_id": organizations["foreign"],
            },
        },
        "credit_accounts": {
            "org": f"crd-val-org-{suffix}",
            "owner": f"crd-val-o-{suffix}",
            "member": f"crd-val-m-{suffix}",
            "admin": f"crd-val-a-{suffix}",
        },
        "org_codes": {
            "primary": f"mtv-validation-{suffix}",
            "foreign": f"mtv-validation-x-{suffix}",
        },
    }


def run_artifact_paths(run_id: str, *, root: Path = Path("/tmp")) -> dict[str, Path]:
    prefix = root / f"mtv_validation_{run_id}"
    return {
        "seed_manifest": Path(f"{prefix}_seed_manifest.json"),
        "http_result": Path(f"{prefix}_http_result.json"),
        "async_result": Path(f"{prefix}_async_result.json"),
        "remote_ledger": Path(f"{prefix}_remote_video_ledger.json"),
    }


VALIDATION_RUN_ID = (
    "20260719-123e4567-e89b-42d3-a456-426614174000"
    if SELF_TEST_MODE
    else require_validation_run_id(os.environ.get("MTV_VALIDATION_RUN_ID"))
)
RUN_HASH = stable_run_hash(VALIDATION_RUN_ID)
SEED_IDS = build_seed_ids(VALIDATION_RUN_ID)
ARTIFACT_PATHS = run_artifact_paths(VALIDATION_RUN_ID)
SEED_MANIFEST_PATH = ARTIFACT_PATHS["seed_manifest"]
LIVE_RESULT_PATH = ARTIFACT_PATHS["http_result"]
ASYNC_RESULT_PATH = ARTIFACT_PATHS["async_result"]
REMOTE_LEDGER_PATH = ARTIFACT_PATHS["remote_ledger"]
EXPECTED_STORAGE_ROOT = Path(f"/tmp/mtv-validation-storage-{RUN_HASH}")
WORKER_NAME_PREFIX = f"mtv-validation-{RUN_HASH}"
LOGIN = {"username": SEED_IDS["users"]["owner"]["username"], "password": "MtvValidation!2026"}
OWNER_ID = SEED_IDS["users"]["owner"]["user_id"]
OWNER_CREDIT_ID = SEED_IDS["credit_accounts"]["owner"]
EXPECTED_USERS = {
    item["user_id"]: item["org_id"]
    for item in SEED_IDS["users"].values()
}
EXPECTED_ORGANIZATIONS = set(SEED_IDS["organizations"].values())
EVENTS: list[dict[str, Any]] = []
OPENAPI_OPERATIONS: set[tuple[str, str]] = set()


def idempotency_key(label: str) -> str:
    value = f"mtv-{label}-{RUN_HASH}"
    assert len(value) <= 36
    return value


def canonical_sha256(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def signed_payload(payload: dict[str, Any]) -> dict[str, Any]:
    assert "sha256" not in payload
    result = dict(payload)
    result["sha256"] = canonical_sha256(payload)
    return result


def verify_signed_payload(payload: dict[str, Any], *, label: str) -> None:
    digest = payload.get("sha256")
    assert isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest), f"{label} SHA 非法"
    unsigned = dict(payload)
    unsigned.pop("sha256")
    assert canonical_sha256(unsigned) == digest, f"{label} SHA 不匹配"


def _write_bytes_exclusive(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


def write_signed_exclusive(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    result = signed_payload(payload)
    _write_bytes_exclusive(path, json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8"))
    _fsync_directory(path.parent)
    return result


def replace_signed_claim(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    assert path.is_file() and not path.is_symlink(), "批次 claim 缺失或非法"
    temporary = path.with_suffix(path.suffix + ".tmp")
    result = write_signed_exclusive(temporary, payload)
    os.replace(temporary, path)
    _fsync_directory(path.parent)
    return result


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_signed(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    assert path.parent.is_dir() and not path.parent.is_symlink()
    if path.exists() or path.is_symlink():
        assert path.is_file() and not path.is_symlink(), "签名目标不是普通文件"
    temporary = path.with_suffix(path.suffix + ".tmp")
    assert not temporary.exists() and not temporary.is_symlink(), "签名临时文件已存在"
    result = write_signed_exclusive(temporary, payload)
    os.replace(temporary, path)
    _fsync_directory(path.parent)
    return result


def _regular_file_identity(metadata: os.stat_result) -> dict[str, int]:
    assert stat.S_ISREG(metadata.st_mode), "签名制品不是普通文件"
    return {
        "device": int(metadata.st_dev),
        "inode": int(metadata.st_ino),
        "mode": int(stat.S_IMODE(metadata.st_mode)),
        "size": int(metadata.st_size),
        "mtime_ns": int(metadata.st_mtime_ns),
        "link_count": int(metadata.st_nlink),
    }


def _stable_file_identity_matches(
    left: Mapping[str, int],
    right: Mapping[str, int],
) -> bool:
    stable_fields = ("device", "inode", "mode", "size", "mtime_ns")
    return all(left.get(field_name) == right.get(field_name) for field_name in stable_fields)


def _read_signed_json_from_descriptor(
    descriptor: int,
    *,
    label: str,
) -> tuple[dict[str, Any], dict[str, int], bytes]:
    before = _regular_file_identity(os.fstat(descriptor))
    duplicate = os.dup(descriptor)
    with os.fdopen(duplicate, "r", encoding="utf-8", closefd=True) as handle:
        content = handle.read()
    after = _regular_file_identity(os.fstat(descriptor))
    assert before == after, f"{label} 读取期间文件身份或内容元数据变化"
    payload = json.loads(content)
    assert isinstance(payload, dict)
    verify_signed_payload(payload, label=label)
    return payload, before, content.encode("utf-8")


def _open_signed_json_with_identity(
    path: Path,
    *,
    label: str,
) -> tuple[int, dict[str, Any], dict[str, int], bytes]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    try:
        payload, identity, content = _read_signed_json_from_descriptor(
            descriptor,
            label=label,
        )
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise
    return descriptor, payload, identity, content


def read_signed_json_with_identity(
    path: Path,
    *,
    label: str,
) -> tuple[dict[str, Any], dict[str, int]]:
    descriptor, payload, identity, _ = _open_signed_json_with_identity(path, label=label)
    try:
        os.close(descriptor)
    except OSError:
        pass
    return payload, identity


def read_signed_json(path: Path, *, label: str) -> dict[str, Any]:
    payload, _ = read_signed_json_with_identity(path, label=label)
    return payload


def verify_signed_file_identity(
    path: Path,
    *,
    label: str,
    expected_sha256: str,
    expected_identity: dict[str, int],
) -> dict[str, Any]:
    payload, identity = read_signed_json_with_identity(path, label=label)
    assert payload.get("sha256") == expected_sha256, f"{label} SHA 已变化"
    assert identity == expected_identity, f"{label} 文件身份已变化"
    return payload


def _verified_delete_artifact_paths(path: Path) -> dict[str, Path]:
    prefix = f".{path.name}.{RUN_HASH}.verified-delete"
    quarantine_dir = path.parent / prefix
    return {
        "quarantine_dir": quarantine_dir,
        "quarantine_file": quarantine_dir / path.name,
        "tombstone": path.parent / f"{prefix}.commit",
        "evidence": path.parent / f"{prefix}.evidence",
    }


def _path_holds_identity(path: Path, expected_identity: Mapping[str, int]) -> bool:
    if not (path.exists() or path.is_symlink()):
        return False
    try:
        identity = _regular_file_identity(os.stat(path, follow_symlinks=False))
    except (AssertionError, OSError):
        return False
    return _stable_file_identity_matches(identity, expected_identity)


def delete_verified_signed_file(
    path: Path,
    *,
    label: str,
    expected_sha256: str,
    expected_identity: dict[str, int],
) -> dict[str, Any]:
    """保持验签 fd 至 unlink 后，只提交同一单链接 inode 的删除。"""
    artifacts = _verified_delete_artifact_paths(path)
    quarantine_dir = artifacts["quarantine_dir"]
    quarantine_path = artifacts["quarantine_file"]
    tombstone_path = artifacts["tombstone"]
    evidence_path = artifacts["evidence"]
    assert path.parent.is_dir() and not path.parent.is_symlink()
    for artifact in artifacts.values():
        assert not artifact.exists() and not artifact.is_symlink(), (
            f"{label} 存在上一轮 verified-delete 残留: {artifact.name}"
        )
    descriptor, payload, opened_identity, signed_content = _open_signed_json_with_identity(
        path,
        label=label,
    )
    parent_descriptor: int | None = None
    quarantine_descriptor: int | None = None
    committed = False
    try:
        assert payload.get("sha256") == expected_sha256, f"{label} SHA 已变化"
        assert opened_identity == expected_identity, f"{label} 文件身份已变化"
        assert opened_identity["link_count"] == 1, f"{label} 存在额外硬链接"
        source_identity = _regular_file_identity(os.stat(path, follow_symlinks=False))
        assert source_identity == opened_identity, f"{label} 路径与验签 fd 身份不一致"

        quarantine_dir.mkdir(mode=0o700)
        directory_flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        parent_descriptor = os.open(path.parent, directory_flags)
        quarantine_descriptor = os.open(quarantine_dir, directory_flags)
        quarantine_directory_identity = os.fstat(quarantine_descriptor)
        assert stat.S_ISDIR(quarantine_directory_identity.st_mode)
        assert stat.S_IMODE(quarantine_directory_identity.st_mode) == 0o700
        os.rename(
            path.name,
            path.name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=quarantine_descriptor,
        )
        os.fsync(parent_descriptor)
        _fsync_directory(path.parent)
        assert not path.exists() and not path.is_symlink(), (
            f"{label} 移入隔离目录后原路径被重新占用，保留证据"
        )
        quarantine_identity = _regular_file_identity(
            os.stat(quarantine_path, follow_symlinks=False)
        )
        descriptor_identity = _regular_file_identity(os.fstat(descriptor))
        assert quarantine_identity == descriptor_identity == opened_identity, (
            f"{label} quarantine 路径与持续打开的验签 fd 不一致"
        )
        assert _regular_file_identity(
            os.stat(path.name, dir_fd=quarantine_descriptor, follow_symlinks=False)
        ) == opened_identity
        os.fsync(quarantine_descriptor)
        _fsync_directory(quarantine_dir)

        descriptor_identity = _regular_file_identity(os.fstat(descriptor))
        assert descriptor_identity == opened_identity
        assert descriptor_identity["link_count"] == 1, f"{label} 删除提交前出现额外硬链接"
        os.unlink(path.name, dir_fd=quarantine_descriptor)
        post_unlink_identity = _regular_file_identity(os.fstat(descriptor))
        assert _stable_file_identity_matches(post_unlink_identity, opened_identity), (
            f"{label} unlink 后 fd 身份变化"
        )
        assert post_unlink_identity["link_count"] == 0, (
            f"{label} unlink 后 inode 仍有外部硬链接，拒绝报告删除成功"
        )
        os.fsync(quarantine_descriptor)
        _fsync_directory(quarantine_dir)
        current_quarantine_directory = os.stat(quarantine_dir, follow_symlinks=False)
        assert (
            current_quarantine_directory.st_dev == quarantine_directory_identity.st_dev
            and current_quarantine_directory.st_ino == quarantine_directory_identity.st_ino
        ), f"{label} quarantine 目录路径已被替换"
        os.rmdir(quarantine_dir.name, dir_fd=parent_descriptor)
        os.fstat(quarantine_descriptor)
        os.fsync(parent_descriptor)
        assert not quarantine_dir.exists() and not quarantine_dir.is_symlink()
        committed = True
    except BaseException as original:
        if quarantine_descriptor is not None and parent_descriptor is not None:
            try:
                relative_identity = _regular_file_identity(
                    os.stat(
                        path.name,
                        dir_fd=quarantine_descriptor,
                        follow_symlinks=False,
                    )
                )
            except (AssertionError, OSError):
                relative_identity = None
            if (
                relative_identity is not None
                and _stable_file_identity_matches(relative_identity, opened_identity)
                and not (path.exists() or path.is_symlink())
            ):
                try:
                    os.rename(
                        path.name,
                        path.name,
                        src_dir_fd=quarantine_descriptor,
                        dst_dir_fd=parent_descriptor,
                    )
                    os.fsync(parent_descriptor)
                except BaseException as restore_error:
                    original.add_note(
                        f"{label} 通过 dirfd 恢复证据失败: {type(restore_error).__name__}"
                    )
        for candidate in (tombstone_path, quarantine_path):
            if not (candidate.exists() or candidate.is_symlink()):
                continue
            if not _path_holds_identity(candidate, opened_identity):
                continue
            if not (path.exists() or path.is_symlink()):
                try:
                    os.rename(candidate, path)
                    _fsync_directory(path.parent)
                except BaseException as restore_error:
                    original.add_note(
                        f"{label} 恢复 verified-delete 证据失败: {type(restore_error).__name__}"
                    )
            break

        try:
            if (
                quarantine_descriptor is not None
                and parent_descriptor is not None
                and not os.listdir(quarantine_descriptor)
                and quarantine_dir.exists()
                and not quarantine_dir.is_symlink()
            ):
                current_directory = os.stat(quarantine_dir, follow_symlinks=False)
                held_directory = os.fstat(quarantine_descriptor)
                assert current_directory.st_dev == held_directory.st_dev
                assert current_directory.st_ino == held_directory.st_ino
                os.rmdir(quarantine_dir.name, dir_fd=parent_descriptor)
                os.fsync(parent_descriptor)
        except BaseException as cleanup_error:
            original.add_note(
                f"{label} 回收空 quarantine 目录失败: {type(cleanup_error).__name__}"
            )

        known_evidence = (path, quarantine_path, tombstone_path)
        descriptor_was_expected = _stable_file_identity_matches(
            opened_identity,
            expected_identity,
        )
        if descriptor_was_expected and not any(
            _path_holds_identity(candidate, expected_identity) for candidate in known_evidence
        ):
            try:
                _write_bytes_exclusive(evidence_path, signed_content)
                _fsync_directory(path.parent)
            except BaseException as evidence_error:
                original.add_note(
                    f"{label} 固化 fd 证据失败: {type(evidence_error).__name__}"
                )
        raise
    finally:
        for directory_descriptor in (quarantine_descriptor, parent_descriptor):
            if directory_descriptor is None:
                continue
            try:
                os.close(directory_descriptor)
            except OSError:
                pass
        try:
            os.close(descriptor)
        except OSError:
            pass
    assert committed
    return {
        "deleted": True,
        "sha256": expected_sha256,
        "file_identity": dict(expected_identity),
        "fd_held_through_unlink": True,
        "quarantine_dirfd_held_through_unlink": True,
        "quarantine_directory_unlinked_verified": True,
        "same_user_directory_adversary": "fail_closed_on_detected_identity_change",
        "pre_unlink_link_count": 1,
        "post_unlink_link_count": 0,
    }


def validate_evidence_chain(
    seed: dict[str, Any],
    http_result: dict[str, Any],
    *,
    async_started_at: datetime,
) -> None:
    verify_signed_payload(seed, label="seed manifest")
    verify_signed_payload(http_result, label="HTTP result")
    assert seed.get("status") == http_result.get("status") == "passed"
    assert seed.get("run_id") == http_result.get("run_id") == VALIDATION_RUN_ID
    assert seed.get("seed_ids") == SEED_IDS
    assert http_result.get("seed_manifest_sha256") == seed.get("sha256")
    seed_started = datetime.fromisoformat(str(seed["started_at"]))
    seed_finished = datetime.fromisoformat(str(seed["finished_at"]))
    http_started = datetime.fromisoformat(str(http_result["started_at"]))
    http_finished = datetime.fromisoformat(str(http_result["finished_at"]))
    assert all(value.tzinfo is not None for value in (seed_started, seed_finished, http_started, http_finished))
    assert seed_started <= seed_finished <= http_started <= http_finished <= async_started_at
    assert (async_started_at - http_finished).total_seconds() <= 4 * 60 * 60, "HTTP 证据已过期"


def claim_async_result(started_at: datetime) -> None:
    assert not ASYNC_RESULT_PATH.exists() and not ASYNC_RESULT_PATH.is_symlink(), (
        "本 nonce 已有 async result，失败尝试不可复用"
    )
    async_temporary = ASYNC_RESULT_PATH.with_suffix(ASYNC_RESULT_PATH.suffix + ".tmp")
    assert not async_temporary.exists() and not async_temporary.is_symlink()
    assert not REMOTE_LEDGER_PATH.exists() and not REMOTE_LEDGER_PATH.is_symlink(), (
        "本 nonce 已有远端债务账本，拒绝复用"
    )
    ledger_temporary = REMOTE_LEDGER_PATH.with_suffix(REMOTE_LEDGER_PATH.suffix + ".tmp")
    assert not ledger_temporary.exists() and not ledger_temporary.is_symlink()
    write_signed_exclusive(
        ASYNC_RESULT_PATH,
        {
            "status": "running",
            "run_id": VALIDATION_RUN_ID,
            "started_at": started_at.isoformat(),
            "finished_at": None,
        },
    )


def persist_async_preflight(
    *,
    live_result: dict[str, Any],
    worker_evidence: dict[str, Any],
    redis_baseline: dict[str, set[str]],
) -> dict[str, Any]:
    """首次预算登记前把 worker 后置门禁写回本轮签名 claim。"""
    assert redis_baseline == {"app": set(), "broker": set(), "backend": set()}
    current = read_signed_json(ASYNC_RESULT_PATH, label="async running claim")
    assert current.get("status") == "awaiting_worker" and current.get("run_id") == VALIDATION_RUN_ID
    worker_start_authorization = current.get("worker_start_authorization")
    assert isinstance(worker_start_authorization, dict)
    verify_signed_payload(worker_start_authorization, label="worker start authorization")
    preflight = signed_payload(
        {
            "run_id": VALIDATION_RUN_ID,
            "captured_at": datetime.now(UTC).isoformat(),
            "http_result_sha256": live_result["sha256"],
            "redis_baseline_empty": {"app": True, "broker": True, "backend": True},
            "worker_start_authorization_sha256": worker_start_authorization["sha256"],
            "worker": worker_evidence,
        }
    )
    updated = {
        "status": "running",
        "run_id": VALIDATION_RUN_ID,
        "started_at": current["started_at"],
        "finished_at": None,
        "worker_start_authorization": worker_start_authorization,
        "preflight": preflight,
        "dispatch_delivery_ledger": [],
    }
    written = replace_signed_claim(ASYNC_RESULT_PATH, updated)
    verified = read_signed_json(ASYNC_RESULT_PATH, label="async preflight claim")
    assert verified == written
    verify_signed_payload(verified["preflight"], label="async preflight")
    return preflight


def _dispatch_attempt_projection(
    attempts: frozenset[tuple[str, str, int]],
) -> list[dict[str, Any]]:
    assert isinstance(attempts, frozenset)
    return [
        {"task_type": task_type, "job_id": job_id, "attempt": attempt}
        for task_type, job_id, attempt in sorted(attempts)
    ]


def persist_dispatch_delivery_state(
    *,
    task_id: str,
    task_name: str,
    purpose: str,
    dispatch_attempts: frozenset[tuple[str, str, int]],
    state: str,
    reason: str | None = None,
    error_type: str | None = None,
) -> dict[str, Any]:
    """原子持久化无敏感派发状态；sending/uncertain 均禁止自动重发。"""
    task_id = validated_celery_result_id(task_id)
    assert state in {"sending", "confirmed", "uncertain"}
    assert reason is None or reason in {
        "send_exception",
        "invalid_result_id",
        "confirmation_persist_failed",
        "audit_failed",
        "result_tree_failed",
    }
    assert error_type is None or (error_type.isidentifier() and len(error_type) <= 128)
    current = read_signed_json(ASYNC_RESULT_PATH, label="async dispatch claim")
    assert current.get("status") == "running" and current.get("run_id") == VALIDATION_RUN_ID
    ledger = current.get("dispatch_delivery_ledger", [])
    assert isinstance(ledger, list)
    records = [dict(record) for record in ledger]
    matching = [record for record in records if record.get("task_id") == task_id]
    attempts = _dispatch_attempt_projection(dispatch_attempts)
    now = datetime.now(UTC).isoformat()
    if state == "sending":
        assert matching == [], "预生成 task ID 已存在派发状态"
        record = {
            "task_id": task_id,
            "task_name": task_name,
            "purpose": purpose,
            "dispatch_attempts": attempts,
            "state": "sending",
            "created_at": now,
            "updated_at": now,
            "reason": None,
            "error_type": None,
            "retry_allowed": False,
        }
        records.append(record)
    else:
        assert len(matching) == 1, "派发终态缺少唯一 sending 记录"
        record = matching[0]
        assert record.get("task_name") == task_name
        assert record.get("purpose") == purpose
        assert record.get("dispatch_attempts") == attempts
        assert record.get("state") in {"sending", "confirmed"}
        record.update(
            {
                "state": state,
                "updated_at": now,
                "reason": reason,
                "error_type": error_type,
                "retry_allowed": False,
            }
        )
    unsigned = dict(current)
    unsigned.pop("sha256", None)
    unsigned["dispatch_delivery_ledger"] = sorted(
        records,
        key=lambda item: (str(item["created_at"]), str(item["task_id"])),
    )
    replace_signed_claim(ASYNC_RESULT_PATH, unsigned)
    return dict(record)


def assert_database_url(label: str, raw_url: str) -> dict[str, Any]:
    url = make_url(raw_url)
    assert url.get_backend_name() == "postgresql", f"{label} 不是 PostgreSQL"
    assert url.drivername in {"postgresql+asyncpg", "postgresql+psycopg2"}, f"{label} driver 非法"
    assert not url.query, f"{label} 禁止 query 参数覆盖连接身份"
    assert url.host == EXPECTED_DATABASE_HOST, f"{label} host 非隔离实例"
    assert url.port == EXPECTED_DATABASE_PORT, f"{label} port 非隔离实例"
    assert url.database == EXPECTED_DATABASE, f"{label} database 非隔离库"
    assert url.username == EXPECTED_DATABASE_USER, f"{label} user 非隔离用户"
    return {
        "driver": url.drivername.split("+", 1)[-1],
        "host": url.host,
        "port": url.port,
        "database": url.database,
        "user": url.username,
    }


def parse_redis_target(label: str, raw_url: str, expected_db: int) -> dict[str, Any]:
    parsed = urlsplit(raw_url)
    assert parsed.scheme == "redis", f"{label} scheme 非法"
    assert parsed.username is None and parsed.password is None, f"{label} 禁止用户名或密码"
    assert not parsed.query and not parsed.fragment, f"{label} 禁止 query/fragment"
    assert parsed.netloc == f"{EXPECTED_REDIS_HOST}:{EXPECTED_REDIS_PORT}", f"{label} authority 非法"
    assert parsed.hostname == EXPECTED_REDIS_HOST and parsed.port == EXPECTED_REDIS_PORT
    assert parsed.path == f"/{expected_db}", f"{label} 必须使用 Redis DB {expected_db}"
    return {"scheme": parsed.scheme, "host": parsed.hostname, "port": parsed.port, "db": expected_db}


def assert_redis_urls(redis_url: str, broker_url: str, backend_url: str) -> dict[str, dict[str, Any]]:
    targets = {
        "app": parse_redis_target("应用 Redis", redis_url, 0),
        "broker": parse_redis_target("Celery broker", broker_url, 1),
        "backend": parse_redis_target("Celery result backend", backend_url, 2),
    }
    assert len({(item["scheme"], item["host"], item["port"]) for item in targets.values()}) == 1
    return targets


def parse_ps_processes(output: str) -> list[dict[str, Any]]:
    """解析固定 ps 格式；坏行按失败关闭处理。"""
    processes: list[dict[str, Any]] = []
    seen_pids: set[int] = set()
    for line_number, line in enumerate(output.splitlines(), start=1):
        if not line.strip():
            continue
        match = re.fullmatch(r"\s*(\d+)\s+(\d+)\s+(.+?)\s*", line)
        assert match is not None, f"ps 第 {line_number} 行格式非法"
        pid = int(match.group(1))
        ppid = int(match.group(2))
        command = match.group(3)
        assert pid > 0 and ppid >= 0 and pid not in seen_pids and command
        seen_pids.add(pid)
        processes.append({"pid": pid, "ppid": ppid, "command": command})
    assert processes, "ps 未返回任何进程，拒绝形成隔离证据"
    return processes


_CELERY_EXECUTABLES = frozenset({"celery", "celerybeat", "celeryd"})
_CELERY_SUSPECT_PATTERN = re.compile(
    r"(?i)(?<![a-z0-9_])celery(?:beat|d)?(?=$|[^a-z0-9_])"
)


def _celery_command(tokens: list[str]) -> tuple[int, str, str] | None:
    if not tokens:
        return None
    executable = Path(tokens[0]).name
    if executable in _CELERY_EXECUTABLES:
        return 0, "cli", executable
    if re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", executable):
        if len(tokens) > 1 and Path(tokens[1]).name in _CELERY_EXECUTABLES:
            script_name = Path(tokens[1]).name
            return 1, "python-script", script_name
        if tokens[1:3] == ["-m", "celery"]:
            return 2, "python-module", "celery"
    return None


def _celery_child_process_title(command: str) -> bool:
    return re.match(r"^\[?celeryd:", command.strip(), flags=re.IGNORECASE) is not None


def _looks_like_celery_process(command: str, tokens: list[str]) -> bool:
    return bool(_CELERY_SUSPECT_PATTERN.search(command)) or any(
        Path(token.strip("[](){}'\"")).name.rstrip(":").lower()
        in _CELERY_EXECUTABLES
        for token in tokens
    )


def _celery_worker_app(tokens: list[str], celery_index: int) -> str:
    """只接受目标 app 参数后紧跟 worker 子命令。"""
    trailing = tokens[celery_index + 1 :]
    accepted_prefixes = (
        ["-A", EXPECTED_CELERY_APP, "worker"],
        ["--app", EXPECTED_CELERY_APP, "worker"],
        [f"-A{EXPECTED_CELERY_APP}", "worker"],
        [f"--app={EXPECTED_CELERY_APP}", "worker"],
    )
    suffix: list[str] | None = None
    for prefix in accepted_prefixes:
        if trailing[: len(prefix)] == prefix:
            suffix = trailing[len(prefix) :]
            break
    assert suffix is not None, "Celery 命令不是精确的目标 app worker 子命令"
    assert not any(
        token in {"worker", "beat", "-A", "--app", "-B"}
        or token.startswith(("-A", "--app=", "--beat"))
        for token in suffix
    ), "Celery worker 后缀包含冲突子命令或 app/beat 参数"
    return EXPECTED_CELERY_APP


def evaluate_celery_processes(
    processes: list[dict[str, Any]],
    *,
    expect_worker: bool,
) -> dict[str, Any]:
    """只接受目标 app 的 worker 进程树，任何 beat 或模糊 Celery 命令都拒绝。"""
    celery_processes: list[dict[str, Any]] = []
    deferred_child_titles: set[int] = set()
    for process in processes:
        command = str(process["command"])
        try:
            tokens = shlex.split(command, posix=True)
        except ValueError:
            assert not _CELERY_SUSPECT_PATTERN.search(command), (
                f"疑似 Celery 进程命令无法解析 pid={process['pid']}"
            )
            continue
        celery = _celery_command(tokens)
        if celery is None:
            if _celery_child_process_title(command):
                deferred_child_titles.add(int(process["pid"]))
                continue
            assert not _looks_like_celery_process(command, tokens), (
                f"疑似 Celery 命令入口无法归属 pid={process['pid']}"
            )
            continue
        celery_index, invocation, executable_kind = celery
        assert executable_kind != "celerybeat", f"检测到 Celery beat pid={process['pid']}"
        assert executable_kind != "celeryd", (
            f"检测到无法按精确 worker 子命令归属的 celeryd pid={process['pid']}"
        )
        app = _celery_worker_app(tokens, celery_index)
        celery_processes.append(
            {
                "pid": int(process["pid"]),
                "ppid": int(process["ppid"]),
                "mode": "worker",
                "app": app,
                "invocation": invocation,
                "command_sha256": hashlib.sha256(command.encode("utf-8")).hexdigest(),
            }
        )

    worker_pids = {item["pid"] for item in celery_processes}
    while True:
        descendants = [
            process
            for process in processes
            if int(process["pid"]) not in worker_pids and int(process["ppid"]) in worker_pids
        ]
        if not descendants:
            break
        for process in descendants:
            command = str(process["command"])
            worker_pids.add(int(process["pid"]))
            celery_processes.append(
                {
                    "pid": int(process["pid"]),
                    "ppid": int(process["ppid"]),
                    "mode": "worker-child",
                    "app": EXPECTED_CELERY_APP,
                    "invocation": "trusted-descendant",
                    "command_sha256": hashlib.sha256(command.encode("utf-8")).hexdigest(),
                }
            )
    orphan_celeryd = sorted(deferred_child_titles - worker_pids)
    assert orphan_celeryd == [], f"发现无法归属到隔离 worker 根进程的 celeryd: {orphan_celeryd}"
    roots = sorted(item["pid"] for item in celery_processes if item["ppid"] not in worker_pids)
    if expect_worker:
        assert len(roots) == 1 and celery_processes, "必须恰有一个隔离 Celery worker 进程树"
    else:
        assert not celery_processes and not roots, "worker shutdown 后仍有 Celery 进程"
    return {
        "expected_worker": expect_worker,
        "worker_root_count": len(roots),
        "worker_root_pids": roots,
        "worker_process_count": len(celery_processes),
        "worker_process_pids": sorted(worker_pids),
        "beat_process_count": 0,
        "celery_app": EXPECTED_CELERY_APP,
        "processes": celery_processes,
    }


def collect_celery_process_evidence(*, expect_worker: bool) -> dict[str, Any]:
    completed = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,command="],
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert completed.stderr == "", "ps 输出 stderr，拒绝形成进程证据"
    evidence = evaluate_celery_processes(
        parse_ps_processes(completed.stdout),
        expect_worker=expect_worker,
    )
    evidence["observed_at"] = datetime.now(UTC).isoformat()
    return evidence


def is_application_error_contract(status: int, body: Any) -> bool:
    return bool(
        400 <= status < 500
        and isinstance(body, dict)
        and set(body) == {"detail"}
        and isinstance(body["detail"], str)
        and body["detail"].strip()
        and not (status == 404 and body["detail"] == "Not Found")
    )


def is_handler_evidence(status: int, body: Any) -> bool:
    if status in {401, 405} or status >= 500:
        return False
    return 200 <= status < 400 or is_application_error_contract(status, body)


def persisted_remote_outcome(payload: Mapping[str, Any]) -> str:
    """只读取结构化 outcome，禁止根据 detail 文本推导远端事实。"""
    return str(payload.get("outcome") or "pending")


def direct_remote_cleanup_state(provider_outcome: str) -> tuple[str, bool]:
    """将 Provider 直接清理结果映射为独立账本状态。"""
    assert provider_outcome in {"canceled", "failed", "unsupported"}
    if provider_outcome == "canceled":
        return "deleted", False
    return provider_outcome, provider_outcome == "failed"


@dataclass
class RemoteVideoTask:
    job_id: str
    provider_key: str
    provider_task_id: str
    local_status: str
    outcome: str = "pending"
    retryable: bool = True
    attempts: int = 0
    detail: str | None = None
    updated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    @property
    def terminal(self) -> bool:
        return self.outcome in SETTLED_REMOTE_OUTCOMES


def _remote_settlement_projection(payload: Mapping[str, Any]) -> dict[str, Any]:
    job_id = payload.get("job_id")
    provider_key = payload.get("provider_key")
    provider_task_id = payload.get("provider_task_id")
    local_status = payload.get("local_status")
    outcome = payload.get("outcome")
    retryable = payload.get("retryable")
    attempts = payload.get("attempts")
    detail = payload.get("detail")
    updated_at = payload.get("updated_at")
    terminal = payload.get("terminal")
    assert isinstance(job_id, str) and job_id
    assert isinstance(provider_key, str) and provider_key
    assert isinstance(provider_task_id, str) and provider_task_id
    assert isinstance(local_status, str) and local_status
    assert isinstance(outcome, str) and outcome
    assert isinstance(retryable, bool)
    assert isinstance(attempts, int) and not isinstance(attempts, bool) and attempts >= 0
    assert detail is None or isinstance(detail, str)
    assert isinstance(updated_at, str)
    parsed_updated_at = datetime.fromisoformat(updated_at)
    assert parsed_updated_at.tzinfo is not None
    assert isinstance(terminal, bool)
    return {
        "job_id": job_id,
        "provider_key": provider_key,
        "provider_task_id": provider_task_id,
        "local_status": local_status,
        "outcome": outcome,
        "retryable": retryable,
        "attempts": attempts,
        "detail": detail,
        "updated_at": updated_at,
        "terminal": terminal,
    }


def remote_task_payload(task: RemoteVideoTask) -> dict[str, Any]:
    return _remote_settlement_projection({**asdict(task), "terminal": task.terminal})


@dataclass
class CleanupRegistry:
    script_id: str
    episode_id: str
    material_id: str
    storage_keys: set[str]
    generation_jobs: set[str] = field(default_factory=set)
    pipeline_runs: set[str] = field(default_factory=set)
    storyboard_jobs: set[str] = field(default_factory=set)
    video_jobs: set[str] = field(default_factory=set)
    render_jobs: set[str] = field(default_factory=set)
    media_jobs: set[str] = field(default_factory=set)
    reservation_sources: set[tuple[str, str]] = field(default_factory=set)
    remote_video_tasks: dict[str, RemoteVideoTask] = field(default_factory=dict)
    worker_name: str | None = None
    worker_root_pid: int | None = None
    worker_shutdown_evidence: dict[str, Any] | None = None
    ledger_sealed: bool = False
    cleanup_armed: bool = False
    cleanup_authorization_sha256: str | None = None
    success: bool = False

    def register_remote_video_task(
        self,
        *,
        job_id: str,
        provider_key: str,
        provider_task_id: str,
        local_status: str,
    ) -> RemoteVideoTask:
        assert job_id and provider_key and provider_task_id
        _, persisted_output = video_job_snapshot(job_id)
        assert str((persisted_output or {}).get("provider_task_id") or "") == provider_task_id, (
            "远端 task ID 尚未在数据库提交可见，拒绝登记独立账本"
        )
        existing = self.remote_video_tasks.get(job_id)
        if existing is not None:
            assert existing.provider_key == provider_key
            assert existing.provider_task_id == provider_task_id
            return self.update_remote_video_task(existing, local_status=local_status)
        else:
            task = RemoteVideoTask(
                job_id=job_id,
                provider_key=provider_key,
                provider_task_id=provider_task_id,
                local_status=local_status,
            )
            self.remote_video_tasks[job_id] = task
        self.persist_remote_ledger()
        return task

    def update_remote_video_task(
        self,
        task: RemoteVideoTask,
        **changes: Any,
    ) -> RemoteVideoTask:
        assert self.remote_video_tasks.get(task.job_id) is task
        allowed = {"local_status", "outcome", "retryable", "attempts", "detail"}
        assert changes and set(changes) <= allowed
        for key, value in changes.items():
            setattr(task, key, value)
        task.updated_at = datetime.now(UTC).isoformat()
        remote_task_payload(task)
        self.persist_remote_ledger()
        return task

    def persist_remote_ledger(self) -> None:
        assert not self.ledger_sealed, "远端 ledger 已封存，禁止在最终核验后改写"
        written = atomic_write_signed(
            REMOTE_LEDGER_PATH,
            {
                "status": "open",
                "run_id": VALIDATION_RUN_ID,
                "script_id": self.script_id,
                "updated_at": datetime.now(UTC).isoformat(),
                "tasks": {
                    job_id: remote_task_payload(task)
                    for job_id, task in sorted(self.remote_video_tasks.items())
                },
            },
        )
        assert read_signed_json(REMOTE_LEDGER_PATH, label="persisted remote video ledger") == written


def arm_cleanup_after_worker_authorization(
    registry: CleanupRegistry,
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    """授权一旦成功即武装回收，后续 OpenAPI、登录和前置检查失败也不跳过。"""
    verify_signed_payload(dict(authorization), label="cleanup worker authorization")
    assert authorization.get("run_id") == VALIDATION_RUN_ID
    assert authorization.get("required_worker") == {
        "app": EXPECTED_CELERY_APP,
        "worker_name_prefix": WORKER_NAME_PREFIX,
        "beat_allowed": False,
    }
    registry.cleanup_armed = True
    registry.cleanup_authorization_sha256 = str(authorization["sha256"])
    return {
        "armed": True,
        "basis": "verified_worker_start_authorization",
        "worker_name_prefix": WORKER_NAME_PREFIX,
        "authorization_sha256": registry.cleanup_authorization_sha256,
    }


def recover_armed_preflight_failure(
    registry: CleanupRegistry,
    redis_baseline: dict[str, set[str]],
    recoverer: Callable[[CleanupRegistry, dict[str, set[str]]], dict[str, Any]],
) -> dict[str, Any]:
    """OpenAPI/登录/前置失败时回收授权 worker；证据不足则明确 residual。"""
    assert registry.cleanup_armed
    assert registry.worker_name is None and registry.worker_root_pid is None
    assert redis_baseline == {"app": set(), "broker": set(), "backend": set()}
    try:
        recovery = recoverer(registry, redis_baseline)
        assert recovery.get("completed") is True
        assert recovery.get("worker_absent") is True
    except BaseException as exc:
        return {
            "status": "residual",
            "worker_authorization_recovery": {
                "completed": False,
                "error_type": type(exc).__name__,
                "evidence_basis": "nonce+inspect_destination+root_pid_required",
            },
            "local_records_retained": True,
        }
    return {
        "status": "residual",
        "worker_authorization_recovery": recovery,
        "local_records_retained": True,
        "reason": "failure_before_worker_identity_bound_to_registry",
    }


@dataclass
class PaidBudgetLedger:
    dispatch_attempts: dict[tuple[str, str], int] = field(default_factory=dict)
    celery_dispatch_task_ids: set[str] = field(default_factory=set)
    celery_dispatch_records: list[dict[str, Any]] = field(default_factory=list)
    runtime_gate_evidence: list[dict[str, Any]] = field(default_factory=list)
    owned_backend_keys: set[str] = field(default_factory=set)
    async_result_roots: list[Any] = field(default_factory=list, repr=False)
    dispatch_delivery_states: dict[str, str] = field(default_factory=dict)
    worker_prerequisites_verified: bool = False
    workflow_prerequisites_verified: bool = False
    text_prerequisites_verified: bool = False
    media_prerequisites_verified: bool = False

    LIMITS = {
        "pipeline.dag": 2,
        "storyboard.generate": 1,
        "generation.generate_images": 2,
        "video.generate": 2,
        "timeline.render": 4,
    }

    def record_dispatch(self, expectations: frozenset[tuple[str, str, int]]) -> None:
        assert isinstance(expectations, frozenset), "预算登记必须使用不可变 dispatch attempts"
        if not expectations:
            return
        assert self.worker_prerequisites_verified, "首次派发前未验证本轮唯一 worker"
        assert self.workflow_prerequisites_verified, "基础业务前提尚未验证"
        pending: dict[tuple[str, str], int] = {}
        for task_type, job_id, attempt in sorted(expectations):
            assert task_type in self.LIMITS, f"未知 outbox task_type: {task_type}"
            assert attempt >= 1
            if task_type in {"pipeline.dag", "storyboard.generate"}:
                assert self.text_prerequisites_verified, "文本/分镜 Provider 前提尚未验证"
            if task_type in {
                "generation.generate_images",
                "video.generate",
                "timeline.render",
            }:
                assert self.media_prerequisites_verified, "镜头/素材前提尚未验证"
            key = (task_type, job_id)
            assert key not in self.dispatch_attempts and key not in pending, (
                "同一业务任务被重复派发"
            )
            pending[key] = attempt
            count = sum(
                recorded_type == task_type
                for recorded_type, _ in (*self.dispatch_attempts, *pending)
            )
            assert count <= self.LIMITS[task_type], f"{task_type} 超过最小付费预算"
        self.dispatch_attempts.update(pending)

    def record_celery_dispatch(
        self,
        task_id: str,
        *,
        task_name: str,
        purpose: str,
        runtime_gate: dict[str, Any],
    ) -> None:
        task_id = validated_celery_result_id(task_id)
        assert task_id not in self.celery_dispatch_task_ids
        assert self.dispatch_delivery_states.get(task_id) == "confirmed"
        assert task_name and purpose
        self.celery_dispatch_task_ids.add(task_id)
        self.register_task_result_id(task_id)
        assert runtime_gate.get("worker_count") == 1
        assert runtime_gate.get("process", {}).get("beat_process_count") == 0
        gate_record = {
            **runtime_gate,
            "task_id": task_id,
            "task_name": task_name,
            "purpose": purpose,
        }
        self.runtime_gate_evidence.append(gate_record)
        self.celery_dispatch_records.append(
            {
                "task_id": task_id,
                "task_name": task_name,
                "purpose": purpose,
                "worker_name": runtime_gate.get("worker_name"),
                "worker_root_pid": runtime_gate.get("worker_root_pid"),
                "observed_at": runtime_gate.get("observed_at"),
            }
        )

    def begin_dispatch_delivery(self, task_id: str) -> None:
        task_id = validated_celery_result_id(task_id)
        assert task_id not in self.dispatch_delivery_states
        self.dispatch_delivery_states[task_id] = "sending"
        self.register_task_result_id(task_id)

    def mark_dispatch_delivery(self, task_id: str, state: str) -> None:
        task_id = validated_celery_result_id(task_id)
        assert state in {"confirmed", "uncertain"}
        assert self.dispatch_delivery_states.get(task_id) in {"sending", "confirmed"}
        self.dispatch_delivery_states[task_id] = state

    def register_task_result_id(self, task_id: str) -> None:
        task_id = validated_celery_result_id(task_id)
        self.owned_backend_keys.add(f"celery-task-meta-{task_id}")

    def register_group_result_id(self, group_id: str) -> None:
        group_id = validated_celery_result_id(group_id)
        for prefix in ("celery-taskset-meta", "celery-group-meta"):
            self.owned_backend_keys.add(f"{prefix}-{group_id}")
            self.owned_backend_keys.update(
                f"{prefix}-{group_id}.{suffix}" for suffix in ("j", "t", "s")
            )

    def job_ids(self, task_type: str) -> set[str]:
        return {job_id for recorded_type, job_id in self.dispatch_attempts if recorded_type == task_type}

    @property
    def pipeline_dags(self) -> set[str]:
        return self.job_ids("pipeline.dag")

    @property
    def storyboard_tasks(self) -> set[str]:
        return self.job_ids("storyboard.generate")

    @property
    def image_requests(self) -> set[str]:
        return self.job_ids("generation.generate_images")

    @property
    def video_tasks(self) -> set[str]:
        return self.job_ids("video.generate")

    @property
    def render_tasks(self) -> set[str]:
        return self.job_ids("timeline.render")

    def assert_business_final(self) -> None:
        actual = {
            task_type: sum(recorded_type == task_type for recorded_type, _ in self.dispatch_attempts)
            for task_type in self.LIMITS
        }
        assert actual == self.LIMITS, actual
        expected_paid_dispatches = sum(self.LIMITS.values())
        paid_records = [
            record
            for record in self.celery_dispatch_records
            if record["purpose"] == "business_outbox"
        ]
        assert len(paid_records) == expected_paid_dispatches, "付费 outbox 派发审计数量异常"
        self.assert_runtime_audit()

    def assert_final(
        self,
        *,
        media_recovery_runs: int,
        media_recovery_stop_reason: str,
        remote_task_count: int,
    ) -> dict[str, Any]:
        self.assert_business_final()
        return dispatch_integrity_summary(
            self,
            media_recovery_runs=media_recovery_runs,
            media_recovery_stop_reason=media_recovery_stop_reason,
            remote_task_count=remote_task_count,
        )

    def assert_runtime_audit(self) -> None:
        assert len(self.runtime_gate_evidence) == len(self.celery_dispatch_records)
        assert len(self.celery_dispatch_task_ids) == len(self.celery_dispatch_records)
        assert {
            record["task_id"] for record in self.celery_dispatch_records
        } == self.celery_dispatch_task_ids
        assert set(self.dispatch_delivery_states) == self.celery_dispatch_task_ids
        assert set(self.dispatch_delivery_states.values()) == {"confirmed"}


def dispatch_integrity_summary(
    ledger: PaidBudgetLedger,
    *,
    media_recovery_runs: int,
    media_recovery_stop_reason: str,
    remote_task_count: int,
) -> dict[str, Any]:
    """从最终 ledger 生成摘要，并拒绝预算、门禁和派发审计之间的缺口。"""
    ledger.assert_runtime_audit()
    business_dispatch_count = len(ledger.dispatch_attempts)
    business_audit_count = sum(
        record["purpose"] == "business_outbox"
        for record in ledger.celery_dispatch_records
    )
    celery_dispatch_task_count = len(ledger.celery_dispatch_task_ids)
    runtime_gate_count = len(ledger.runtime_gate_evidence)
    assert business_audit_count == business_dispatch_count, (
        "业务预算与 Celery 派发审计数量不一致"
    )
    assert celery_dispatch_task_count == len(ledger.celery_dispatch_records)
    assert runtime_gate_count == celery_dispatch_task_count
    purpose_counts = {
        purpose: sum(record["purpose"] == purpose for record in ledger.celery_dispatch_records)
        for purpose in {
            "business_outbox",
            "remote_cancellation_recovery",
            "media_recovery",
        }
    }
    assert sum(purpose_counts.values()) == celery_dispatch_task_count, "存在未知派发 purpose"
    assert purpose_counts["business_outbox"] == sum(ledger.LIMITS.values()) == 11
    assert 1 <= media_recovery_runs <= 20
    assert media_recovery_stop_reason == "active_count_zero"
    assert purpose_counts["media_recovery"] == media_recovery_runs
    remote_recovery_max = remote_task_count * (MAX_REMOTE_DELETE_ATTEMPTS - 1)
    assert 0 <= purpose_counts["remote_cancellation_recovery"] <= remote_recovery_max
    return {
        "business_dispatch_count": business_dispatch_count,
        "celery_dispatch_task_count": celery_dispatch_task_count,
        "runtime_gate_count": runtime_gate_count,
        "recovery_dispatch_count": celery_dispatch_task_count - business_audit_count,
        "purpose_counts": purpose_counts,
        "recovery_bounds": {
            "media_recovery": {
                "exact": media_recovery_runs,
                "stop_reason": media_recovery_stop_reason,
                "max_iterations": 20,
            },
            "remote_cancellation_recovery": {
                "minimum": 0,
                "maximum": remote_recovery_max,
                "basis": "registered_remote_tasks*(max_delete_attempts-1)",
                "registered_remote_task_count": remote_task_count,
            },
        },
    }


def verify_remote_ledger_settlement(
    path: Path,
    *,
    script_id: str,
    expected_tasks: Mapping[str, RemoteVideoTask],
    settled_report: dict[str, Any],
) -> dict[str, Any]:
    """删除 ledger 前重读签名，并与内存 registry 和 settled report 逐项对账。"""
    verify_signed_payload(settled_report, label="remote settled report")
    assert settled_report.get("status") == "settled" and settled_report.get("settled") is True
    assert settled_report.get("run_id") == VALIDATION_RUN_ID
    ledger, ledger_identity = read_signed_json_with_identity(path, label="remote video ledger")
    assert ledger.get("status") == "open"
    assert ledger.get("run_id") == VALIDATION_RUN_ID
    assert ledger.get("script_id") == script_id
    ledger_tasks = ledger.get("tasks")
    report_tasks = settled_report.get("tasks")
    assert isinstance(ledger_tasks, dict) and isinstance(report_tasks, dict)
    expected_ids = set(expected_tasks)
    assert set(ledger_tasks) == set(report_tasks) == expected_ids, "远端 ledger 任务集合不一致"
    for job_id in sorted(expected_ids):
        expected = expected_tasks[job_id]
        assert expected.job_id == job_id
        expected_projection = remote_task_payload(expected)
        assert expected_projection["terminal"] is True
        assert _remote_settlement_projection(ledger_tasks[job_id]) == expected_projection
        assert _remote_settlement_projection(report_tasks[job_id]) == expected_projection
    return {
        "verified": True,
        "task_count": len(expected_ids),
        "ledger_sha256": ledger["sha256"],
        "ledger_file_identity": ledger_identity,
        "settled_report_sha256": settled_report["sha256"],
        "verified_at": datetime.now(UTC).isoformat(),
    }


_CELERY_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"


def validated_celery_result_id(value: str) -> str:
    assert re.fullmatch(_CELERY_UUID, value), f"Celery result ID 非标准 UUID: {value!r}"
    return value


def register_async_result_tree(
    result: Any,
    *,
    ledger: PaidBudgetLedger,
) -> set[str]:
    """仅从脚本实际获得的 AsyncResult/GroupResult 对象登记本轮结果 ID。"""
    stack = [result]
    seen: set[tuple[str, str]] = set()
    while stack:
        current = stack.pop()
        result_id = validated_celery_result_id(str(getattr(current, "id", "") or ""))
        group_children = getattr(current, "results", None)
        if group_children is not None:
            marker = ("group", result_id)
            ledger.register_group_result_id(result_id)
            children = list(group_children)
        else:
            marker = ("task", result_id)
            ledger.register_task_result_id(result_id)
            children = list(getattr(current, "children", None) or [])
        if marker in seen:
            continue
        seen.add(marker)
        stack.extend(children)
    return set(ledger.owned_backend_keys)


def _redis_dump_bytes(shape: Mapping[str, Any]) -> bytes:
    encoded = shape.get("dump_b64")
    digest = shape.get("dump_sha256")
    assert isinstance(encoded, str) and encoded
    assert isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest)
    try:
        raw = base64.b64decode(encoded.encode("ascii"), validate=True)
    except (UnicodeEncodeError, ValueError, binascii.Error):
        raise AssertionError("Redis DUMP 序列化指纹非法") from None
    assert raw and base64.b64encode(raw).decode("ascii") == encoded
    assert hashlib.sha256(raw).hexdigest() == digest, "Redis DUMP SHA256 不匹配"
    return raw


def _valid_redis_dump_shape(shape: Mapping[str, Any]) -> bool:
    try:
        _redis_dump_bytes(shape)
    except AssertionError:
        return False
    return True


def _owned_backend_key_shape(key: str, shape: dict[str, Any]) -> bool:
    if (
        shape.get("type") not in {"string", "zset"}
        or int(shape.get("size", -1)) < 0
        or re.fullmatch(r"[0-9a-f]{64}", str(shape.get("sha256") or "")) is None
        or not _valid_redis_dump_shape(shape)
    ):
        return False
    return re.fullmatch(
        rf"(?:celery-task-meta-{_CELERY_UUID}|"
        rf"(?:celery-taskset-meta|celery-group-meta)-{_CELERY_UUID}(?:\.[jts])?)",
        key,
    ) is not None


def _owned_broker_key_shape(key: str, shape: dict[str, Any]) -> bool:
    key_type = shape.get("type")
    size = int(shape.get("size", -1))
    assert re.fullmatch(r"[0-9a-f]{64}", str(shape.get("sha256") or "")), (
        f"broker Redis key 缺少完整内容指纹: {key}"
    )
    assert _valid_redis_dump_shape(shape), f"broker Redis key 缺少有效 DUMP 指纹: {key}"
    if key_type == "list":
        assert size == 0, f"broker list 非空，禁止清理: {key}"
        return key in {"celery"} or re.fullmatch(rf"{_CELERY_UUID}\.reply\.celery\.pidbox", key) is not None
    if key == "unacked":
        assert key_type == "hash" and size == 0, "broker unacked 非空"
        return True
    if key == "unacked_index":
        assert key_type == "zset" and size == 0, "broker unacked_index 非空"
        return True
    return key_type == "set" and re.fullmatch(
        rf"_kombu\.binding\.(?:celery|celery\.pidbox|reply\.celery\.pidbox|"
        rf"{_CELERY_UUID}\.reply\.celery\.pidbox)",
        key,
    ) is not None


def plan_redis_cleanup(
    baseline: dict[str, set[str]],
    first: dict[str, dict[str, dict[str, Any]]],
    second: dict[str, dict[str, dict[str, Any]]],
    *,
    owned_backend_keys: set[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    """在删除前冻结稳定快照并生成精确 key 集；未知状态不返回计划。"""
    assert set(baseline) == {"app", "broker", "backend"}
    assert all(not keys for keys in baseline.values()), "Redis 清理基线必须来自签名 HTTP 空实例"
    assert set(first) == set(second) == set(baseline)
    assert first == second, "Redis 两次稳定快照不一致，拒绝删除"
    assert first["app"] == {}, "应用 Redis 出现非本轮可清理 key"
    broker_keys: dict[str, dict[str, Any]] = {}
    for key, shape in first["broker"].items():
        assert _owned_broker_key_shape(key, shape), f"broker Redis 出现未知 key: {key}"
        broker_keys[key] = dict(shape)
    backend_keys = set(first["backend"])
    assert backend_keys <= owned_backend_keys, "result backend 出现未登记 AsyncResult/group key"
    assert all(_owned_backend_key_shape(key, first["backend"][key]) for key in backend_keys), (
        "result backend key 名称或类型非法"
    )
    return {
        "app": {},
        "broker": broker_keys,
        "backend": {key: dict(first["backend"][key]) for key in backend_keys},
    }


_REDIS_ATOMIC_DELETE_LUA = """
local expected_count = tonumber(ARGV[1])
local actual_count = redis.call('DBSIZE')
if expected_count == nil or #KEYS ~= expected_count or actual_count ~= expected_count then
    return {0, 1, actual_count}
end
for index, key in ipairs(KEYS) do
    local expected_type = ARGV[3 * index - 1]
    local expected_size = tonumber(ARGV[3 * index])
    local expected_dump = ARGV[3 * index + 1]
    local current_type = redis.call('TYPE', key)['ok']
    if current_type ~= expected_type then
        return {0, 2, index}
    end
    local size_commands = {
        string = 'STRLEN',
        list = 'LLEN',
        hash = 'HLEN',
        set = 'SCARD',
        zset = 'ZCARD',
        stream = 'XLEN'
    }
    local size_command = size_commands[current_type]
    if expected_size == nil or not size_command then
        return {0, 3, index}
    end
    local current_size = redis.call(size_command, key)
    if current_size ~= expected_size then
        return {0, 4, index}
    end
    local current_dump = redis.call('DUMP', key)
    if not current_dump or current_dump ~= expected_dump then
        return {0, 5, index}
    end
end
local deleted = 0
if #KEYS > 0 then
    deleted = redis.call('DEL', unpack(KEYS))
end
if deleted ~= #KEYS then
    return {0, 6, deleted}
end
return {1, deleted}
""".strip()


def _lua_delete_exact_redis_snapshot(
    client: Any,
    *,
    label: str,
    expected: dict[str, dict[str, Any]],
) -> None:
    """以完整计划为 Lua 输入，原子核对全库后只删除计划内 key。"""
    keys = sorted(expected)
    arguments: list[Any] = [str(len(keys))]
    for key in keys:
        shape = expected[key]
        key_type = shape.get("type")
        key_size = shape.get("size")
        assert isinstance(key_type, str) and key_type
        assert isinstance(key_size, int) and key_size >= 0
        arguments.extend((key_type, str(key_size), _redis_dump_bytes(shape)))
    result = client.eval(
        _REDIS_ATOMIC_DELETE_LUA,
        len(keys),
        *keys,
        *arguments,
    )
    assert result == [1, len(keys)], (
        f"{label} Redis Lua 全快照核对失败，拒绝任何删除: {result!r}"
    )


def parse_operation_set(values: Any, *, label: str) -> set[tuple[str, str]]:
    assert isinstance(values, list), f"{label} 不是列表"
    assert values == sorted(values), f"{label} 未稳定排序"
    assert len(values) == len(set(values)), f"{label} 包含重复项"
    result: set[tuple[str, str]] = set()
    for value in values:
        assert isinstance(value, str) and " " in value, f"{label} 项非法: {value!r}"
        method, path = value.split(" ", 1)
        assert method in {"GET", "POST", "PUT", "PATCH", "DELETE"}
        assert path.startswith("/")
        result.add((method, path))
    return result


def validate_http_isolation_evidence(payload: dict[str, Any]) -> dict[str, set[str]]:
    evidence = payload.get("isolation_evidence")
    assert isinstance(evidence, dict)
    assert evidence.get("baseline_empty") == {"app": True, "broker": True, "backend": True}
    result_started = datetime.fromisoformat(str(payload["started_at"]))
    result_finished = datetime.fromisoformat(str(payload["finished_at"]))
    previous_finished = result_started
    for key, expected_stage in (("http_start", "start"), ("http_end", "end")):
        phase = evidence.get(key)
        assert isinstance(phase, dict) and phase.get("stage") == expected_stage
        assert float(phase.get("stable_window_seconds", 0)) >= MIN_HTTP_STABLE_WINDOW_SECONDS
        phase_started = datetime.fromisoformat(str(phase["started_at"]))
        phase_finished = datetime.fromisoformat(str(phase["finished_at"]))
        assert phase_started.tzinfo is not None and phase_finished.tzinfo is not None
        assert previous_finished <= phase_started <= phase_finished <= result_finished
        assert (phase_finished - phase_started).total_seconds() >= MIN_HTTP_STABLE_WINDOW_SECONDS
        previous_finished = phase_finished
        for sample_name in ("first", "second"):
            sample = phase.get(sample_name)
            assert isinstance(sample, dict)
            process = sample.get("process")
            redis = sample.get("redis")
            assert isinstance(process, dict) and isinstance(redis, dict)
            assert process.get("expected_worker") is False
            assert process.get("worker_root_count") == 0
            assert process.get("worker_process_count") == 0
            assert process.get("beat_process_count") == 0
            assert process.get("celery_app") == EXPECTED_CELERY_APP
            assert process.get("processes") == []
            assert redis.get("redis_db_sizes") == {"app": 0, "broker": 0, "backend": 0}
    assert result_started <= previous_finished <= result_finished
    return {"app": set(), "broker": set(), "backend": set()}


def load_worker_authorization_fail_closed(
    loader: Callable[[], dict[str, Any]],
    cleanup: Callable[[], dict[str, Any]],
    reporter: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    """授权失败时保留原异常，并报告当前 nonce worker 的回收结果。"""
    try:
        return loader()
    except BaseException as original:
        try:
            cleanup_report = cleanup()
        except BaseException as cleanup_error:
            cleanup_report = {
                "completed": False,
                "residual": True,
                "error_type": type(cleanup_error).__name__,
                "reason": "worker_identity_or_cleanup_evidence_incomplete",
            }
            original.add_note(
                f"worker 授权失败回收未完成: {type(cleanup_error).__name__}"
            )
        else:
            if not isinstance(cleanup_report, dict):
                cleanup_report = {
                    "completed": False,
                    "residual": True,
                    "error_type": "InvalidCleanupReport",
                    "reason": "worker_cleanup_report_is_not_an_object",
                }
                original.add_note("worker 授权失败回收报告格式非法")
            elif cleanup_report.get("completed") is not True:
                original.add_note("worker 授权失败回收报告存在残留")
        try:
            reporter(cleanup_report)
        except BaseException as reporter_error:
            original.add_note(
                f"worker 授权失败回收报告输出失败: {type(reporter_error).__name__}"
            )
        raise


def load_verified_live_result(async_started_at: datetime) -> tuple[dict[str, Any], dict[str, Any]]:
    """验证 seed→HTTP 的签名和时间链，再允许异步阶段占用结果。"""
    for label, path in (("seed manifest", SEED_MANIFEST_PATH), ("HTTP result", LIVE_RESULT_PATH)):
        temporary = path.with_suffix(path.suffix + ".tmp")
        assert not temporary.exists() and not temporary.is_symlink(), f"{label} 有未完成临时文件"
    seed = read_signed_json(SEED_MANIFEST_PATH, label="seed manifest")
    payload = read_signed_json(LIVE_RESULT_PATH, label="HTTP result")
    validate_evidence_chain(seed, payload, async_started_at=async_started_at)
    assert payload.get("base_url") == BASE_URL
    validate_http_isolation_evidence(payload)
    http_finished = datetime.fromisoformat(str(payload["finished_at"]))
    assert (async_started_at - http_finished).total_seconds() <= MAX_HTTP_PREFLIGHT_AGE_SECONDS, (
        "HTTP 隔离预检已过期"
    )
    coverage = payload.get("operation_coverage")
    assert isinstance(coverage, dict)
    operations = parse_operation_set(coverage.get("operation_set"), label="HTTP operation_set")
    happy = parse_operation_set(coverage.get("happy_path_set"), label="HTTP happy_path_set")
    route_reached = parse_operation_set(coverage.get("route_reached_set"), label="HTTP route_reached_set")
    assert len(operations) == int(coverage.get("total", -1)) == EXPECTED_OPERATION_COUNT
    assert int(coverage.get("path_total", -1)) == EXPECTED_PATH_COUNT
    assert int(coverage.get("operation_id_total", -1)) == EXPECTED_OPERATION_COUNT
    assert route_reached == operations
    assert happy <= operations
    return seed, payload


def evaluate_cleanup_gate(
    *,
    remote_terminal: bool,
    active_tasks: dict[str, list[str]],
    active_outbox: list[dict[str, Any]],
    media_active: int,
    celery_snapshot: dict[str, Any],
    broker_snapshot: dict[str, Any],
    storage: dict[str, Any],
    script_http_status: int,
    require_script_missing: bool,
    require_storage_empty: bool,
    require_media_quiet: bool,
    process_snapshot: dict[str, Any] | None = None,
    redis_clients: dict[str, Any] | None = None,
    require_worker_absent: bool = False,
    registry_integrity: bool = True,
    redis_clean: bool = True,
) -> dict[str, bool]:
    """纯判定函数，任何不可观测状态或缺项都按 residual 处理。"""
    return {
        "remote_terminal": remote_terminal,
        "no_active_tasks": all(not values for values in active_tasks.values()),
        "no_active_outbox": not active_outbox,
        "no_active_media_jobs": not require_media_quiet or media_active == 0,
        "celery_quiet": (
            celery_snapshot.get("available") is True
            and (
                celery_snapshot.get("worker_absent") is True
                if require_worker_absent
                else celery_snapshot.get("identity_valid") is True
            )
            and celery_snapshot.get("total") == 0
        ),
        "broker_quiet": (
            broker_snapshot.get("available") is True
            and broker_snapshot.get("queued") == 0
            and broker_snapshot.get("unacked") == 0
            and broker_snapshot.get("unacked_index") == 0
        ),
        "process_quiet": (
            isinstance(process_snapshot, dict)
            and process_snapshot.get("beat_process_count") == 0
            and (
                process_snapshot.get("worker_root_count") == 0
                if require_worker_absent
                else process_snapshot.get("worker_root_count") == 1
            )
        ),
        "redis_clients_owned": (
            isinstance(redis_clients, dict)
            and (
                redis_clients.get("external_client_count") == 0
                if require_worker_absent
                else redis_clients.get("probe_only") is False
            )
        ),
        "script_state": (script_http_status == 404 if require_script_missing else script_http_status == 200),
        "storage_safe": storage.get("issues") == [],
        "storage_empty": (
            not require_storage_empty
            or (storage.get("clean") is True and storage.get("files") == [] and storage.get("issues") == [])
        ),
        "registry_integrity": registry_integrity,
        "redis_clean": redis_clean,
    }


def failure_cleanup_plan(script_http_status: int) -> tuple[str, ...]:
    common_tail = ("media_recovery", "final_gate")
    if script_http_status == 200:
        return ("discover_jobs", "cancel_jobs", "remote_cleanup", "predelete", "delete_if_safe", *common_tail)
    if script_http_status == 404:
        return ("remote_cleanup", *common_tail)
    return common_tail


def run_bootstrap_offline_self_tests() -> None:
    shanghai_boundary = datetime(2026, 7, 18, 16, 30, tzinfo=UTC)
    valid_run_id = "20260719-123e4567-e89b-42d3-a456-426614174000"
    assert require_validation_run_id(valid_run_id, now=shanghai_boundary) == valid_run_id
    for wrong_day in (
        "20260718-123e4567-e89b-42d3-a456-426614174000",
        "20260720-123e4567-e89b-42d3-a456-426614174000",
    ):
        try:
            require_validation_run_id(wrong_day, now=shanghai_boundary)
        except AssertionError:
            continue
        raise AssertionError("nonce 非上海当日日期未失败关闭")
    try:
        require_validation_run_id(
            "20260230-123e4567-e89b-42d3-a456-426614174000",
            now=shanghai_boundary,
        )
    except AssertionError:
        pass
    else:
        raise AssertionError("nonce 日期门禁未失败关闭")
    assert not is_handler_evidence(404, {"detail": "Not Found"})
    assert not is_handler_evidence(401, {"detail": "未登录"})
    assert is_handler_evidence(404, {"detail": "剧本不存在"})

    valid_pg = "postgresql+psycopg2://mtv_validation:x@127.0.0.1:55438/mtv_validation"
    assert_database_url("DATABASE_SYNC_URL", valid_pg)
    for invalid_pg in (valid_pg + "?host=other", valid_pg.replace("55438", "5432")):
        try:
            assert_database_url("DATABASE_SYNC_URL", invalid_pg)
        except AssertionError:
            continue
        raise AssertionError("PG guard 未拒绝错误目标")
    assert_redis_urls(
        "redis://127.0.0.1:56380/0",
        "redis://127.0.0.1:56380/1",
        "redis://127.0.0.1:56380/2",
    )
    try:
        parse_redis_target("Redis", "redis://user:x@127.0.0.1:56380/0", 0)
    except AssertionError:
        pass
    else:
        raise AssertionError("Redis 凭据 guard 未失败关闭")

    unsupported = RemoteVideoTask(
        "job", "volcengine", "remote", "succeeded", outcome="unsupported", retryable=False
    )
    assert unsupported.terminal is False
    unsupported.outcome = "deleted"
    assert unsupported.terminal is True
    assert persisted_remote_outcome(
        {"outcome": "canceled", "detail": "任意包含不存在字样的非权威详情"}
    ) == "canceled"
    assert direct_remote_cleanup_state("canceled") == ("deleted", False)
    assert direct_remote_cleanup_state("unsupported") == ("unsupported", False)
    exhausted = RemoteVideoTask(
        "job-exhausted",
        "volcengine",
        "remote-exhausted",
        "canceled",
        outcome="failed",
        retryable=True,
        attempts=MAX_REMOTE_DELETE_ATTEMPTS,
    )
    assert exhausted.terminal is False
    exhausted.outcome = "authority_conflict"
    exhausted.retryable = False
    assert exhausted.terminal is False

    process_table = parse_ps_processes(
        "1 0 /sbin/launchd\n"
        "100 1 /opt/mtv/.venv/bin/celery -A app.workers.celery_app worker -l INFO\n"
        "101 100 [celeryd: worker@host:ForkPoolWorker-1]\n"
    )
    assert evaluate_celery_processes(process_table, expect_worker=True)["worker_root_pids"] == [100]
    for unsafe_command in (
        "/opt/mtv/.venv/bin/celery -A app.workers.celery_app beat -l INFO",
        "/opt/mtv/.venv/bin/celery -A app.workers.celery_app worker -B",
        "/opt/mtv/.venv/bin/celery -A other.app worker",
        "/opt/mtv/.venv/bin/celery -A app.workers.celery_app status worker",
        "/opt/mtv/.venv/bin/celery -A app.workers.celery_app worker --app=other.app",
        "/opt/mtv/.venv/bin/celerybeat -A app.workers.celery_app",
        "/opt/mtv/.venv/bin/celeryd -A other.app",
        "/opt/mtv/.venv/bin/python /opt/mtv/.venv/bin/celerybeat -A app.workers.celery_app",
        "/bin/sh -c 'celery -A app.workers.celery_app beat -l INFO'",
        "[celeryd: worker@host:ForkPoolWorker-1]",
    ):
        try:
            evaluate_celery_processes(
                [{"pid": 200, "ppid": 1, "command": unsafe_command}],
                expect_worker=False,
            )
        except AssertionError:
            continue
        raise AssertionError(f"Celery 进程门禁未拒绝: {unsafe_command}")

    empty_baseline = {"app": set(), "broker": set(), "backend": set()}
    empty_snapshot: dict[str, dict[str, dict[str, Any]]] = {
        "app": {},
        "broker": {},
        "backend": {},
    }

    def redis_test_shape(key_type: str, size: int, marker: str) -> dict[str, Any]:
        serialized = f"dump:{key_type}:{marker}".encode("ascii")
        return {
            "type": key_type,
            "size": size,
            "sha256": canonical_sha256({"type": key_type, "value": marker}),
            "dump_b64": base64.b64encode(serialized).decode("ascii"),
            "dump_sha256": hashlib.sha256(serialized).hexdigest(),
        }

    active_queue_snapshot = {
        "app": {},
        "broker": {"celery": redis_test_shape("list", 1, "active")},
        "backend": {},
    }
    for first, second in (
        (active_queue_snapshot, active_queue_snapshot),
        (empty_snapshot, active_queue_snapshot),
    ):
        try:
            plan_redis_cleanup(
                empty_baseline,
                first,
                second,
                owned_backend_keys=set(),
            )
        except AssertionError:
            continue
        raise AssertionError("Redis 活动队列/TOCTOU 未在删除计划前失败")
    task_id = "123e4567-e89b-42d3-a456-426614174000"
    backend_key = f"celery-task-meta-{task_id}"
    owned_snapshot = {
        "app": {},
        "broker": {
            "_kombu.binding.celery": redis_test_shape("set", 1, "binding")
        },
        "backend": {
            backend_key: redis_test_shape("string", 120, "owned-result")
        },
    }
    try:
        plan_redis_cleanup(
            empty_baseline,
            owned_snapshot,
            owned_snapshot,
            owned_backend_keys=set(),
        )
    except AssertionError:
        pass
    else:
        raise AssertionError("未登记 AsyncResult key 被错误归属")
    assert plan_redis_cleanup(
        empty_baseline,
        owned_snapshot,
        owned_snapshot,
        owned_backend_keys={backend_key},
    ) == {
        "app": {},
        "broker": {"_kombu.binding.celery": owned_snapshot["broker"]["_kombu.binding.celery"]},
        "backend": {backend_key: owned_snapshot["backend"][backend_key]},
    }

    class FakeLuaRedis:
        def __init__(self, mutation: str) -> None:
            owned_dump = _redis_dump_bytes(owned_snapshot["backend"][backend_key])
            self.values: dict[str, tuple[str, int, bytes]] = {
                backend_key: ("string", 120, owned_dump)
            }
            self.mutation = mutation
            self.deleted_key_count = 0
            self.eval_call_count = 0

        def eval(self, script: str, key_count: int, *values: Any) -> list[int]:
            self.eval_call_count += 1
            assert script == _REDIS_ATOMIC_DELETE_LUA
            keys = [str(value) for value in values[:key_count]]
            arguments = list(values[key_count:])
            expected_count = int(arguments[0])
            if self.mutation == "insert":
                self.values["unknown"] = ("string", 7, b"unknown")
            elif self.mutation == "delete":
                self.values.pop(backend_key, None)
            elif self.mutation == "same_count_replace_key":
                self.values.pop(backend_key, None)
                self.values["unknown"] = ("string", 7, b"unknown")
            elif self.mutation == "type_replace":
                self.values[backend_key] = ("hash", 120, self.values[backend_key][2])
            elif self.mutation == "size_replace":
                self.values[backend_key] = ("string", 121, self.values[backend_key][2])
            elif self.mutation == "value_replace":
                self.values[backend_key] = ("string", 120, b"same-length-x")
            if key_count != expected_count or len(self.values) != expected_count:
                return [0, 1, len(self.values)]
            for index, key in enumerate(keys, start=1):
                current = self.values.get(key)
                expected_type = str(arguments[3 * index - 2])
                expected_size = int(arguments[3 * index - 1])
                expected_dump = bytes(arguments[3 * index])
                if current is None or current[0] != expected_type:
                    return [0, 2, index]
                if current[1] != expected_size:
                    return [0, 4, index]
                if current[2] != expected_dump:
                    return [0, 5, index]
            for key in keys:
                del self.values[key]
                self.deleted_key_count += 1
            return [1, self.deleted_key_count]

    assert "redis.call('DBSIZE')" in _REDIS_ATOMIC_DELETE_LUA
    assert "redis.call('TYPE', key)" in _REDIS_ATOMIC_DELETE_LUA
    assert "redis.call(size_command, key)" in _REDIS_ATOMIC_DELETE_LUA
    assert "redis.call('DUMP', key)" in _REDIS_ATOMIC_DELETE_LUA
    assert "redis.call('DEL', unpack(KEYS))" in _REDIS_ATOMIC_DELETE_LUA
    assert "redis.call('KEYS'" not in _REDIS_ATOMIC_DELETE_LUA
    assert "redis.call('SCAN'" not in _REDIS_ATOMIC_DELETE_LUA
    for mutation in (
        "insert",
        "delete",
        "same_count_replace_key",
        "type_replace",
        "size_replace",
        "value_replace",
    ):
        mutated = FakeLuaRedis(mutation)
        try:
            _lua_delete_exact_redis_snapshot(
                mutated,
                label="backend",
                expected=owned_snapshot["backend"],
            )
        except AssertionError:
            pass
        else:
            raise AssertionError(f"Redis Lua 未拒绝快照并发变化: {mutation}")
        assert mutated.eval_call_count == 1
        assert mutated.deleted_key_count == 0

    inserted_into_empty = FakeLuaRedis("insert")
    inserted_into_empty.values.clear()
    try:
        _lua_delete_exact_redis_snapshot(
            inserted_into_empty,
            label="app",
            expected={},
        )
    except AssertionError:
        pass
    else:
        raise AssertionError("Redis Lua 空计划未拒绝未知 key 插入")
    assert inserted_into_empty.eval_call_count == 1
    assert inserted_into_empty.deleted_key_count == 0

    unchanged = FakeLuaRedis("none")
    _lua_delete_exact_redis_snapshot(
        unchanged,
        label="backend",
        expected=owned_snapshot["backend"],
    )
    assert unchanged.eval_call_count == 1
    assert unchanged.deleted_key_count == 1
    assert unchanged.values == {}

    source_tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    redis_delete_function_names = {
        "_redis_snapshot_evidence",
        "_classify_redis_target_after_delete",
        "_delete_exact_redis_keys",
    }
    redis_delete_module = ast.Module(
        body=[
            ast.ImportFrom(
                module="__future__",
                names=[ast.alias(name="annotations")],
                level=0,
            ),
            *[
                node
                for node in source_tree.body
                if isinstance(node, ast.FunctionDef)
                and node.name in redis_delete_function_names
            ],
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(redis_delete_module)
    redis_delete_namespace: dict[str, Any] = {"canonical_sha256": canonical_sha256}
    exec(compile(redis_delete_module, __file__, "exec"), redis_delete_namespace)
    isolated_delete_exact_redis_keys = redis_delete_namespace[
        "_delete_exact_redis_keys"
    ]

    redis_partial_state = {
        label: {key: dict(shape) for key, shape in shapes.items()}
        for label, shapes in owned_snapshot.items()
    }

    def injected_cross_db_eval(
        label: str,
        raw_url: str,
        expected: dict[str, dict[str, Any]],
    ) -> None:
        assert raw_url == f"redis://self-test/{label}"
        assert expected == owned_snapshot[label]
        if label == "broker":
            redis_partial_state[label] = {}
        elif label == "backend":
            redis_partial_state[label] = {}
            raise ConnectionError("injected response disconnect after accept")

    def injected_cross_db_snapshot() -> dict[str, dict[str, dict[str, Any]]]:
        return {
            label: {key: dict(shape) for key, shape in shapes.items()}
            for label, shapes in redis_partial_state.items()
        }

    partial_report = isolated_delete_exact_redis_keys(
        owned_snapshot,
        evaluator=injected_cross_db_eval,
        snapshotter=injected_cross_db_snapshot,
        raw_urls={
            label: f"redis://self-test/{label}"
            for label in ("app", "broker", "backend")
        },
    )
    assert partial_report["completed"] is False
    assert partial_report["cross_db_atomic"] is False
    assert partial_report["interrupted_at"] == "backend"
    assert {
        label: evidence["state"]
        for label, evidence in partial_report["targets"].items()
    } == {"app": "unchanged", "broker": "deleted", "backend": "deleted"}
    assert "dump_b64" not in json.dumps(partial_report, ensure_ascii=False)

    wrapper_nodes = [
        node
        for node in source_tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "send_task_with_runtime_gate"
    ]
    assert len(wrapper_nodes) == 1
    wrapper_node = wrapper_nodes[0]
    send_task_calls = [
        node
        for node in ast.walk(source_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "send_task"
    ]
    assert send_task_calls == [
        node
        for node in ast.walk(wrapper_node)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "send_task"
    ], "send_task 只能由统一门禁包装器调用"

    main_nodes = [
        node
        for node in source_tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "main"
    ]
    assert len(main_nodes) == 1
    main_node = main_nodes[0]
    ledger_delete_lines = [
        node.lineno
        for node in ast.walk(main_node)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "delete_verified_signed_file"
    ]
    passed_status_lines = [
        node.lineno
        for node in ast.walk(main_node)
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Constant)
        and node.value.value == "passed"
        and any(
            isinstance(target, ast.Subscript)
            and isinstance(target.slice, ast.Constant)
            and target.slice.value == "status"
            for target in node.targets
        )
    ]
    finalizing_status_lines = [
        value.lineno
        for node in ast.walk(main_node)
        if isinstance(node, ast.Dict)
        for key, value in zip(node.keys, node.values, strict=True)
        if isinstance(key, ast.Constant)
        and key.value == "status"
        and isinstance(value, ast.Constant)
        and value.value == "finalizing"
    ]
    assert len(ledger_delete_lines) == len(passed_status_lines) == len(finalizing_status_lines) == 1
    assert finalizing_status_lines[0] < ledger_delete_lines[0] < passed_status_lines[0]

    record_calls = [
        node
        for node in ast.walk(wrapper_node)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "record_dispatch"
    ]
    sending_persistence = [
        node
        for node in ast.walk(wrapper_node)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "persist_dispatch_delivery_state"
        and any(
            keyword.arg == "state"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "sending"
            for keyword in node.keywords
        )
    ]
    assert len(record_calls) == len(sending_persistence) == len(send_task_calls) == 1
    assert record_calls[0].lineno < sending_persistence[0].lineno < send_task_calls[0].lineno

    class InjectedGateFailure(RuntimeError):
        pass

    class FakeRegistry:
        worker_name = f"{WORKER_NAME_PREFIX}-self-test"
        worker_root_pid = 101

    class FakeResult:
        def __init__(self, task_id: str) -> None:
            self.id = task_id

    class FakeBudget:
        def __init__(self) -> None:
            self.dispatch_attempts: list[frozenset[tuple[str, str, int]]] = []
            self.celery_dispatch_records: list[dict[str, Any]] = []
            self.runtime_gate_evidence: list[dict[str, Any]] = []
            self.async_result_roots: list[Any] = []
            self.dispatch_delivery_states: dict[str, str] = {}

        def record_dispatch(
            self,
            attempts: frozenset[tuple[str, str, int]],
        ) -> None:
            dispatch_events.append("budget")
            self.dispatch_attempts.append(attempts)

        def begin_dispatch_delivery(self, task_id: str) -> None:
            dispatch_events.append("begin")
            self.dispatch_delivery_states[task_id] = "sending"

        def mark_dispatch_delivery(self, task_id: str, state: str) -> None:
            dispatch_events.append(f"mark_{state}")
            self.dispatch_delivery_states[task_id] = state

        def record_celery_dispatch(self, task_id: str, **evidence: Any) -> None:
            dispatch_events.append("audit")
            if injected_failure == "audit":
                raise RuntimeError("injected audit failure")
            self.celery_dispatch_records.append({"task_id": task_id, **evidence})
            self.runtime_gate_evidence.append(dict(evidence["runtime_gate"]))

    class FakeCelery:
        def __init__(self) -> None:
            self.send_count = 0

        def send_task(
            self,
            task_name: str,
            *,
            kwargs: dict[str, Any],
            task_id: str,
        ) -> FakeResult:
            del task_name, kwargs
            dispatch_events.append("send")
            self.send_count += 1
            if injected_failure == "send":
                raise RuntimeError("injected send response failure")
            if injected_failure == "invalid_result_id":
                return FakeResult("not-a-celery-id")
            return FakeResult(task_id)

    wrapper_module = ast.Module(
        body=[
            ast.ImportFrom(
                module="__future__",
                names=[ast.alias(name="annotations")],
                level=0,
            ),
            wrapper_node,
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(wrapper_module)
    dispatch_events: list[str] = []
    persisted_states: list[str] = []
    injected_failure: str | None = None

    def known_backend_guard() -> set[str]:
        dispatch_events.append("known_backend")
        return set()

    def reject_runtime_gate(**_: Any) -> dict[str, Any]:
        dispatch_events.append("gate")
        raise InjectedGateFailure("injected fresh gate failure")

    def fake_persist_dispatch_delivery_state(**record: Any) -> dict[str, Any]:
        state = str(record["state"])
        dispatch_events.append(f"persist_{state}")
        if injected_failure == "sending_persist" and state == "sending":
            raise OSError("injected sending persistence failure")
        if injected_failure == "confirmation_persist" and state == "confirmed":
            raise OSError("injected confirmation persistence failure")
        persisted_states.append(state)
        return dict(record)

    def fake_register_result_tree(result: Any, *, ledger: Any) -> None:
        del result, ledger
        dispatch_events.append("result_tree")
        if injected_failure == "result_tree":
            raise RuntimeError("injected result-tree failure")

    fake_budget = FakeBudget()
    fake_celery = FakeCelery()

    wrapper_namespace: dict[str, Any] = {
        "PAID_BUDGET": fake_budget,
        "WORKER_NAME_PREFIX": WORKER_NAME_PREFIX,
        "assert_known_backend_keys": known_backend_guard,
        "assert_isolated_worker_before_first_dispatch": reject_runtime_gate,
        "celery_app": fake_celery,
        "uuid": uuid,
        "validated_celery_result_id": validated_celery_result_id,
        "persist_dispatch_delivery_state": fake_persist_dispatch_delivery_state,
        "register_async_result_tree": fake_register_result_tree,
    }
    exec(compile(wrapper_module, __file__, "exec"), wrapper_namespace)
    isolated_wrapper = wrapper_namespace["send_task_with_runtime_gate"]
    immutable_attempts = frozenset({("pipeline.dag", "job-gate", 1)})
    try:
        isolated_wrapper(
            FakeRegistry(),
            "outbox.dispatch",
            purpose="business_outbox",
            dispatch_attempts=immutable_attempts,
        )
    except InjectedGateFailure:
        pass
    else:
        raise AssertionError("新鲜 worker 门禁失败未阻断派发")
    assert dispatch_events == ["known_backend", "gate"]
    assert fake_budget.dispatch_attempts == []
    assert fake_budget.celery_dispatch_records == []
    assert fake_budget.runtime_gate_evidence == []
    assert fake_celery.send_count == 0

    wrapper_namespace["assert_isolated_worker_before_first_dispatch"] = lambda **_: (
        dispatch_events.append("gate")
        or {
            "worker_count": 1,
            "worker_name": FakeRegistry.worker_name,
            "worker_root_pid": FakeRegistry.worker_root_pid,
            "process": {"beat_process_count": 0},
        }
    )

    def reset_dispatch_fakes(failure: str | None) -> tuple[FakeBudget, FakeCelery]:
        nonlocal fake_budget, fake_celery, injected_failure
        dispatch_events.clear()
        persisted_states.clear()
        injected_failure = failure
        fake_budget = FakeBudget()
        fake_celery = FakeCelery()
        wrapper_namespace["PAID_BUDGET"] = fake_budget
        wrapper_namespace["celery_app"] = fake_celery
        return fake_budget, fake_celery

    success_budget, success_celery = reset_dispatch_fakes(None)
    isolated_wrapper(
        FakeRegistry(),
        "outbox.dispatch",
        purpose="business_outbox",
        dispatch_attempts=immutable_attempts,
    )
    assert dispatch_events == [
        "known_backend",
        "gate",
        "budget",
        "begin",
        "persist_sending",
        "send",
        "persist_confirmed",
        "mark_confirmed",
        "audit",
        "result_tree",
    ]
    assert persisted_states == ["sending", "confirmed"]
    assert success_budget.dispatch_attempts == [immutable_attempts]
    assert success_celery.send_count == 1

    failure_types = {
        "sending_persist": OSError,
        "send": RuntimeError,
        "invalid_result_id": AssertionError,
        "confirmation_persist": OSError,
        "audit": RuntimeError,
        "result_tree": RuntimeError,
    }
    for failure, expected_error in failure_types.items():
        failed_budget, failed_celery = reset_dispatch_fakes(failure)
        try:
            isolated_wrapper(
                FakeRegistry(),
                "outbox.dispatch",
                purpose="business_outbox",
                dispatch_attempts=immutable_attempts,
            )
        except expected_error:
            pass
        else:
            raise AssertionError(f"派发故障注入未保留原异常: {failure}")
        if failure == "sending_persist":
            assert failed_celery.send_count == 0
            assert list(failed_budget.dispatch_delivery_states.values()) == ["sending"]
            assert persisted_states == []
        else:
            assert failed_celery.send_count == 1
            assert list(failed_budget.dispatch_delivery_states.values()) == ["uncertain"]
            assert persisted_states[-1] == "uncertain"

    budget = PaidBudgetLedger(
        worker_prerequisites_verified=True,
        workflow_prerequisites_verified=True,
        text_prerequisites_verified=True,
        media_prerequisites_verified=True,
    )
    sequence = {
        "pipeline.dag": 2,
        "storyboard.generate": 1,
        "generation.generate_images": 2,
        "video.generate": 2,
        "timeline.render": 4,
    }
    dispatch_index = 0
    for task_type, count in sequence.items():
        for index in range(count):
            dispatch_index += 1
            task_id = f"00000000-0000-4000-8000-{dispatch_index:012x}"
            budget.begin_dispatch_delivery(task_id)
            budget.mark_dispatch_delivery(task_id, "confirmed")
            budget.record_celery_dispatch(
                task_id,
                task_name="outbox.dispatch",
                purpose="business_outbox",
                runtime_gate={
                    "worker_count": 1,
                    "worker_name": "self-test-worker",
                    "worker_root_pid": 100,
                    "process": {"beat_process_count": 0},
                },
            )
            budget.record_dispatch(frozenset({(task_type, f"{task_type}-{index}", 1)}))
    budget.assert_business_final()
    try:
        budget.record_dispatch(frozenset({("unknown.billable", "job", 1)}))
    except AssertionError:
        pass
    else:
        raise AssertionError("未知付费任务未被拒绝")

    recovery_gate = {
        "worker_count": 1,
        "worker_name": "self-test-worker",
        "worker_root_pid": 100,
        "process": {"beat_process_count": 0},
    }
    for suffix in (90, 91):
        task_id = f"00000000-0000-4000-8000-{suffix:012x}"
        budget.begin_dispatch_delivery(task_id)
        budget.mark_dispatch_delivery(task_id, "confirmed")
        budget.record_celery_dispatch(
            task_id,
            task_name="app.workers.media_storage.recover_media_storage_jobs",
            purpose="media_recovery",
            runtime_gate=recovery_gate,
        )
    stale_dispatch_summary = dispatch_integrity_summary(
        budget,
        media_recovery_runs=2,
        media_recovery_stop_reason="active_count_zero",
        remote_task_count=0,
    )
    final_recovery_task_id = "00000000-0000-4000-8000-000000000092"
    budget.begin_dispatch_delivery(final_recovery_task_id)
    budget.mark_dispatch_delivery(final_recovery_task_id, "confirmed")
    budget.record_celery_dispatch(
        final_recovery_task_id,
        task_name="app.workers.media_storage.recover_media_storage_jobs",
        purpose="media_recovery",
        runtime_gate=recovery_gate,
    )
    final_dispatch_summary = dispatch_integrity_summary(
        budget,
        media_recovery_runs=3,
        media_recovery_stop_reason="active_count_zero",
        remote_task_count=0,
    )
    assert budget.assert_final(
        media_recovery_runs=3,
        media_recovery_stop_reason="active_count_zero",
        remote_task_count=0,
    ) == final_dispatch_summary
    assert stale_dispatch_summary["recovery_dispatch_count"] == 2
    assert final_dispatch_summary["business_dispatch_count"] == 11
    assert final_dispatch_summary["celery_dispatch_task_count"] == 14
    assert final_dispatch_summary["runtime_gate_count"] == 14
    assert final_dispatch_summary["recovery_dispatch_count"] == 3
    assert final_dispatch_summary["purpose_counts"] == {
        "business_outbox": 11,
        "remote_cancellation_recovery": 0,
        "media_recovery": 3,
    }
    try:
        dispatch_integrity_summary(
            budget,
            media_recovery_runs=2,
            media_recovery_stop_reason="active_count_zero",
            remote_task_count=0,
        )
    except AssertionError:
        pass
    else:
        raise AssertionError("media recovery 摘要未拒绝少算的 loop 轮数")

    class InjectedAuthorizationError(RuntimeError):
        pass

    cleanup_calls: list[str] = []
    cleanup_reports: list[dict[str, Any]] = []

    def reject_authorization() -> dict[str, Any]:
        raise InjectedAuthorizationError("injected authorization failure")

    def recover_authorization_worker() -> dict[str, Any]:
        cleanup_calls.append("recover")
        return {"completed": True, "worker_absent": True}

    try:
        load_worker_authorization_fail_closed(
            reject_authorization,
            recover_authorization_worker,
            cleanup_reports.append,
        )
    except InjectedAuthorizationError:
        pass
    else:
        raise AssertionError("worker 授权失败未保留原异常")
    assert cleanup_calls == ["recover"]
    assert cleanup_reports == [{"completed": True, "worker_absent": True}]

    cleanup_reports.clear()

    def fail_authorization_recovery() -> dict[str, Any]:
        raise AssertionError("injected cleanup failure")

    try:
        load_worker_authorization_fail_closed(
            reject_authorization,
            fail_authorization_recovery,
            cleanup_reports.append,
        )
    except InjectedAuthorizationError:
        pass
    else:
        raise AssertionError("worker 回收失败掩盖了原授权异常")
    assert cleanup_reports == [
        {
            "completed": False,
            "residual": True,
            "error_type": "AssertionError",
            "reason": "worker_identity_or_cleanup_evidence_incomplete",
        }
    ]

    armed_registry = CleanupRegistry(
        script_id="script",
        episode_id="episode",
        material_id="material",
        storage_keys=set(),
    )
    armed_evidence = arm_cleanup_after_worker_authorization(
        armed_registry,
        signed_payload(
            {
                "run_id": VALIDATION_RUN_ID,
                "required_worker": {
                    "app": EXPECTED_CELERY_APP,
                    "worker_name_prefix": WORKER_NAME_PREFIX,
                    "beat_allowed": False,
                },
            },
        ),
    )
    assert armed_evidence["armed"] is True and armed_registry.cleanup_armed is True
    assert armed_registry.worker_name is None and armed_registry.worker_root_pid is None
    empty_redis_baseline = {"app": set(), "broker": set(), "backend": set()}
    early_cleanup = recover_armed_preflight_failure(
        armed_registry,
        empty_redis_baseline,
        lambda registry, baseline: {
            "completed": registry.cleanup_armed and baseline == empty_redis_baseline,
            "worker_absent": True,
            "worker_name": f"{WORKER_NAME_PREFIX}@self-test",
            "worker_root_pid": 101,
            "destination_shutdown_sent": True,
        },
    )
    assert early_cleanup["worker_authorization_recovery"]["completed"] is True
    assert early_cleanup["local_records_retained"] is True

    def reject_early_worker_evidence(
        registry: CleanupRegistry,
        baseline: dict[str, set[str]],
    ) -> dict[str, Any]:
        del registry, baseline
        raise AssertionError("injected insufficient nonce worker evidence")

    insufficient_cleanup = recover_armed_preflight_failure(
        armed_registry,
        empty_redis_baseline,
        reject_early_worker_evidence,
    )
    assert insufficient_cleanup["status"] == "residual"
    assert insufficient_cleanup["worker_authorization_recovery"] == {
        "completed": False,
        "error_type": "AssertionError",
        "evidence_basis": "nonce+inspect_destination+root_pid_required",
    }

    base_gate = {
        "remote_terminal": True,
        "active_tasks": {"video": []},
        "active_outbox": [],
        "media_active": 0,
        "celery_snapshot": {"available": True, "worker_absent": True, "total": 0},
        "broker_snapshot": {
            "available": True,
            "queued": 0,
            "unacked": 0,
            "unacked_index": 0,
        },
        "process_snapshot": {"worker_root_count": 0, "beat_process_count": 0},
        "redis_clients": {"external_client_count": 0, "probe_only": True},
        "storage": {"clean": True, "files": [], "issues": []},
        "script_http_status": 404,
        "require_script_missing": True,
        "require_storage_empty": True,
        "require_media_quiet": True,
        "require_worker_absent": True,
    }
    assert all(evaluate_cleanup_gate(**base_gate).values())
    assert not all(evaluate_cleanup_gate(**base_gate, registry_integrity=False).values())
    assert not all(evaluate_cleanup_gate(**base_gate, redis_clean=False).values())
    assert failure_cleanup_plan(404) == ("remote_cleanup", "media_recovery", "final_gate")
    assert failure_cleanup_plan(200)[-2:] == ("media_recovery", "final_gate")

    now = datetime.now(UTC)
    seed = signed_payload(
        {
            "status": "passed",
            "run_id": VALIDATION_RUN_ID,
            "seed_ids": SEED_IDS,
            "started_at": now.isoformat(),
            "finished_at": now.isoformat(),
        }
    )
    http_result = signed_payload(
        {
            "status": "passed",
            "run_id": VALIDATION_RUN_ID,
            "seed_manifest_sha256": seed["sha256"],
            "started_at": now.isoformat(),
            "finished_at": now.isoformat(),
        }
    )
    validate_evidence_chain(seed, http_result, async_started_at=now)
    tampered = dict(http_result)
    tampered["seed_manifest_sha256"] = "0" * 64
    try:
        validate_evidence_chain(seed, tampered, async_started_at=now)
    except AssertionError:
        pass
    else:
        raise AssertionError("结果链篡改未被拒绝")

    empty_process = {
        "expected_worker": False,
        "worker_root_count": 0,
        "worker_process_count": 0,
        "beat_process_count": 0,
        "celery_app": EXPECTED_CELERY_APP,
        "processes": [],
    }
    empty_redis = {"redis_db_sizes": {"app": 0, "broker": 0, "backend": 0}}
    http_start = now
    http_middle = now + timedelta(seconds=MIN_HTTP_STABLE_WINDOW_SECONDS + 0.25)
    http_end = http_middle + timedelta(seconds=MIN_HTTP_STABLE_WINDOW_SECONDS + 0.25)
    isolation_payload = {
        "started_at": http_start.isoformat(),
        "finished_at": http_end.isoformat(),
        "isolation_evidence": {
            "baseline_empty": {"app": True, "broker": True, "backend": True},
            "http_start": {
                "stage": "start",
                "started_at": http_start.isoformat(),
                "finished_at": http_middle.isoformat(),
                "stable_window_seconds": MIN_HTTP_STABLE_WINDOW_SECONDS + 0.25,
                "first": {"process": empty_process, "redis": empty_redis},
                "second": {"process": empty_process, "redis": empty_redis},
            },
            "http_end": {
                "stage": "end",
                "started_at": http_middle.isoformat(),
                "finished_at": http_end.isoformat(),
                "stable_window_seconds": MIN_HTTP_STABLE_WINDOW_SECONDS + 0.25,
                "first": {"process": empty_process, "redis": empty_redis},
                "second": {"process": empty_process, "redis": empty_redis},
            },
        },
    }
    assert validate_http_isolation_evidence(isolation_payload) == {
        "app": set(),
        "broker": set(),
        "backend": set(),
    }
    beat_tamper = json.loads(json.dumps(isolation_payload))
    beat_tamper["isolation_evidence"]["http_end"]["second"]["process"][
        "beat_process_count"
    ] = 1
    try:
        validate_http_isolation_evidence(beat_tamper)
    except AssertionError:
        pass
    else:
        raise AssertionError("HTTP 签名证据中的 beat 未被拒绝")

    settled_task = RemoteVideoTask(
        "job-ledger",
        "volcengine",
        "remote-ledger",
        "missing",
        outcome="deleted",
        retryable=False,
        attempts=1,
        detail="Provider 已确认删除",
    )
    settled_report_body = {
        "status": "settled",
        "run_id": VALIDATION_RUN_ID,
        "settled": True,
        "tasks": {settled_task.job_id: remote_task_payload(settled_task)},
    }
    settled_report = signed_payload(settled_report_body)
    with tempfile.TemporaryDirectory(prefix="mtv-ledger-self-test-") as temporary:
        ledger_path = Path(temporary) / "ledger.json"
        ledger_body = {
            "status": "open",
            "run_id": VALIDATION_RUN_ID,
            "script_id": "script-ledger",
            "tasks": {settled_task.job_id: remote_task_payload(settled_task)},
        }
        replace_observations: list[tuple[Path, Path, bool, bool]] = []
        original_replace = os.replace

        def record_atomic_replace(source: Any, destination: Any) -> None:
            source_path = Path(source)
            destination_path = Path(destination)
            replace_observations.append(
                (
                    source_path,
                    destination_path,
                    source_path.is_file(),
                    destination_path.exists() or destination_path.is_symlink(),
                )
            )
            original_replace(source, destination)

        with mock.patch.object(os, "replace", side_effect=record_atomic_replace):
            ledger = atomic_write_signed(ledger_path, ledger_body)
        assert replace_observations == [
            (
                ledger_path.with_suffix(".json.tmp"),
                ledger_path,
                True,
                False,
            )
        ], "ledger 首写必须经同目录临时文件和 os.replace"
        assert not ledger_path.with_suffix(".json.tmp").exists()
        verification = verify_remote_ledger_settlement(
            ledger_path,
            script_id="script-ledger",
            expected_tasks={settled_task.job_id: settled_task},
            settled_report=settled_report,
        )
        assert verification["ledger_sha256"] == ledger["sha256"]

        mismatched_report_values = {
            "local_status": "succeeded",
            "retryable": True,
            "detail": "被篡改的详情",
            "updated_at": (
                datetime.fromisoformat(settled_task.updated_at) + timedelta(seconds=1)
            ).isoformat(),
        }
        for field_name, mismatched_value in mismatched_report_values.items():
            mismatched_body = json.loads(json.dumps(settled_report_body))
            mismatched_body["tasks"][settled_task.job_id][field_name] = mismatched_value
            try:
                verify_remote_ledger_settlement(
                    ledger_path,
                    script_id="script-ledger",
                    expected_tasks={settled_task.job_id: settled_task},
                    settled_report=signed_payload(mismatched_body),
                )
            except AssertionError:
                continue
            raise AssertionError(f"settled report 字段不一致未被拒绝: {field_name}")

        tampered = dict(ledger)
        tampered["tasks"] = {key: dict(value) for key, value in ledger["tasks"].items()}
        tampered["tasks"][settled_task.job_id]["attempts"] = 2
        ledger_path.write_text(json.dumps(tampered), encoding="utf-8")
        try:
            verify_remote_ledger_settlement(
                ledger_path,
                script_id="script-ledger",
                expected_tasks={settled_task.job_id: settled_task},
                settled_report=settled_report,
            )
        except AssertionError:
            pass
        else:
            raise AssertionError("篡改 ledger 未被拒绝")
        assert ledger_path.exists(), "ledger 核验失败后不应删除证据"
        ledger_path.unlink()
        atomic_write_signed(
            ledger_path,
            {
                "status": "open",
                "run_id": VALIDATION_RUN_ID,
                "script_id": "script-ledger",
                "tasks": {},
            },
        )
        try:
            verify_remote_ledger_settlement(
                ledger_path,
                script_id="script-ledger",
                expected_tasks={settled_task.job_id: settled_task},
                settled_report=settled_report,
            )
        except AssertionError:
            pass
        else:
            raise AssertionError("ledger 任务集合不一致未被拒绝")
        assert ledger_path.exists(), "ledger 集合核验失败后不应删除证据"

    with tempfile.TemporaryDirectory(prefix="mtv-ledger-identity-self-test-") as temporary:
        ledger_path = Path(temporary) / "ledger.json"
        ledger = atomic_write_signed(ledger_path, ledger_body)
        _, original_identity = read_signed_json_with_identity(
            ledger_path,
            label="identity self-test ledger",
        )
        replacement_path = Path(temporary) / "replacement.json"
        _write_bytes_exclusive(replacement_path, ledger_path.read_bytes())
        os.replace(replacement_path, ledger_path)
        replacement, replacement_identity = read_signed_json_with_identity(
            ledger_path,
            label="replacement identity self-test ledger",
        )
        assert replacement["sha256"] == ledger["sha256"]
        assert replacement_identity != original_identity, "故障注入未更换 ledger inode"
        try:
            delete_verified_signed_file(
                ledger_path,
                label="identity self-test ledger",
                expected_sha256=ledger["sha256"],
                expected_identity=original_identity,
            )
        except AssertionError:
            pass
        else:
            raise AssertionError("相同 SHA、不同 inode 的 ledger 被错误删除")
        assert ledger_path.exists(), "身份核验失败后替换文件必须保留"
        delete_evidence = delete_verified_signed_file(
            ledger_path,
            label="replacement identity self-test ledger",
            expected_sha256=ledger["sha256"],
            expected_identity=replacement_identity,
        )
        assert delete_evidence == {
            "deleted": True,
            "sha256": ledger["sha256"],
            "file_identity": replacement_identity,
            "fd_held_through_unlink": True,
            "quarantine_dirfd_held_through_unlink": True,
            "quarantine_directory_unlinked_verified": True,
            "same_user_directory_adversary": "fail_closed_on_detected_identity_change",
            "pre_unlink_link_count": 1,
            "post_unlink_link_count": 0,
        }
        assert not ledger_path.exists() and not ledger_path.is_symlink()

        def deletion_fixture(name: str) -> tuple[Path, dict[str, Any], dict[str, int]]:
            fixture_root = Path(temporary) / name
            fixture_root.mkdir()
            fixture_path = fixture_root / "ledger.json"
            fixture_ledger = atomic_write_signed(fixture_path, ledger_body)
            _, fixture_identity = read_signed_json_with_identity(
                fixture_path,
                label=f"{name} ledger",
            )
            return fixture_path, fixture_ledger, fixture_identity

        rename_path, rename_ledger, rename_identity = deletion_fixture("rename-failure")
        with mock.patch.object(os, "rename", side_effect=OSError("injected rename failure")):
            try:
                delete_verified_signed_file(
                    rename_path,
                    label="rename failure ledger",
                    expected_sha256=rename_ledger["sha256"],
                    expected_identity=rename_identity,
                )
            except OSError:
                pass
            else:
                raise AssertionError("rename 失败被错误报告为删除成功")
        assert _path_holds_identity(rename_path, rename_identity)

        link_path, link_ledger, link_identity = deletion_fixture("hard-link-failure")
        hard_link_path = link_path.with_name("ledger-hard-link.json")
        os.link(link_path, hard_link_path)
        try:
            delete_verified_signed_file(
                link_path,
                label="hard link ledger",
                expected_sha256=link_ledger["sha256"],
                expected_identity=link_identity,
            )
        except AssertionError:
            pass
        else:
            raise AssertionError("额外硬链接未阻断删除")
        assert link_path.exists() and hard_link_path.exists()

        fsync_path, fsync_ledger, fsync_identity = deletion_fixture("fsync-failure")
        original_fsync_directory = _fsync_directory
        fsync_call_count = 0

        def fail_first_directory_fsync(directory: Path) -> None:
            nonlocal fsync_call_count
            fsync_call_count += 1
            if fsync_call_count == 1:
                raise OSError("injected fsync failure")
            original_fsync_directory(directory)

        with mock.patch(
            f"{__name__}._fsync_directory",
            side_effect=fail_first_directory_fsync,
        ):
            try:
                delete_verified_signed_file(
                    fsync_path,
                    label="fsync failure ledger",
                    expected_sha256=fsync_ledger["sha256"],
                    expected_identity=fsync_identity,
                )
            except OSError:
                pass
            else:
                raise AssertionError("fsync 失败被错误报告为删除成功")
        assert _path_holds_identity(fsync_path, fsync_identity)

        unlink_path, unlink_ledger, unlink_identity = deletion_fixture("unlink-failure")
        with mock.patch.object(os, "unlink", side_effect=OSError("injected unlink failure")):
            try:
                delete_verified_signed_file(
                    unlink_path,
                    label="unlink failure ledger",
                    expected_sha256=unlink_ledger["sha256"],
                    expected_identity=unlink_identity,
                )
            except OSError:
                pass
            else:
                raise AssertionError("unlink 失败被错误报告为删除成功")
        assert _path_holds_identity(unlink_path, unlink_identity)

        durability_path, durability_ledger, durability_identity = deletion_fixture(
            "post-unlink-fsync-failure"
        )
        durability_bytes = durability_path.read_bytes()
        durability_artifacts = _verified_delete_artifact_paths(durability_path)
        durability_fsync_count = 0

        def fail_post_unlink_directory_fsync(directory: Path) -> None:
            nonlocal durability_fsync_count
            durability_fsync_count += 1
            if durability_fsync_count == 3:
                raise OSError("injected post-unlink fsync failure")
            original_fsync_directory(directory)

        with mock.patch(
            f"{__name__}._fsync_directory",
            side_effect=fail_post_unlink_directory_fsync,
        ):
            try:
                delete_verified_signed_file(
                    durability_path,
                    label="post-unlink fsync failure ledger",
                    expected_sha256=durability_ledger["sha256"],
                    expected_identity=durability_identity,
                )
            except OSError:
                pass
            else:
                raise AssertionError("unlink 后 fsync 失败被错误报告为删除成功")
        assert durability_fsync_count >= 4, "失败后必须 fsync 固化 evidence 目录项"
        assert durability_artifacts["evidence"].read_bytes() == durability_bytes

        directory_path, directory_ledger, directory_identity = deletion_fixture(
            "directory-failure"
        )
        directory_artifacts = _verified_delete_artifact_paths(directory_path)
        directory_artifacts["quarantine_dir"].mkdir()
        try:
            delete_verified_signed_file(
                directory_path,
                label="directory failure ledger",
                expected_sha256=directory_ledger["sha256"],
                expected_identity=directory_identity,
            )
        except AssertionError:
            pass
        else:
            raise AssertionError("quarantine 目录异常未阻断删除")
        assert _path_holds_identity(directory_path, directory_identity)

        swap_path, swap_ledger, swap_identity = deletion_fixture("rename-swap-failure")
        swap_artifacts = _verified_delete_artifact_paths(swap_path)
        original_signed_bytes = swap_path.read_bytes()
        injected_replacement = swap_path.with_name("injected-replacement.json")
        _write_bytes_exclusive(injected_replacement, original_signed_bytes)
        original_rename = os.rename
        swap_injected = False

        def replace_inode_during_rename(
            source: Any,
            destination: Any,
            **rename_options: Any,
        ) -> None:
            nonlocal swap_injected
            source_path = Path(source)
            destination_path = Path(destination)
            if (
                not swap_injected
                and source_path == Path(swap_path.name)
                and destination_path == Path(swap_path.name)
                and rename_options.get("src_dir_fd") is not None
                and rename_options.get("dst_dir_fd") is not None
            ):
                swap_injected = True
                os.replace(injected_replacement, swap_path)
            original_rename(source, destination, **rename_options)

        with mock.patch.object(os, "rename", side_effect=replace_inode_during_rename):
            try:
                delete_verified_signed_file(
                    swap_path,
                    label="rename inode swap ledger",
                    expected_sha256=swap_ledger["sha256"],
                    expected_identity=swap_identity,
                )
            except AssertionError:
                pass
            else:
                raise AssertionError("rename 期间 inode 替换被错误报告为删除成功")
        assert swap_injected
        assert swap_artifacts["quarantine_file"].exists(), "替换 inode 不得被恢复为原证据"
        assert swap_artifacts["evidence"].read_bytes() == original_signed_bytes

        dir_swap_path, dir_swap_ledger, dir_swap_identity = deletion_fixture(
            "quarantine-dir-swap-failure"
        )
        dir_swap_artifacts = _verified_delete_artifact_paths(dir_swap_path)
        dir_swap_bytes = dir_swap_path.read_bytes()
        displaced_quarantine = dir_swap_path.parent / "displaced-quarantine"
        original_unlink = os.unlink
        directory_swap_injected = False

        def replace_quarantine_directory_before_unlink(
            target: Any,
            *,
            dir_fd: int | None = None,
        ) -> None:
            nonlocal directory_swap_injected
            if not directory_swap_injected and dir_fd is not None:
                directory_swap_injected = True
                os.rename(dir_swap_artifacts["quarantine_dir"], displaced_quarantine)
                dir_swap_artifacts["quarantine_dir"].mkdir(mode=0o700)
            original_unlink(target, dir_fd=dir_fd)

        with mock.patch.object(
            os,
            "unlink",
            side_effect=replace_quarantine_directory_before_unlink,
        ):
            try:
                delete_verified_signed_file(
                    dir_swap_path,
                    label="quarantine directory inode swap ledger",
                    expected_sha256=dir_swap_ledger["sha256"],
                    expected_identity=dir_swap_identity,
                )
            except AssertionError:
                pass
            else:
                raise AssertionError("quarantine 目录 inode 替换被错误报告为成功")
        assert directory_swap_injected
        assert dir_swap_artifacts["evidence"].read_bytes() == dir_swap_bytes

    with tempfile.TemporaryDirectory(prefix="mtv-async-self-test-") as temporary:
        path = run_artifact_paths(VALIDATION_RUN_ID, root=Path(temporary))["async_result"]
        write_signed_exclusive(path, {"status": "running"})
        try:
            write_signed_exclusive(path, {"status": "running"})
        except FileExistsError:
            pass
        else:
            raise AssertionError("旧 nonce async result 未被拒绝")
        first_write_failure = Path(temporary) / "first-write-fsync-failure.json"
        with mock.patch(
            f"{__name__}._fsync_directory",
            side_effect=OSError("injected async claim directory fsync failure"),
        ):
            try:
                write_signed_exclusive(first_write_failure, {"status": "running"})
            except OSError:
                pass
            else:
                raise AssertionError("async claim 目录 fsync 失败被错误报告成功")
        assert first_write_failure.exists()

        replace_path = Path(temporary) / "replace-fsync-failure.json"
        write_signed_exclusive(
            replace_path,
            {
                "status": "finalizing",
                "settled": True,
                "remote_ledger_delete_state": "verified_deleted",
            },
        )
        original_fsync_directory = _fsync_directory
        replace_fsync_count = 0

        def fail_replace_directory_fsync(directory: Path) -> None:
            nonlocal replace_fsync_count
            replace_fsync_count += 1
            if replace_fsync_count == 2:
                raise OSError("injected post-replace directory fsync failure")
            original_fsync_directory(directory)

        with mock.patch(
            f"{__name__}._fsync_directory",
            side_effect=fail_replace_directory_fsync,
        ):
            try:
                replace_signed_claim(
                    replace_path,
                    {
                        "status": "passed",
                        "settled": True,
                        "remote_ledger_delete_state": "verified_deleted",
                    },
                )
            except OSError:
                pass
            else:
                raise AssertionError("replace 后目录 fsync 失败被错误报告成功")
        failed_replace_evidence = read_signed_json(
            replace_path,
            label="replace fsync evidence",
        )
        assert failed_replace_evidence["status"] == "passed"
        assert failed_replace_evidence["remote_ledger_delete_state"] == "verified_deleted"
    print("async bootstrap offline self-tests passed")


if SELF_TEST_MODE:
    run_bootstrap_offline_self_tests()
    raise SystemExit(0)


from app.core.config import settings  # noqa: E402
from app.models.credit_reservation import CreditReservation  # noqa: E402
from app.models.generation_job import GenerationJob  # noqa: E402
from app.models.media_storage_job import MediaStorageJob  # noqa: E402
from app.models.pipeline_job import PipelineJob  # noqa: E402
from app.models.storyboard_job import StoryboardJob  # noqa: E402
from app.models.task_dispatch_outbox import TaskDispatchOutbox  # noqa: E402
from app.models.timeline_render_job import TimelineRenderJob  # noqa: E402
from app.models.video_generation_job import (  # noqa: E402
    VideoGenerationIdempotencyRequest,
    VideoGenerationJob,
)
from app.providers.registry import require_capability  # noqa: E402
from app.providers.storage import validate_key_prefix  # noqa: E402
from app.providers.video_gen import get_video_provider  # noqa: E402
from app.services.storyboard import CONSTRAINT_LOCK  # noqa: E402
from app.workers.celery_app import celery_app  # noqa: E402

assert_database_url("DATABASE_URL", settings.database_url)
assert_database_url("DATABASE_SYNC_URL", settings.database_sync_url)
assert_redis_urls(settings.redis_url, settings.celery_broker_url, settings.celery_result_backend)
SYNC_ENGINE = create_engine(settings.database_sync_url, pool_pre_ping=True)
PAID_BUDGET = PaidBudgetLedger()


def inspect_isolated_storage(
    root: Path,
    *,
    resolve_path: Callable[[Path], Path] | None = None,
    relative_path: Callable[[Path, Path], Path] | None = None,
) -> dict[str, Any]:
    """以失败关闭语义盘点隔离存储；解析异常、逃逸或 symlink 都是残留。"""
    resolver = resolve_path or (lambda value: value.resolve(strict=True))
    relativizer = relative_path or (lambda value, parent: value.relative_to(parent))
    issues: list[str] = []
    files: list[str] = []
    if root.is_symlink():
        issues.append("root_is_symlink")
        return {"clean": False, "files": files, "issues": issues}
    if not root.is_dir():
        issues.append("root_not_directory")
        return {"clean": False, "files": files, "issues": issues}
    try:
        resolved_root = resolver(root)
    except (OSError, RuntimeError, ValueError):
        issues.append("root_resolve_failed")
        return {"clean": False, "files": files, "issues": issues}
    try:
        entries = list(root.rglob("*"))
    except OSError:
        issues.append("walk_failed")
        return {"clean": False, "files": files, "issues": issues}
    for entry in entries:
        if entry.is_symlink():
            issues.append(f"symlink:{entry.name}")
            continue
        if entry.is_dir():
            continue
        if not entry.is_file():
            issues.append(f"non_regular:{entry.name}")
            continue
        try:
            resolved = resolver(entry)
        except (OSError, RuntimeError, ValueError):
            issues.append(f"resolve_failed:{entry.name}")
            continue
        try:
            relative = relativizer(resolved, resolved_root)
        except (OSError, RuntimeError, ValueError):
            issues.append(f"relative_to_failed:{entry.name}")
            continue
        key = relative.as_posix()
        try:
            validate_key_prefix(key)
        except ValueError:
            issues.append(f"prefix_rejected:{entry.name}")
            continue
        files.append(key)
    return {"clean": not issues and not files, "files": sorted(files), "issues": sorted(issues)}


def assert_storage_guard_fault_injections() -> list[str]:
    """证明盘点器遇到路径 API 故障或 symlink 时会失败关闭。"""
    verified: list[str] = []
    with tempfile.TemporaryDirectory(prefix="mtv-storage-guard-") as temporary:
        base = Path(temporary)
        root = base / "root"
        media = root / "upload_images" / "material" / "probe.png"
        media.parent.mkdir(parents=True)
        media.write_bytes(b"probe")

        def resolve_failure(value: Path) -> Path:
            if value.name == "probe.png":
                raise OSError("injected resolve failure")
            return value.resolve(strict=True)

        resolve_report = inspect_isolated_storage(root, resolve_path=resolve_failure)
        assert "resolve_failed:probe.png" in resolve_report["issues"]
        verified.append("resolve_failure")

        def relative_failure(value: Path, parent: Path) -> Path:
            del value, parent
            raise ValueError("injected relative_to failure")

        relative_report = inspect_isolated_storage(root, relative_path=relative_failure)
        assert "relative_to_failed:probe.png" in relative_report["issues"]
        verified.append("relative_to_failure")

        outside = base / "outside.png"
        outside.write_bytes(b"outside")
        link = media.with_name("link.png")
        link.symlink_to(outside)
        symlink_report = inspect_isolated_storage(root)
        assert "symlink:link.png" in symlink_report["issues"]
        verified.append("symlink_rejected")

        root_link = base / "root-link"
        root_link.symlink_to(root, target_is_directory=True)
        root_symlink_report = inspect_isolated_storage(root_link)
        assert root_symlink_report == {
            "clean": False,
            "files": [],
            "issues": ["root_is_symlink"],
        }
        verified.append("root_symlink_rejected")

    for invalid in ("../escape.png", "upload_images/../../escape.png", "/etc/passwd"):
        try:
            validate_key_prefix(invalid)
        except ValueError:
            continue
        raise AssertionError(f"前缀防护未拒绝: {invalid}")
    verified.append("prefix_rejected")
    return verified


def assert_real_flow_preconditions(
    *,
    live_result: dict[str, Any],
    context: dict[str, Any],
) -> tuple[list[str], dict[str, set[str]]]:
    """在首次付费任务派发前验证真实流程依赖，不输出任何密钥或连接串。"""
    assert live_result.get("status") == "passed", "HTTP 验收结果不是 passed"
    assert live_result.get("run_id") == VALIDATION_RUN_ID
    assert not REMOTE_LEDGER_PATH.exists(), "存在上一轮未清理的远端视频债务账本"
    assert not REMOTE_LEDGER_PATH.with_suffix(".json.tmp").exists(), "存在未完成写入的远端视频债务账本"
    assert settings.storage_driver == "local", "真实验收要求使用隔离的本地存储"
    storage_root = Path(settings.storage_root)
    assert storage_root == EXPECTED_STORAGE_ROOT, "本地存储不是本轮隔离目录"
    assert storage_root.is_dir() and not storage_root.is_symlink(), "隔离存储根目录非法"
    storage_report = inspect_isolated_storage(storage_root)
    assert storage_report["issues"] == [], storage_report
    existing_files = set(storage_report["files"])
    material_id = str(context["material_id"])
    context_keys = {validate_key_prefix(str(key)) for key in context["storage_keys"]}
    assert all(key.startswith(f"upload_images/{material_id}/") for key in context_keys)
    assert existing_files == context_keys, (
        f"隔离存储与 HTTP 证据不一致: existing={sorted(existing_files)} expected={sorted(context_keys)}"
    )
    guard_evidence = run_offline_guard_self_tests()
    assert shutil.which("ffmpeg") is not None, "找不到 ffmpeg"
    assert shutil.which("ffprobe") is not None, "找不到 ffprobe"

    async_database = assert_database_url("DATABASE_URL", settings.database_url)
    sync_database = assert_database_url("DATABASE_SYNC_URL", settings.database_sync_url)
    assert async_database | {"driver": sync_database["driver"]} == sync_database
    assert SYNC_ENGINE.dialect.name == "postgresql", "真实验收要求 PostgreSQL"
    try:
        with SYNC_ENGINE.connect() as connection:
            assert connection.scalar(text("SELECT 1")) == 1
            current_database = connection.scalar(text("SELECT current_database()"))
            current_user = connection.scalar(text("SELECT current_user"))
            users = connection.execute(text("SELECT user_id, org_id FROM users ORDER BY user_id")).all()
            organizations = set(connection.execute(text("SELECT id FROM organizations")).scalars().all())
            memberships = connection.execute(
                text("SELECT user_id, org_id FROM user_organizations ORDER BY user_id, org_id")
            ).all()
            script = connection.execute(
                text("SELECT scope, owner_user_id, org_id FROM scripts WHERE id = :script_id"),
                {"script_id": context["script_id"]},
            ).one_or_none()
            episode = connection.execute(
                text(
                    "SELECT script_id, episode_version, client_save_seq FROM episodes "
                    "WHERE episode_id = :episode_id"
                ),
                {"episode_id": context["episode_id"]},
            ).one_or_none()
            material_script_id = connection.scalar(
                text("SELECT script_id FROM materials WHERE id = :material_id"),
                {"material_id": context["material_id"]},
            )
            subject_script_id = connection.scalar(
                text("SELECT script_id FROM subjects WHERE id = :subject_id"),
                {"subject_id": context["subject_id"]},
            )
            asset_owner = connection.execute(
                text("SELECT script_id, subject_id FROM asset_entries WHERE id = :asset_id"),
                {"asset_id": context["image_asset_id"]},
            ).one_or_none()
    except AssertionError:
        raise
    except Exception:
        raise AssertionError("PostgreSQL SELECT 1 预检失败") from None
    assert current_database == EXPECTED_DATABASE, "数据库不是本轮 mtv_validation 隔离库"
    assert current_user == "mtv_validation", "数据库登录用户不是本轮隔离用户"
    assert dict(tuple(row) for row in users) == EXPECTED_USERS, "固定 seed 用户或组织归属不匹配"
    assert organizations == EXPECTED_ORGANIZATIONS, "固定 seed 组织不匹配"
    assert {tuple(row) for row in memberships} == set(EXPECTED_USERS.items()), "固定 seed 组织成员关系不匹配"
    assert tuple(script or ()) == (
        "team",
        OWNER_ID,
        SEED_IDS["organizations"]["primary"],
    ), "live result 剧本归属不匹配"
    assert tuple(episode or ()) == (
        context["script_id"],
        context["episode_version"],
        context["client_save_seq"],
    ), "live result 分集版本或归属不匹配"
    assert material_script_id == context["script_id"], "live result Materials 归属不匹配"
    assert subject_script_id == context["script_id"], "live result 主体归属不匹配"
    assert tuple(asset_owner or ()) == (
        context["script_id"],
        context["subject_id"],
    ), "live result 图片资产归属不匹配"

    assert_redis_urls(
        settings.redis_url,
        settings.celery_broker_url,
        settings.celery_result_backend,
    )
    redis_targets = (
        ("app", "应用缓存", settings.redis_url, 0),
        ("broker", "Celery broker", settings.celery_broker_url, 1),
        ("backend", "Celery result backend", settings.celery_result_backend, 2),
    )
    redis_baseline = validate_http_isolation_evidence(live_result)
    redis_current: dict[str, set[str]] = {}
    for target_name, label, url, expected_db in redis_targets:
        redis_client: Redis | None = None
        try:
            redis_client = Redis.from_url(url, decode_responses=True)
            actual_db = int(redis_client.connection_pool.connection_kwargs.get("db", 0))
            assert actual_db == expected_db, f"{label} 必须使用 Redis DB {expected_db}"
            assert redis_client.ping() is True, f"{label} Redis PING 失败"
            redis_current[target_name] = {str(key) for key in redis_client.scan_iter()}
            if expected_db == 0:
                assert redis_client.dbsize() == 0, "应用缓存包含旧验收数据"
            elif expected_db == 1:
                queued_lists = [
                    key
                    for key in redis_client.scan_iter()
                    if redis_client.type(key) == "list" and redis_client.llen(key) > 0
                ]
                assert queued_lists == [], "Celery broker 存在旧排队任务"
                assert redis_client.hlen("unacked") == 0, "Celery broker 存在未确认任务"
                assert redis_client.zcard("unacked_index") == 0, "Celery broker 存在未确认索引"
            else:
                old_results = list(redis_client.scan_iter(match="celery-task-meta-*"))
                assert old_results == [], "Celery result backend 存在旧任务结果"
        except AssertionError:
            raise
        except Exception:
            raise AssertionError(f"{label} Redis 预检失败") from None
        finally:
            if redis_client is not None:
                redis_client.close()

    text_provider = require_capability(settings.text_llm_provider, "text")
    assert settings.text_llm_api_key.strip(), "未配置文本 LLM API Key"
    assert settings.text_llm_model.strip(), "未配置文本 LLM 模型"

    configured_image_provider = require_capability(settings.image_gen_provider, "image")
    image_provider = (
        configured_image_provider
        if settings.is_explicitly_configured("image_gen_provider")
        else require_capability("bailian", "image")
    )
    assert image_provider == "bailian", "固定 qwen-image 模型要求使用百炼图片 Provider"
    assert settings.image_gen_api_key.strip(), "未配置图片生成 API Key"
    assert settings.image_gen_model.strip(), "未配置图片生成模型"
    if not settings.is_explicitly_configured("image_gen_provider"):
        assert settings.dashscope_api_key.strip() or settings.is_explicitly_configured("image_gen_api_key"), (
            "qwen-image 路由缺少百炼可用的 API Key"
        )

    video_provider = require_capability(settings.video_gen_provider, "video")
    assert video_provider == "volcengine", "运行中远端取消验收要求火山视频 Provider"
    assert settings.video_gen_api_key.strip(), "未配置视频生成 API Key"
    assert settings.video_gen_model.strip(), "未配置视频生成模型"
    assert settings.video_provider_selection_mode == "env", "视频 Provider 必须使用 env 模式"

    print(
        "preflight passed: storage=local database=postgresql redis=0/1/2 "
        f"text={text_provider} image={image_provider} video={video_provider}",
        flush=True,
    )
    return guard_evidence, redis_baseline


def operation_template(method: str, actual_path: str) -> str | None:
    candidates = sorted(
        (path for candidate_method, path in OPENAPI_OPERATIONS if candidate_method == method),
        key=lambda path: (path.count("{"), -len(path)),
    )
    for template in candidates:
        cursor = 0
        pieces: list[str] = ["^"]
        for match in re.finditer(r"\{([^}]+)\}", template):
            pieces.append(re.escape(template[cursor : match.start()]))
            pieces.append(r".+" if match.group(1) == "storage_key" else r"[^/]+")
            cursor = match.end()
        pieces.extend((re.escape(template[cursor:]), "$"))
        if re.fullmatch("".join(pieces), actual_path):
            return template
    return None


def json_object(value: Any, *, label: str) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    assert isinstance(value, dict), f"{label} 不是 JSON 对象: {type(value).__name__}"
    return value


def active_outbox_rows(job_id: str) -> list[tuple[str, str, int]]:
    with SYNC_ENGINE.connect() as connection:
        rows = connection.execute(
            select(
                TaskDispatchOutbox.task_type,
                TaskDispatchOutbox.status,
                TaskDispatchOutbox.attempt,
            ).where(
                TaskDispatchOutbox.job_id == job_id,
                TaskDispatchOutbox.status.in_(("pending", "retry", "running")),
            )
        ).all()
    return [(str(task_type), str(status), int(attempt)) for task_type, status, attempt in rows]


def active_outbox_count() -> int:
    with SYNC_ENGINE.connect() as connection:
        return int(
            connection.scalar(
                select(func.count())
                .select_from(TaskDispatchOutbox)
                .where(TaskDispatchOutbox.status.in_(("pending", "retry", "running")))
            )
            or 0
        )


def render_job_snapshot(job_id: str) -> Any:
    with SYNC_ENGINE.connect() as connection:
        row = connection.execute(
            select(
                TimelineRenderJob.input_json,
                TimelineRenderJob.output_json,
                TimelineRenderJob.attempt,
                TimelineRenderJob.status,
            ).where(TimelineRenderJob.job_id == job_id)
        ).one()
    return row


def media_storage_active_count() -> int:
    with SYNC_ENGINE.connect() as connection:
        return int(
            connection.scalar(
                select(func.count())
                .select_from(MediaStorageJob)
                .where(MediaStorageJob.status.in_(("pending", "retry", "running")))
            )
            or 0
        )


def video_job_snapshot(job_id: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    with SYNC_ENGINE.connect() as connection:
        row = connection.execute(
            select(VideoGenerationJob.input_json, VideoGenerationJob.output_json).where(
                VideoGenerationJob.job_id == job_id
            )
        ).one()
    return dict(row.input_json or {}), dict(row.output_json or {}) if row.output_json else None


def call(
    client: httpx.Client,
    method: str,
    path: str,
    expected: int | set[int] = 200,
    **kwargs: Any,
) -> Any:
    response = client.request(method, path, **kwargs)
    expected_set = {expected} if isinstance(expected, int) else expected
    try:
        body = response.json()
    except ValueError:
        body = None
    assert response.status_code in expected_set, (
        f"{method} {path}: expected={sorted(expected_set)} actual={response.status_code} "
        f"body_type={type(body).__name__}"
    )
    assert response.status_code < 500
    if response.status_code >= 400:
        assert is_application_error_contract(response.status_code, body)
    EVENTS.append(
        {
            "method": method,
            "path": path,
            "template": operation_template(method, response.request.url.path),
            "status": response.status_code,
            "handler_evidence": is_handler_evidence(response.status_code, body),
        }
    )
    return body


def load_openapi(client: httpx.Client) -> None:
    global OPENAPI_OPERATIONS
    response = client.get("/v3api/openapi.json")
    response.raise_for_status()
    spec = response.json()
    assert isinstance(spec, dict) and len(spec.get("paths", {})) == EXPECTED_PATH_COUNT
    operations = [
        (method.upper(), path, operation)
        for path, methods in spec["paths"].items()
        for method, operation in methods.items()
        if method.upper() in {"GET", "POST", "PUT", "PATCH", "DELETE"}
    ]
    operation_ids = [operation.get("operationId") for _, _, operation in operations]
    assert all(isinstance(value, str) and value for value in operation_ids)
    assert len(operations) == EXPECTED_OPERATION_COUNT
    assert len(set(operation_ids)) == EXPECTED_OPERATION_COUNT, "OpenAPI operationId 必须存在且唯一"
    OPENAPI_OPERATIONS = {(method, path) for method, path, _ in operations}
    assert len(OPENAPI_OPERATIONS) == EXPECTED_OPERATION_COUNT


def active_outbox_snapshot() -> list[dict[str, Any]]:
    with SYNC_ENGINE.connect() as connection:
        rows = connection.execute(
            select(
                TaskDispatchOutbox.task_type,
                TaskDispatchOutbox.job_id,
                TaskDispatchOutbox.status,
                TaskDispatchOutbox.payload,
                TaskDispatchOutbox.attempt,
                TaskDispatchOutbox.publish_attempts,
            )
            .where(TaskDispatchOutbox.status.in_(("pending", "retry", "running")))
            .order_by(TaskDispatchOutbox.created_at.asc())
        ).all()
    return [
        {
            "task_type": str(row.task_type),
            "job_id": str(row.job_id),
            "status": str(row.status),
            "payload": dict(row.payload or {}),
            "attempt": int(row.attempt),
            "publish_attempts": int(row.publish_attempts),
        }
        for row in rows
    ]


def initial_pipeline_expectation(script_id: str, episode_id: str) -> tuple[str, str]:
    rows = active_outbox_snapshot()
    assert len(rows) == 1, f"初始流水线之外仍有活动 outbox: {rows}"
    row = rows[0]
    assert row["task_type"] == "pipeline.dag" and row["status"] in {"pending", "retry"}
    assert row["publish_attempts"] == 0, "初始流水线已在预算登记前发布"
    assert row["payload"].get("script_id") == script_id
    assert row["payload"].get("episode_id") == episode_id
    return row["task_type"], row["job_id"]


def dispatch_expected(
    script_id: str,
    expectations: set[tuple[str, str]],
    *,
    registry: CleanupRegistry,
) -> int:
    """只在活动 outbox 精确等于本步骤登记任务时调用生产 dispatcher。"""
    assert expectations, "dispatch_expected 至少需要一个预期任务"
    assert registry.worker_name is not None and registry.worker_root_pid is not None
    rows = active_outbox_snapshot()
    actual = {(row["task_type"], row["job_id"]) for row in rows}
    assert actual == expectations, f"拒绝派发未知活动 outbox: actual={actual} expected={expectations}"
    assert len(rows) == len(expectations), "同一业务任务出现多个活动 outbox attempt"
    assert all(row["status"] != "running" for row in rows), "拒绝重复派发 running outbox"
    assert all(row["payload"].get("script_id") == script_id for row in rows), (
        "拒绝派发不属于本轮剧本的 outbox"
    )

    dispatch_attempts = frozenset(
        (row["task_type"], row["job_id"], row["attempt"])
        for row in rows
    )
    for task_type, job_id, _ in dispatch_attempts:
        if task_type == "pipeline.dag":
            registry.pipeline_runs.add(job_id)
            registry.reservation_sources.add(("pipeline", job_id))
        elif task_type == "storyboard.generate":
            registry.storyboard_jobs.add(job_id)
            registry.reservation_sources.add(("storyboard", job_id))
        elif task_type == "generation.generate_images":
            registry.generation_jobs.add(job_id)
            registry.reservation_sources.add(("generation_job", job_id))
        elif task_type == "video.generate":
            registry.video_jobs.add(job_id)
            registry.reservation_sources.add(("generation_job", job_id))
        elif task_type == "timeline.render":
            registry.render_jobs.add(job_id)
            registry.reservation_sources.add(("timeline_render_job", job_id))
    result = send_task_with_runtime_gate(
        registry,
        "outbox.dispatch",
        purpose="business_outbox",
        dispatch_attempts=dispatch_attempts,
    )
    published = int(result.get(timeout=60) or 0)
    register_async_result_tree(result, ledger=PAID_BUDGET)
    assert published == len(rows), f"outbox 发布数量异常: {published} != {len(rows)}"
    with SYNC_ENGINE.connect() as connection:
        persisted = connection.execute(
            select(
                TaskDispatchOutbox.task_type,
                TaskDispatchOutbox.job_id,
                TaskDispatchOutbox.status,
                TaskDispatchOutbox.publish_attempts,
            ).where(TaskDispatchOutbox.job_id.in_({job_id for _, job_id in expectations}))
        ).all()
    matched = {
        (str(row.task_type), str(row.job_id)): (str(row.status), int(row.publish_attempts))
        for row in persisted
        if (str(row.task_type), str(row.job_id)) in expectations
    }
    assert matched == {expectation: ("published", 1) for expectation in expectations}, matched
    assert_known_backend_keys()
    print(f"outbox dispatched={published}", flush=True)
    return published


def assert_no_active_outbox() -> None:
    rows = active_outbox_snapshot()
    assert rows == [], f"仍有活动 outbox: {rows}"


def assert_job_artifacts(
    *,
    task_type: str,
    job_id: str,
    reservation_source_type: str,
) -> None:
    with SYNC_ENGINE.connect() as connection:
        outbox_count = int(
            connection.scalar(
                select(func.count())
                .select_from(TaskDispatchOutbox)
                .where(
                    TaskDispatchOutbox.task_type == task_type,
                    TaskDispatchOutbox.job_id == job_id,
                )
            )
            or 0
        )
        reservation_count = int(
            connection.scalar(
                select(func.count())
                .select_from(CreditReservation)
                .where(
                    CreditReservation.source_type == reservation_source_type,
                    CreditReservation.source_id == job_id,
                )
            )
            or 0
        )
    assert outbox_count == 1, f"任务 {job_id} 的 outbox 数量不是 1"
    assert reservation_count == 1, f"任务 {job_id} 的积分预约数量不是 1"


def assert_video_idempotency_request_count(job_id: str, expected: int) -> None:
    with SYNC_ENGINE.connect() as connection:
        actual = int(
            connection.scalar(
                select(func.count())
                .select_from(VideoGenerationIdempotencyRequest)
                .where(VideoGenerationIdempotencyRequest.job_id == job_id)
            )
            or 0
        )
    assert actual == expected, f"视频任务 {job_id} 幂等映射数量异常: {actual} != {expected}"


def retryable_remote_cancellation_job_ids() -> set[str]:
    with SYNC_ENGINE.connect() as connection:
        rows = (
            connection.execute(
                select(VideoGenerationJob.job_id).where(
                    VideoGenerationJob.status == "canceled",
                    VideoGenerationJob.output_json["provider_cancellation"]["retryable"]
                    .as_boolean()
                    .is_(True),
                )
            )
            .scalars()
            .all()
        )
    return {str(job_id) for job_id in rows}


def await_final_provider_cancellation(
    *,
    registry: CleanupRegistry,
    job_id: str,
    provider_task_id: str,
    initial: dict[str, Any],
    deadline: float | None = None,
) -> dict[str, Any]:
    """在预算内驱动 canceled 任务的持久化取消债务；耗尽时保留 residual。"""
    assert initial.get("provider_task_id") == provider_task_id
    assert initial.get("outcome") in {"canceled", "failed", "pending", "unsupported"}
    remote = registry.remote_video_tasks[job_id]
    stop_at = deadline or (time.monotonic() + MAX_REMOTE_DELETE_SECONDS)
    persisted = initial
    previous_attempts = max(0, int(initial.get("attempts", 0)))
    while True:
        _, output_json = video_job_snapshot(job_id)
        persisted = json_object(
            (output_json or {}).get("provider_cancellation"),
            label="数据库中的视频远端取消结果",
        )
        current_attempts = int(persisted.get("attempts", 0))
        assert previous_attempts <= current_attempts <= MAX_REMOTE_DELETE_ATTEMPTS
        previous_attempts = current_attempts
        persisted_task_id = str(persisted.get("provider_task_id") or "")
        if persisted_task_id != provider_task_id:
            registry.update_remote_video_task(
                remote,
                outcome="authority_conflict",
                retryable=False,
                attempts=current_attempts,
                detail="数据库远端取消 task ID 与独立账本不一致",
            )
            break
        outcome = persisted_remote_outcome(persisted)
        detail = str(persisted.get("detail") or "") or None
        registry.update_remote_video_task(
            remote,
            outcome=outcome,
            retryable=bool(persisted.get("retryable", True)),
            attempts=current_attempts,
            detail=detail,
            local_status="canceled",
        )
        if remote.terminal:
            break
        remaining = stop_at - time.monotonic()
        if (
            not remote.retryable
            or current_attempts >= MAX_REMOTE_DELETE_ATTEMPTS
            or remaining <= 0
        ):
            break
        debts = retryable_remote_cancellation_job_ids()
        assert job_id in debts, f"数据库未保留远端取消 debt: {debts}"
        assert not debts - registry.video_jobs, f"拒绝恢复未登记远端取消债务: {debts}"
        recovery = send_task_with_runtime_gate(
            registry,
            "app.workers.video_generation.reconcile_video_remote_cancellations",
            purpose="remote_cancellation_recovery",
            dispatch_attempts=frozenset(),
            kwargs={"limit": 1},
        )
        result = recovery.get(timeout=max(1.0, min(30.0, remaining)))
        register_async_result_tree(recovery, ledger=PAID_BUDGET)
        assert_known_backend_keys()
        assert isinstance(result, dict) and int(result.get("recovered", 0)) in {0, 1}
        if int(result.get("recovered", 0)) == 0:
            break

    assert remote.attempts <= MAX_REMOTE_DELETE_ATTEMPTS
    return persisted


def delete_remote_video_task(
    registry: CleanupRegistry,
    remote: RemoteVideoTask,
    *,
    deadline: float,
) -> None:
    """直接清理账本中的远端对象；只有结构化成功结果才能结清。"""
    while not remote.terminal:
        remaining = deadline - time.monotonic()
        if (
            not remote.retryable
            or remote.attempts >= MAX_REMOTE_DELETE_ATTEMPTS
            or remaining <= 0
        ):
            break
        started = time.monotonic()
        try:
            result = asyncio.run(
                asyncio.wait_for(
                    get_video_provider(remote.provider_key).cancel(remote.provider_task_id),
                    timeout=min(30.0, remaining),
                )
            )
        except Exception as exc:
            changes = {
                "attempts": remote.attempts + 1,
                "detail": f"远端删除调用失败: {type(exc).__name__}",
                "outcome": "failed",
                "retryable": True,
            }
        else:
            changes = {"attempts": remote.attempts + 1}
            if time.monotonic() > deadline:
                changes.update(
                    detail="Provider 清理结果超过总时间预算",
                    outcome="deadline_exceeded",
                    retryable=False,
                )
            elif result.provider_task_id != remote.provider_task_id:
                changes.update(
                    detail="Provider 清理结果 task ID 与独立账本不一致",
                    outcome="authority_conflict",
                    retryable=False,
                )
            else:
                outcome, retryable = direct_remote_cleanup_state(result.outcome.value)
                changes.update(
                    detail=result.detail,
                    outcome=outcome,
                    retryable=retryable,
                )
        registry.update_remote_video_task(remote, **changes)
        assert time.monotonic() - started <= min(30.0, remaining) + 1.0
    assert remote.attempts <= MAX_REMOTE_DELETE_ATTEMPTS


def reconcile_registered_remote_video_tasks(registry: CleanupRegistry) -> dict[str, Any]:
    """以独立账本清偿远端任务；本地行缺失时仍直接调用 Provider。"""
    started_at = datetime.now(UTC)
    deadline = time.monotonic() + MAX_REMOTE_DELETE_SECONDS
    for job_id, remote in sorted(registry.remote_video_tasks.items()):
        with SYNC_ENGINE.connect() as connection:
            row = connection.execute(
                select(
                    VideoGenerationJob.status,
                    VideoGenerationJob.provider_key,
                    VideoGenerationJob.output_json,
                ).where(VideoGenerationJob.job_id == job_id)
            ).one_or_none()
        if row is None:
            registry.update_remote_video_task(remote, local_status="missing")
            delete_remote_video_task(registry, remote, deadline=deadline)
            continue
        registry.update_remote_video_task(remote, local_status=str(row.status))
        if str(row.provider_key) != remote.provider_key:
            registry.update_remote_video_task(
                remote,
                outcome="authority_conflict",
                retryable=False,
                detail="数据库 Provider key 与独立账本不一致",
            )
            continue
        output_json = dict(row.output_json or {})
        persisted_task_ids = {
            str(value)
            for value in (
                output_json.get("provider_task_id"),
                (output_json.get("provider_cancellation") or {}).get("provider_task_id")
                if isinstance(output_json.get("provider_cancellation"), dict)
                else None,
            )
            if value
        }
        if persisted_task_ids != {remote.provider_task_id}:
            registry.update_remote_video_task(
                remote,
                outcome="authority_conflict",
                retryable=False,
                detail="数据库 Provider task ID 与独立账本不一致",
            )
            continue
        if remote.local_status == "canceled":
            debt = json_object(
                output_json.get("provider_cancellation"),
                label=f"视频 {job_id} 远端取消 debt",
            )
            await_final_provider_cancellation(
                registry=registry,
                job_id=job_id,
                provider_task_id=remote.provider_task_id,
                initial=debt,
                deadline=deadline,
            )
        else:
            delete_remote_video_task(registry, remote, deadline=deadline)
    settled = all(remote.terminal for remote in registry.remote_video_tasks.values())
    return signed_payload(
        {
            "status": "settled" if settled else "residual",
            "run_id": VALIDATION_RUN_ID,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "settled": settled,
            "tasks": {
                job_id: remote_task_payload(remote)
                for job_id, remote in sorted(registry.remote_video_tasks.items())
            },
        }
    )


def _cleanup_request(
    client: httpx.Client,
    method: str,
    path: str,
    **kwargs: Any,
) -> tuple[int, dict[str, Any] | None]:
    try:
        response = client.request(method, path, timeout=30.0, **kwargs)
        try:
            body = response.json()
        except ValueError:
            body = None
        return response.status_code, body if isinstance(body, dict) else None
    except Exception:
        return 0, None


def _discover_active_script_jobs(registry: CleanupRegistry) -> list[str]:
    """补获网络超时后服务端已落库、但客户端尚未来得及登记的任务。"""
    with SYNC_ENGINE.connect() as connection:
        registry.generation_jobs.update(
            str(value)
            for value in connection.execute(
                text(
                    "SELECT job_id FROM generation_jobs "
                    "WHERE script_id = :script_id AND status IN ('starting', 'processing')"
                ),
                {"script_id": registry.script_id},
            ).scalars()
        )
        registry.storyboard_jobs.update(
            str(value)
            for value in connection.execute(
                text(
                    "SELECT job_id FROM storyboard_jobs "
                    "WHERE script_id = :script_id "
                    "AND status IN ('pending', 'retry', 'running')"
                ),
                {"script_id": registry.script_id},
            ).scalars()
        )
        registry.video_jobs.update(
            str(value)
            for value in connection.execute(
                text(
                    "SELECT job_id FROM video_generation_jobs "
                    "WHERE script_id = :script_id AND status IN ('starting', 'processing')"
                ),
                {"script_id": registry.script_id},
            ).scalars()
        )
        registry.render_jobs.update(
            str(value)
            for value in connection.execute(
                text(
                    "SELECT job_id FROM timeline_render_jobs "
                    "WHERE script_id = :script_id AND status IN ('starting', 'processing')"
                ),
                {"script_id": registry.script_id},
            ).scalars()
        )
        active_pipeline_runs = (
            connection.execute(
                text(
                    "SELECT DISTINCT run_id FROM pipeline_jobs "
                    "WHERE script_id = :script_id "
                    "AND status IN ('pending', 'retry', 'ready', 'running')"
                ),
                {"script_id": registry.script_id},
            )
            .scalars()
            .all()
        )
        remote_rows = connection.execute(
            select(
                VideoGenerationJob.job_id,
                VideoGenerationJob.provider_key,
                VideoGenerationJob.status,
                VideoGenerationJob.output_json,
            ).where(VideoGenerationJob.script_id == registry.script_id)
        ).all()
    for row in remote_rows:
        output_json = dict(row.output_json or {})
        provider_task_id = str(output_json.get("provider_task_id") or "")
        if provider_task_id:
            registry.register_remote_video_task(
                job_id=str(row.job_id),
                provider_key=str(row.provider_key),
                provider_task_id=provider_task_id,
                local_status=str(row.status),
            )
    return sorted(str(value) for value in active_pipeline_runs)


def _cancel_registered_jobs(client: httpx.Client, registry: CleanupRegistry) -> dict[str, Any]:
    report: dict[str, Any] = {"generation": {}, "storyboard": {}, "video": {}, "render": {}}
    targets = (
        (
            "generation",
            registry.generation_jobs,
            lambda job_id: f"/v3api/generation-jobs/{job_id}",
            lambda job_id: f"/v3api/generation-jobs/{job_id}/cancel",
        ),
        (
            "storyboard",
            registry.storyboard_jobs,
            lambda job_id: (
                f"/v3api/scripts/{registry.script_id}/episodes/{registry.episode_id}/storyboard/{job_id}"
            ),
            lambda job_id: (
                f"/v3api/scripts/{registry.script_id}/episodes/{registry.episode_id}/"
                f"storyboard/{job_id}/cancel"
            ),
        ),
        (
            "video",
            registry.video_jobs,
            lambda job_id: f"/v3api/video-generation/{job_id}",
            lambda job_id: f"/v3api/video-generation/{job_id}/cancel",
        ),
        (
            "render",
            registry.render_jobs,
            lambda job_id: f"/v3api/video-timeline-arrangements/render/{job_id}",
            lambda job_id: f"/v3api/video-timeline-arrangements/render/{job_id}/cancel",
        ),
    )
    for label, job_ids, status_path, cancel_path in targets:
        for job_id in sorted(job_ids):
            status_code, body = _cleanup_request(client, "GET", status_path(job_id))
            status_value = (body or {}).get("status")
            if status_code == 200 and status_value not in TERMINAL:
                _cleanup_request(client, "POST", cancel_path(job_id))
                for _ in range(20):
                    _, refreshed = _cleanup_request(client, "GET", status_path(job_id))
                    status_value = (refreshed or {}).get("status")
                    if status_value in TERMINAL or refreshed is None:
                        break
                    time.sleep(0.25)
            report[label][job_id] = status_value or f"http_{status_code}"
    return report


def _recover_media_after_cleanup(registry: CleanupRegistry) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for _ in range(5):
        recovery = send_task_with_runtime_gate(
            registry,
            "app.workers.media_storage.recover_media_storage_jobs",
            purpose="media_recovery",
            dispatch_attempts=frozenset(),
            kwargs={"limit": 100},
        )
        result = recovery.get(timeout=120)
        register_async_result_tree(recovery, ledger=PAID_BUDGET)
        assert_known_backend_keys()
        if isinstance(result, dict):
            runs.append(
                {
                    "succeeded": len(result.get("succeeded", [])),
                    "retried": len(result.get("retried", [])),
                    "failed": len(result.get("failed", [])),
                }
            )
        if media_storage_active_count() == 0:
            break
    return runs


def active_task_snapshot() -> dict[str, list[str]]:
    queries = {
        "pipeline": (
            "SELECT run_id || ':' || stage FROM pipeline_jobs "
            "WHERE status IN ('pending', 'retry', 'ready', 'running')"
        ),
        "generation": ("SELECT job_id FROM generation_jobs WHERE status IN ('starting', 'processing')"),
        "storyboard": ("SELECT job_id FROM storyboard_jobs WHERE status IN ('pending', 'retry', 'running')"),
        "video": ("SELECT job_id FROM video_generation_jobs WHERE status IN ('starting', 'processing')"),
        "render": ("SELECT job_id FROM timeline_render_jobs WHERE status IN ('starting', 'processing')"),
        "credit_reservations": ("SELECT id FROM credit_reservations WHERE status = 'reserved'"),
    }
    with SYNC_ENGINE.connect() as connection:
        return {
            label: sorted(str(value) for value in connection.execute(text(query)).scalars())
            for label, query in queries.items()
        }


def parse_lsof_redis_connections(output: str) -> dict[int, int]:
    """将 lsof 字段输出解析为 client 本地端口到 PID 的映射。"""
    current_pid: int | None = None
    connections: dict[int, int] = {}
    for line in output.splitlines():
        if not line:
            continue
        prefix, value = line[0], line[1:]
        if prefix == "p":
            assert value.isdigit() and int(value) > 0
            current_pid = int(value)
        elif prefix == "n" and "->" in value:
            assert current_pid is not None, "lsof 连接缺少 PID"
            match = re.fullmatch(
                rf"127\.0\.0\.1:(\d+)->{re.escape(EXPECTED_REDIS_HOST)}:{EXPECTED_REDIS_PORT}",
                value,
            )
            if match is None:
                continue
            local_port = int(match.group(1))
            assert local_port not in connections or connections[local_port] == current_pid
            connections[local_port] = current_pid
    return connections


def evaluate_redis_clients(
    entries: list[dict[str, Any]],
    port_owners: dict[int, int],
    *,
    probe_names: set[str],
    allowed_pids: set[int],
    probe_only: bool,
) -> dict[str, Any]:
    """用 CLIENT LIST 的 addr 与 lsof PID 对账，不根据 Redis key 猜进程身份。"""
    relevant = [entry for entry in entries if int(entry.get("db", -1)) in {0, 1, 2}]
    seen_probe_names: set[str] = set()
    external_pids: set[int] = set()
    for entry in relevant:
        address = str(entry.get("addr") or "")
        match = re.fullmatch(r"127\.0\.0\.1:(\d+)", address)
        assert match is not None, "Redis client 地址不是固定 IPv4 loopback"
        owner_pid = port_owners.get(int(match.group(1)))
        assert owner_pid is not None, "Redis client 无法由 lsof 归属到本机 PID"
        name = str(entry.get("name") or "")
        if name in probe_names:
            seen_probe_names.add(name)
            assert owner_pid == os.getpid(), "Redis 具名探针不属于当前脚本"
            continue
        external_pids.add(owner_pid)
        if probe_only:
            raise AssertionError(f"worker shutdown 后仍有非探针 Redis client pid={owner_pid}")
        assert owner_pid in allowed_pids, f"Redis client 不属于本轮 worker/脚本 pid={owner_pid}"
    assert seen_probe_names == probe_names, "Redis CLIENT LIST 未完整观察三个具名探针"
    return {
        "client_count": len(relevant),
        "probe_count": len(seen_probe_names),
        "external_client_count": len(relevant) - len(seen_probe_names),
        "external_pids": sorted(external_pids),
        "probe_only": probe_only,
        "observed_at": datetime.now(UTC).isoformat(),
    }


def assert_redis_client_ownership(
    *,
    allowed_pids: set[int],
    probe_only: bool,
) -> dict[str, Any]:
    assert_redis_urls(settings.redis_url, settings.celery_broker_url, settings.celery_result_backend)
    lsof_path = shutil.which("lsof")
    assert lsof_path is not None, "缺少 lsof，无法归属 Redis client"
    clients: list[Redis] = []
    probes = (
        (f"mtv-validation-{RUN_HASH}-probe-app", settings.redis_url),
        (f"mtv-validation-{RUN_HASH}-probe-broker", settings.celery_broker_url),
        (f"mtv-validation-{RUN_HASH}-probe-backend", settings.celery_result_backend),
    )
    probe_names = {name for name, _ in probes}
    try:
        for name, raw_url in probes:
            client = Redis.from_url(raw_url, decode_responses=True)
            assert client.ping() is True
            assert client.client_setname(name) is True
            clients.append(client)
        completed = subprocess.run(
            [
                lsof_path,
                "-nP",
                "-a",
                f"-iTCP@{EXPECTED_REDIS_HOST}:{EXPECTED_REDIS_PORT}",
                "-sTCP:ESTABLISHED",
                "-Fpn",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert completed.stderr == "", "lsof 输出 stderr，拒绝形成连接证据"
        entries = clients[0].client_list()
        assert isinstance(entries, list)
        return evaluate_redis_clients(
            entries,
            parse_lsof_redis_connections(completed.stdout),
            probe_names=probe_names,
            allowed_pids=allowed_pids | {os.getpid()},
            probe_only=probe_only,
        )
    finally:
        for client in clients:
            client.close()


def celery_execution_snapshot() -> dict[str, Any]:
    inspector = celery_app.control.inspect(timeout=2.0)
    payloads: dict[str, Any] = {}
    worker_sets: list[frozenset[str]] = []
    unavailable: list[str] = []
    total = 0
    for name, getter in (
        ("active", inspector.active),
        ("reserved", inspector.reserved),
        ("scheduled", inspector.scheduled),
    ):
        payload = getter()
        if payload is None:
            unavailable.append(name)
            payloads[name] = None
            continue
        worker_sets.append(frozenset(str(worker) for worker in payload))
        compact = {
            worker: [str(item.get("id") or item.get("request", {}).get("id") or "unknown") for item in items]
            for worker, items in payload.items()
            if items
        }
        payloads[name] = compact
        total += sum(len(items) for items in compact.values())
    fingerprints_consistent = bool(worker_sets) and len(set(worker_sets)) == 1
    observed_workers = frozenset().union(*worker_sets) if worker_sets else frozenset()
    workers = next(iter(worker_sets), frozenset()) if fingerprints_consistent else observed_workers
    identity_valid = (
        not unavailable
        and fingerprints_consistent
        and len(workers) == 1
        and next(iter(workers)).startswith(WORKER_NAME_PREFIX)
    )
    return {
        "available": not unavailable,
        "unavailable": unavailable,
        "worker_count": len(workers),
        "identity_valid": identity_valid,
        "total": total,
        **payloads,
    }


def broker_execution_snapshot() -> dict[str, Any]:
    client: Redis | None = None
    try:
        client = Redis.from_url(settings.celery_broker_url, decode_responses=True)
        queues = {
            str(key): int(client.llen(key))
            for key in client.scan_iter()
            if client.type(key) == "list" and int(client.llen(key)) > 0
        }
        unacked = int(client.hlen("unacked"))
        unacked_index = int(client.zcard("unacked_index"))
        return {
            "available": True,
            "queues": queues,
            "queued": sum(queues.values()),
            "unacked": unacked,
            "unacked_index": unacked_index,
        }
    except Exception as exc:
        return {
            "available": False,
            "error_type": type(exc).__name__,
            "queues": {},
            "queued": -1,
            "unacked": -1,
            "unacked_index": -1,
        }
    finally:
        if client is not None:
            client.close()


def wait_for_execution_quiescence(timeout_seconds: float = 30.0) -> tuple[dict[str, Any], dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        celery_snapshot = celery_execution_snapshot()
        broker_snapshot = broker_execution_snapshot()
        if (
            celery_snapshot["available"]
            and celery_snapshot["identity_valid"]
            and celery_snapshot["total"] == 0
            and broker_snapshot["available"]
            and broker_snapshot["queued"] == 0
            and broker_snapshot["unacked"] == 0
            and broker_snapshot["unacked_index"] == 0
        ):
            return celery_snapshot, broker_snapshot
        if time.monotonic() >= deadline:
            return celery_snapshot, broker_snapshot
        time.sleep(0.5)


REQUIRED_CELERY_TASKS = frozenset(
    {
        "outbox.dispatch",
        "pipeline.split",
        "pipeline.extract",
        "pipeline.merge",
        "pipeline.enrich",
        "pipeline.parents",
        "pipeline.finalize",
        "app.workers.generation.generate_images_task",
        "app.workers.storyboard.generate_storyboard",
        "app.workers.video_generation.run_video_generation",
        "timeline.render",
        "app.workers.video_generation.reconcile_video_remote_cancellations",
        "app.workers.media_storage.recover_media_storage_jobs",
    }
)


def assert_isolated_worker_before_first_dispatch(
    *,
    expected_worker_name: str | None = None,
    expected_root_pid: int | None = None,
) -> dict[str, Any]:
    """付费前证明 inspect 与本机进程均只有同一个 nonce worker，且没有 beat。"""
    inspector = celery_app.control.inspect(timeout=3.0)
    responses = {
        "ping": inspector.ping(),
        "registered": inspector.registered(),
        "stats": inspector.stats(),
        "active": inspector.active(),
        "reserved": inspector.reserved(),
        "scheduled": inspector.scheduled(),
    }
    assert all(isinstance(payload, dict) and payload for payload in responses.values()), (
        "Celery inspect 不完整，拒绝首次派发"
    )
    worker_sets = {frozenset(payload) for payload in responses.values()}
    assert len(worker_sets) == 1, "Celery inspect 出现不一致 worker 指纹"
    workers = next(iter(worker_sets))
    assert len(workers) == 1, "必须恰有一个本轮 worker"
    worker_name = next(iter(workers))
    assert worker_name.startswith(WORKER_NAME_PREFIX), "worker 名称前缀不属于本 nonce"
    if expected_worker_name is not None:
        assert worker_name == expected_worker_name, "Celery worker 名称在派发间发生变化"
    registered = {
        str(value).split(" [", 1)[0]
        for value in responses["registered"][worker_name]
    }
    assert REQUIRED_CELERY_TASKS <= registered, (
        f"worker 缺少必要任务数量={len(REQUIRED_CELERY_TASKS - registered)}"
    )
    assert responses["active"][worker_name] == []
    assert responses["reserved"][worker_name] == []
    assert responses["scheduled"][worker_name] == []
    broker = broker_execution_snapshot()
    assert broker["available"] and broker["queued"] == broker["unacked"] == broker["unacked_index"] == 0
    process = collect_celery_process_evidence(expect_worker=True)
    root_pid = int(process["worker_root_pids"][0])
    if expected_root_pid is not None:
        assert root_pid == expected_root_pid, "Celery worker 根进程在派发间发生变化"
    redis_clients = assert_redis_client_ownership(
        allowed_pids=set(process["worker_process_pids"]),
        probe_only=False,
    )
    return {
        "worker_count": 1,
        "worker_name": worker_name,
        "worker_name_prefix": WORKER_NAME_PREFIX,
        "worker_root_pid": root_pid,
        "registered_required_task_count": len(REQUIRED_CELERY_TASKS),
        "active": 0,
        "reserved": 0,
        "scheduled": 0,
        "process": process,
        "broker": broker,
        "redis_clients": redis_clients,
        "observed_at": datetime.now(UTC).isoformat(),
    }


def shutdown_registered_worker(
    registry: CleanupRegistry,
    *,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """按 inspect destination 关闭唯一 worker，并证明进程与 Redis 连接均已消失。"""
    if registry.worker_shutdown_evidence is not None:
        return registry.worker_shutdown_evidence
    assert registry.worker_name is not None and registry.worker_root_pid is not None
    pre_shutdown = assert_isolated_worker_before_first_dispatch(
        expected_worker_name=registry.worker_name,
        expected_root_pid=registry.worker_root_pid,
    )
    assert_known_backend_keys()
    celery_app.control.shutdown(destination=[registry.worker_name])
    deadline = time.monotonic() + timeout_seconds
    last_ping: dict[str, Any] | None = None
    process_evidence: dict[str, Any] | None = None
    while True:
        ping = celery_app.control.inspect(timeout=1.0).ping()
        last_ping = ping if isinstance(ping, dict) else None
        try:
            process_evidence = collect_celery_process_evidence(expect_worker=False)
        except AssertionError as exc:
            if "worker shutdown 后仍有 Celery 进程" not in str(exc):
                raise
            process_evidence = None
        if not ping and process_evidence is not None:
            break
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"本轮 Celery worker 未在时限内关闭 ping_count={len(last_ping or {})}"
            )
        time.sleep(0.25)
    celery_app.close()
    time.sleep(0.25)
    redis_clients = assert_redis_client_ownership(
        allowed_pids={os.getpid()},
        probe_only=True,
    )
    evidence = {
        "worker_name": registry.worker_name,
        "worker_root_pid": registry.worker_root_pid,
        "destination_shutdown_sent": True,
        "inspect_response_count": 0,
        "worker_absent": True,
        "process": process_evidence,
        "redis_clients": redis_clients,
        "pre_shutdown": pre_shutdown,
        "finished_at": datetime.now(UTC).isoformat(),
    }
    registry.worker_shutdown_evidence = evidence
    return evidence


def _redis_key_shape(client: Any, key: str) -> dict[str, Any]:
    key_type = str(client.type(key))
    size_readers = {
        "string": client.strlen,
        "list": client.llen,
        "hash": client.hlen,
        "set": client.scard,
        "zset": client.zcard,
        "stream": client.xlen,
    }
    assert key_type in size_readers, f"Redis key 类型不可归属: {key_type}"
    if key_type == "string":
        value: Any = client.get(key)
    elif key_type == "list":
        value = list(client.lrange(key, 0, -1))
    elif key_type == "hash":
        value = sorted(
            (str(item_key), str(item_value))
            for item_key, item_value in client.hgetall(key).items()
        )
    elif key_type == "set":
        value = sorted(str(item) for item in client.smembers(key))
    elif key_type == "zset":
        value = [
            [str(item), format(float(score), ".17g")]
            for item, score in client.zrange(key, 0, -1, withscores=True)
        ]
    else:
        value = [
            [
                str(entry_id),
                sorted((str(item_key), str(item_value)) for item_key, item_value in fields.items()),
            ]
            for entry_id, fields in client.xrange(key)
        ]
    serialized = client.dump(key)
    assert isinstance(serialized, bytes) and serialized, f"Redis key DUMP 失败: {key}"
    return {
        "type": key_type,
        "size": int(size_readers[key_type](key)),
        "sha256": canonical_sha256({"type": key_type, "value": value}),
        "dump_b64": base64.b64encode(serialized).decode("ascii"),
        "dump_sha256": hashlib.sha256(serialized).hexdigest(),
    }


def capture_redis_key_shapes() -> dict[str, dict[str, dict[str, Any]]]:
    assert_redis_urls(settings.redis_url, settings.celery_broker_url, settings.celery_result_backend)
    result: dict[str, dict[str, dict[str, Any]]] = {}
    for label, raw_url in (
        ("app", settings.redis_url),
        ("broker", settings.celery_broker_url),
        ("backend", settings.celery_result_backend),
    ):
        client: Redis | None = None
        try:
            client = Redis.from_url(raw_url, decode_responses=True)
            assert client.ping() is True
            keys = sorted(str(key) for key in client.scan_iter())
            result[label] = {key: _redis_key_shape(client, key) for key in keys}
        finally:
            if client is not None:
                client.close()
    return result


def _redis_snapshot_evidence(snapshot: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "key_count": len(snapshot),
        "fingerprint_sha256": canonical_sha256(
            {"keys": sorted(snapshot), "shapes": dict(snapshot)}
        ),
    }


def _classify_redis_target_after_delete(
    expected: Mapping[str, Any],
    current: Mapping[str, Any],
) -> str:
    if current == expected:
        return "unchanged"
    if expected and not current:
        return "deleted"
    return "uncertain"


def _eval_redis_delete_target(
    label: str,
    raw_url: str,
    expected: dict[str, dict[str, Any]],
) -> None:
    client: Redis | None = None
    try:
        client = Redis.from_url(raw_url, decode_responses=True)
        _lua_delete_exact_redis_snapshot(client, label=label, expected=expected)
    finally:
        if client is not None:
            client.close()


def _delete_exact_redis_keys(
    plan: dict[str, dict[str, dict[str, Any]]],
    *,
    evaluator: Callable[[str, str, dict[str, dict[str, Any]]], None] | None = None,
    snapshotter: Callable[[], dict[str, dict[str, dict[str, Any]]]] | None = None,
    raw_urls: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """逐库提交并逐库留证；跨 DB 永不冒充原子成功。"""
    assert set(plan) == {"app", "broker", "backend"}
    evaluate = evaluator or _eval_redis_delete_target
    take_snapshot = snapshotter or capture_redis_key_shapes
    targets: dict[str, dict[str, Any]] = {}
    urls = raw_urls or {
        "app": settings.redis_url,
        "broker": settings.celery_broker_url,
        "backend": settings.celery_result_backend,
    }
    assert set(urls) == {"app", "broker", "backend"}
    target_urls = tuple((label, urls[label]) for label in ("app", "broker", "backend"))
    for index, (label, raw_url) in enumerate(target_urls):
        expected = plan[label]
        try:
            evaluate(label, raw_url, expected)
            after = take_snapshot()
        except BaseException as exc:
            try:
                after = take_snapshot()
                snapshot_error_type = None
            except BaseException as snapshot_error:
                after = {name: {} for name, _ in target_urls}
                snapshot_error_type = type(snapshot_error).__name__
            for current_label, _ in target_urls:
                current = after.get(current_label, {})
                state = (
                    "uncertain"
                    if snapshot_error_type is not None
                    else _classify_redis_target_after_delete(
                        plan[current_label],
                        current,
                    )
                )
                targets[current_label] = {
                    **targets.get(current_label, {}),
                    "state": state,
                    "planned": _redis_snapshot_evidence(plan[current_label]),
                    "failure_resnapshot_observed": _redis_snapshot_evidence(current),
                    "eval_attempted": targets.get(current_label, {}).get(
                        "eval_attempted",
                        current_label in {name for name, _ in target_urls[: index + 1]},
                    ),
                }
            return {
                "completed": False,
                "cross_db_atomic": False,
                "interrupted_at": label,
                "error_type": type(exc).__name__,
                "resnapshot_error_type": snapshot_error_type,
                "targets": targets,
            }
        state = _classify_redis_target_after_delete(expected, after[label])
        expected_state = "deleted" if expected else "unchanged"
        targets[label] = {
            "state": state,
            "planned": _redis_snapshot_evidence(expected),
            "post_eval_observed": _redis_snapshot_evidence(after[label]),
            "eval_attempted": True,
        }
        if state != expected_state:
            for remaining_label, _ in target_urls[index + 1 :]:
                targets[remaining_label] = {
                    "state": _classify_redis_target_after_delete(
                        plan[remaining_label],
                        after[remaining_label],
                    ),
                    "planned": _redis_snapshot_evidence(plan[remaining_label]),
                    "failure_resnapshot_observed": _redis_snapshot_evidence(
                        after[remaining_label]
                    ),
                    "eval_attempted": False,
                }
            return {
                "completed": False,
                "cross_db_atomic": False,
                "interrupted_at": label,
                "error_type": "PostDeleteStateMismatch",
                "resnapshot_error_type": None,
                "targets": targets,
            }
    return {
        "completed": True,
        "cross_db_atomic": False,
        "interrupted_at": None,
        "error_type": None,
        "resnapshot_error_type": None,
        "targets": targets,
    }


def assert_known_backend_keys() -> set[str]:
    for result in PAID_BUDGET.async_result_roots:
        register_async_result_tree(result, ledger=PAID_BUDGET)
    snapshot = capture_redis_key_shapes()["backend"]
    unknown = set(snapshot) - PAID_BUDGET.owned_backend_keys
    assert unknown == set(), "result backend 出现未绑定到本轮实际 task/group ID 的 key"
    assert all(_owned_backend_key_shape(key, shape) for key, shape in snapshot.items()), (
        "result backend 已登记 key 的名称、类型或完整指纹非法"
    )
    return set(snapshot)


def send_task_with_runtime_gate(
    registry: CleanupRegistry,
    task_name: str,
    *,
    purpose: str,
    dispatch_attempts: frozenset[tuple[str, str, int]],
    kwargs: dict[str, Any] | None = None,
) -> Any:
    """持久化 sending 后用预生成 ID 派发；任何不确定结果均禁止重发。"""
    assert registry.worker_name is not None and registry.worker_root_pid is not None
    assert registry.worker_name.startswith(WORKER_NAME_PREFIX)
    assert purpose in {
        "business_outbox",
        "remote_cancellation_recovery",
        "media_recovery",
    }
    assert isinstance(dispatch_attempts, frozenset)
    assert bool(dispatch_attempts) is (purpose == "business_outbox"), (
        "仅 business outbox 可以携带不可变预算 attempts"
    )
    assert all(
        state == "confirmed" for state in PAID_BUDGET.dispatch_delivery_states.values()
    ), "存在 sending/uncertain 派发，禁止任何后续自动派发"
    task_kwargs = dict(kwargs or {})
    assert_known_backend_keys()
    runtime_gate = assert_isolated_worker_before_first_dispatch(
        expected_worker_name=registry.worker_name,
        expected_root_pid=registry.worker_root_pid,
    )
    task_id = validated_celery_result_id(str(uuid.uuid4()))
    PAID_BUDGET.record_dispatch(dispatch_attempts)
    PAID_BUDGET.begin_dispatch_delivery(task_id)
    persist_dispatch_delivery_state(
        task_id=task_id,
        task_name=task_name,
        purpose=purpose,
        dispatch_attempts=dispatch_attempts,
        state="sending",
    )
    try:
        result = celery_app.send_task(task_name, kwargs=task_kwargs, task_id=task_id)
    except BaseException as original:
        PAID_BUDGET.mark_dispatch_delivery(task_id, "uncertain")
        try:
            persist_dispatch_delivery_state(
                task_id=task_id,
                task_name=task_name,
                purpose=purpose,
                dispatch_attempts=dispatch_attempts,
                state="uncertain",
                reason="send_exception",
                error_type=type(original).__name__,
            )
        except BaseException as persist_error:
            original.add_note(
                f"派发 uncertain 状态持久化失败: {type(persist_error).__name__}"
            )
        raise
    try:
        result_task_id = validated_celery_result_id(str(result.id))
        assert result_task_id == task_id, "Celery 返回 task ID 与预生成 ID 不一致"
    except BaseException as original:
        PAID_BUDGET.mark_dispatch_delivery(task_id, "uncertain")
        try:
            persist_dispatch_delivery_state(
                task_id=task_id,
                task_name=task_name,
                purpose=purpose,
                dispatch_attempts=dispatch_attempts,
                state="uncertain",
                reason="invalid_result_id",
                error_type=type(original).__name__,
            )
        except BaseException as persist_error:
            original.add_note(
                f"非法响应 uncertain 状态持久化失败: {type(persist_error).__name__}"
            )
        raise
    try:
        persist_dispatch_delivery_state(
            task_id=task_id,
            task_name=task_name,
            purpose=purpose,
            dispatch_attempts=dispatch_attempts,
            state="confirmed",
        )
    except BaseException as original:
        try:
            persist_dispatch_delivery_state(
                task_id=task_id,
                task_name=task_name,
                purpose=purpose,
                dispatch_attempts=dispatch_attempts,
                state="uncertain",
                reason="confirmation_persist_failed",
                error_type=type(original).__name__,
            )
        except BaseException as persist_error:
            original.add_note(
                f"确认失败后的 uncertain 状态持久化失败: {type(persist_error).__name__}"
            )
        PAID_BUDGET.mark_dispatch_delivery(task_id, "uncertain")
        raise
    PAID_BUDGET.mark_dispatch_delivery(task_id, "confirmed")
    PAID_BUDGET.async_result_roots.append(result)
    try:
        PAID_BUDGET.record_celery_dispatch(
            task_id,
            task_name=task_name,
            purpose=purpose,
            runtime_gate=runtime_gate,
        )
    except BaseException as original:
        PAID_BUDGET.mark_dispatch_delivery(task_id, "uncertain")
        try:
            persist_dispatch_delivery_state(
                task_id=task_id,
                task_name=task_name,
                purpose=purpose,
                dispatch_attempts=dispatch_attempts,
                state="uncertain",
                reason="audit_failed",
                error_type=type(original).__name__,
            )
        except BaseException as persist_error:
            original.add_note(f"审计失败状态持久化失败: {type(persist_error).__name__}")
        raise
    try:
        register_async_result_tree(result, ledger=PAID_BUDGET)
    except BaseException as original:
        PAID_BUDGET.mark_dispatch_delivery(task_id, "uncertain")
        try:
            persist_dispatch_delivery_state(
                task_id=task_id,
                task_name=task_name,
                purpose=purpose,
                dispatch_attempts=dispatch_attempts,
                state="uncertain",
                reason="result_tree_failed",
                error_type=type(original).__name__,
            )
        except BaseException as persist_error:
            original.add_note(
                f"result-tree 失败状态持久化失败: {type(persist_error).__name__}"
            )
        raise
    return result


def cleanup_run_redis_delta(
    baseline: dict[str, set[str]],
    *,
    owned_backend_keys: set[str],
    snapshotter: Callable[[], dict[str, dict[str, dict[str, Any]]]] | None = None,
    client_guard: Callable[[], dict[str, Any]] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    deleter: Callable[[dict[str, dict[str, dict[str, Any]]]], dict[str, Any]] = (
        _delete_exact_redis_keys
    ),
) -> dict[str, Any]:
    """worker shutdown 后以三次前置快照防 TOCTOU，再执行精确删除。"""
    take_snapshot = snapshotter or capture_redis_key_shapes
    guard = client_guard or (
        lambda: assert_redis_client_ownership(allowed_pids={os.getpid()}, probe_only=True)
    )
    assert all(not keys for keys in baseline.values()), "Redis 清理基线不是签名空基线"
    first_clients = guard()
    first = take_snapshot()
    sleeper(REDIS_STABLE_WINDOW_SECONDS)
    second_clients = guard()
    second = take_snapshot()
    plan = plan_redis_cleanup(
        baseline,
        first,
        second,
        owned_backend_keys=owned_backend_keys,
    )
    predelete_clients = guard()
    predelete = take_snapshot()
    assert predelete == second, "Redis 删除前快照发生变化，拒绝删除"
    deletion = deleter(plan)
    if deletion.get("completed") is not True:
        return {
            "completed": False,
            "ownership": "signed-empty-baseline+exact-observed-async-results",
            "stable_window_seconds": REDIS_STABLE_WINDOW_SECONDS,
            "clients": {
                "first": first_clients,
                "second": second_clients,
                "predelete": predelete_clients,
            },
            "cross_db_cleanup": deletion,
            "targets": deletion.get("targets", {}),
        }
    postdelete_clients = guard()
    postdelete = take_snapshot()
    assert all(postdelete[label] == {} for label in baseline), "Redis 删除后未回到签名空基线"
    return {
        "completed": True,
        "ownership": "signed-empty-baseline+exact-observed-async-results",
        "stable_window_seconds": REDIS_STABLE_WINDOW_SECONDS,
        "clients": {
            "first": first_clients,
            "second": second_clients,
            "predelete": predelete_clients,
            "postdelete": postdelete_clients,
        },
        "cross_db_cleanup": deletion,
        "targets": {
            label: {
                "baseline_count": 0,
                "deleted_delta_count": len(plan[label]),
                "remaining_delta_count": 0,
            }
            for label in ("app", "broker", "backend")
        },
    }


def recover_worker_after_authorization_failure(
    registry: CleanupRegistry,
    redis_baseline: dict[str, set[str]],
) -> dict[str, Any]:
    """只回收经当前 nonce 进程、inspect 与 destination 三重证明的唯一 worker。"""
    assert redis_baseline == {"app": set(), "broker": set(), "backend": set()}
    try:
        process_absent = collect_celery_process_evidence(expect_worker=False)
    except AssertionError:
        worker = assert_isolated_worker_before_first_dispatch()
        pre_shutdown_redis = capture_redis_key_shapes()
        assert pre_shutdown_redis["app"] == {}, "授权失败回收前应用 Redis 非空"
        assert pre_shutdown_redis["backend"] == {}, (
            "授权失败发生在任何任务派发前，result backend 非空时禁止推断归属"
        )
        assert PAID_BUDGET.owned_backend_keys == set()
        registry.worker_name = str(worker["worker_name"])
        registry.worker_root_pid = int(worker["worker_root_pid"])
        shutdown = shutdown_registered_worker(registry)
        redis_cleanup = cleanup_run_redis_delta(
            redis_baseline,
            owned_backend_keys=set(),
        )
        assert redis_cleanup.get("completed") is True
        return {
            "completed": True,
            "residual": False,
            "worker_absent": True,
            "worker_name": registry.worker_name,
            "worker_root_pid": registry.worker_root_pid,
            "shutdown": shutdown,
            "redis_cleanup": redis_cleanup,
        }

    redis_clients = assert_redis_client_ownership(
        allowed_pids={os.getpid()},
        probe_only=True,
    )
    redis_snapshot = capture_redis_key_shapes()
    assert all(redis_snapshot[label] == {} for label in redis_baseline), (
        "授权失败且 worker 已缺失时 Redis 未保持签名空基线，拒绝推断归属或删除"
    )
    return {
        "completed": True,
        "residual": False,
        "worker_absent": True,
        "process": process_absent,
        "redis_clients": redis_clients,
        "redis_key_counts": {label: 0 for label in redis_baseline},
    }


def assert_dispatch_integrity(
    ledger: PaidBudgetLedger,
    *,
    media_recovery_runs: int,
    media_recovery_stop_reason: str,
    remote_task_count: int,
) -> dict[str, Any]:
    with SYNC_ENGINE.connect() as connection:
        rows = connection.execute(
            select(
                TaskDispatchOutbox.task_type,
                TaskDispatchOutbox.job_id,
                TaskDispatchOutbox.attempt,
                TaskDispatchOutbox.status,
                TaskDispatchOutbox.publish_attempts,
            ).where(TaskDispatchOutbox.job_id.in_({job_id for _, job_id in ledger.dispatch_attempts}))
        ).all()
    grouped: dict[tuple[str, str], list[Any]] = {}
    for row in rows:
        grouped.setdefault((str(row.task_type), str(row.job_id)), []).append(row)
    for key, attempt in ledger.dispatch_attempts.items():
        matched = grouped.get(key, [])
        assert len(matched) == 1, f"业务 outbox attempt 数量异常: {key}"
        row = matched[0]
        assert int(row.attempt) == attempt
        assert str(row.status) == "published"
        assert int(row.publish_attempts) == 1
    assert set(grouped) == set(ledger.dispatch_attempts)
    return {
        **dispatch_integrity_summary(
            ledger,
            media_recovery_runs=media_recovery_runs,
            media_recovery_stop_reason=media_recovery_stop_reason,
            remote_task_count=remote_task_count,
        ),
        "duplicate_business_attempts": 0,
        "duplicate_outbox_publish_attempts": 0,
        "evidence_scope": "all script-initiated Celery business and recovery dispatches",
        "provider_sdk_internal_http_retries": "not_observable",
    }


def registry_integrity_snapshot(registry: CleanupRegistry, ledger: PaidBudgetLedger) -> dict[str, Any]:
    """按登记 ID 区分必须消失的业务行与允许保留的明确终态审计行。"""
    with SYNC_ENGINE.connect() as connection:
        remaining = {
            "pipeline": sorted(
                str(value)
                for value in connection.execute(
                    select(PipelineJob.run_id).where(PipelineJob.run_id.in_(registry.pipeline_runs))
                ).scalars()
            ),
            "generation": sorted(
                str(value)
                for value in connection.execute(
                    select(GenerationJob.job_id).where(GenerationJob.job_id.in_(registry.generation_jobs))
                ).scalars()
            ),
            "storyboard": sorted(
                str(value)
                for value in connection.execute(
                    select(StoryboardJob.job_id).where(StoryboardJob.job_id.in_(registry.storyboard_jobs))
                ).scalars()
            ),
            "video": sorted(
                str(value)
                for value in connection.execute(
                    select(VideoGenerationJob.job_id).where(VideoGenerationJob.job_id.in_(registry.video_jobs))
                ).scalars()
            ),
            "render": sorted(
                str(value)
                for value in connection.execute(
                    select(TimelineRenderJob.job_id).where(TimelineRenderJob.job_id.in_(registry.render_jobs))
                ).scalars()
            ),
        }
        dispatch_rows = connection.execute(
            select(
                TaskDispatchOutbox.task_type,
                TaskDispatchOutbox.job_id,
                TaskDispatchOutbox.status,
                TaskDispatchOutbox.attempt,
                TaskDispatchOutbox.publish_attempts,
            ).where(TaskDispatchOutbox.job_id.in_({job_id for _, job_id in ledger.dispatch_attempts}))
        ).all()
        reservation_ids = {source_id for _, source_id in registry.reservation_sources}
        reservation_rows = connection.execute(
            select(
                CreditReservation.source_type,
                CreditReservation.source_id,
                CreditReservation.status,
                CreditReservation.reserved_points,
                CreditReservation.actual_points,
            ).where(CreditReservation.source_id.in_(reservation_ids))
        ).all()
        media_rows = connection.execute(
            select(MediaStorageJob.id, MediaStorageJob.status).where(
                or_(
                    MediaStorageJob.source_key.in_(registry.storage_keys),
                    MediaStorageJob.final_key.in_(registry.storage_keys),
                )
            )
        ).all()

    registry.media_jobs.update(str(row.id) for row in media_rows)
    gone_ok = all(not values for values in remaining.values())
    outbox_residual = [
        str(row.job_id)
        for row in dispatch_rows
        if str(row.status) not in {"published", "canceled"} or int(row.publish_attempts) != 1
    ]
    reservations_by_source = {
        (str(row.source_type), str(row.source_id)): row
        for row in reservation_rows
    }
    reservation_residual = [
        source_id
        for source_type, source_id in sorted(registry.reservation_sources)
        if (source_type, source_id) not in reservations_by_source
        or str(reservations_by_source[(source_type, source_id)].status)
        not in {"captured", "released", "refunded"}
    ]
    media_residual = [str(row.id) for row in media_rows if str(row.status) not in {"succeeded", "canceled"}]
    completed = gone_ok and not outbox_residual and not reservation_residual and not media_residual
    return {
        "completed": completed,
        "must_be_gone": {label: {"remaining_ids": values, "passed": not values} for label, values in remaining.items()},
        "terminal_outbox": {
            "row_count": len(dispatch_rows),
            "residual_ids": sorted(outbox_residual),
        },
        "terminal_reservations": {
            "expected_count": len(registry.reservation_sources),
            "row_count": len(reservation_rows),
            "residual_source_ids": sorted(reservation_residual),
        },
        "terminal_media_jobs": {
            "row_count": len(media_rows),
            "residual_ids": sorted(media_residual),
        },
    }


def run_offline_guard_self_tests() -> list[str]:
    """不依赖数据库/网络的最小 fake，证明关键失败状态都按 residual 处理。"""
    evidence = assert_storage_guard_fault_injections()

    budget = PaidBudgetLedger(
        worker_prerequisites_verified=True,
        workflow_prerequisites_verified=True,
        text_prerequisites_verified=True,
        media_prerequisites_verified=True,
    )
    dispatch_index = 100
    for task_type, count in budget.LIMITS.items():
        for index in range(count):
            dispatch_index += 1
            task_id = f"00000000-0000-4000-8000-{dispatch_index:012x}"
            budget.begin_dispatch_delivery(task_id)
            budget.mark_dispatch_delivery(task_id, "confirmed")
            budget.record_celery_dispatch(
                task_id,
                task_name="outbox.dispatch",
                purpose="business_outbox",
                runtime_gate={
                    "worker_count": 1,
                    "worker_name": "self-test-worker",
                    "worker_root_pid": 100,
                    "process": {"beat_process_count": 0},
                },
            )
            budget.record_dispatch(frozenset({(task_type, f"{task_type}-{index}", 1)}))
    budget.assert_business_final()
    try:
        budget.record_dispatch(frozenset({("unknown.billable", "unknown", 1)}))
    except AssertionError:
        pass
    else:
        raise AssertionError("预算护栏未拒绝未知 task_type")
    evidence.append("paid_budget_allowlist")

    pending_remote = RemoteVideoTask("job", "volcengine", "task", "canceled")
    assert pending_remote.terminal is False
    pending_remote.outcome = "canceled"
    pending_remote.retryable = False
    assert pending_remote.terminal is True
    non_retryable_remote = RemoteVideoTask(
        "job-2",
        "volcengine",
        "task-2",
        "succeeded",
        outcome="unsupported",
        retryable=False,
    )
    assert non_retryable_remote.terminal is False
    evidence.append("remote_terminal_semantics")

    base = {
        "remote_terminal": True,
        "active_tasks": {"video": []},
        "active_outbox": [],
        "media_active": 0,
        "celery_snapshot": {"available": True, "worker_absent": True, "total": 0},
        "broker_snapshot": {
            "available": True,
            "queued": 0,
            "unacked": 0,
            "unacked_index": 0,
        },
        "process_snapshot": {"worker_root_count": 0, "beat_process_count": 0},
        "redis_clients": {"external_client_count": 0, "probe_only": True},
        "storage": {"clean": True, "files": [], "issues": []},
        "script_http_status": 404,
        "require_script_missing": True,
        "require_storage_empty": True,
        "require_media_quiet": True,
        "require_worker_absent": True,
    }
    assert all(evaluate_cleanup_gate(**base).values())

    worker_unreachable = dict(base)
    worker_unreachable["celery_snapshot"] = {"available": False, "total": 0}
    assert not all(evaluate_cleanup_gate(**worker_unreachable).values())
    evidence.append("worker_unreachable_is_residual")

    closed_client = dict(base)
    closed_client["script_http_status"] = 0
    assert not all(evaluate_cleanup_gate(**closed_client).values())
    evidence.append("closed_http_client_is_residual")

    remote_debt = dict(base)
    remote_debt["remote_terminal"] = False
    assert not all(evaluate_cleanup_gate(**remote_debt).values())
    evidence.append("retryable_remote_debt_is_residual")

    storage_failure = dict(base)
    storage_failure["storage"] = {
        "clean": False,
        "files": [],
        "issues": ["root_resolve_failed"],
    }
    assert not all(evaluate_cleanup_gate(**storage_failure).values())
    evidence.append("storage_failure_is_residual")

    first = {"run_id": VALIDATION_RUN_ID, "items": ["GET /a", "POST /b"]}
    second = {"items": ["GET /a", "POST /b"], "run_id": VALIDATION_RUN_ID}
    assert canonical_sha256(first) == canonical_sha256(second)
    assert parse_operation_set(first["items"], label="self-test") == {
        ("GET", "/a"),
        ("POST", "/b"),
    }
    evidence.append("canonical_evidence_digest")
    return evidence


def cleanup_gate_snapshot(
    client: httpx.Client,
    registry: CleanupRegistry,
    *,
    require_script_missing: bool,
    require_storage_empty: bool,
    require_media_quiet: bool,
    require_worker_absent: bool = False,
    registry_report: dict[str, Any] | None = None,
    redis_report: dict[str, Any] | None = None,
    redis_baseline: dict[str, set[str]] | None = None,
) -> dict[str, Any]:
    active_tasks = active_task_snapshot()
    worker_shutdown: dict[str, Any] | None = None
    if require_worker_absent:
        worker_shutdown = shutdown_registered_worker(registry)
        celery_snapshot = {
            "available": True,
            "identity_valid": False,
            "worker_absent": True,
            "worker_count": 0,
            "total": 0,
        }
        process_snapshot = worker_shutdown["process"]
        redis_clients = worker_shutdown["redis_clients"]
    else:
        celery_snapshot, _ = wait_for_execution_quiescence()
        process_snapshot = collect_celery_process_evidence(expect_worker=True)
        redis_clients = assert_redis_client_ownership(
            allowed_pids=set(process_snapshot["worker_process_pids"]),
            probe_only=False,
        )
    broker_snapshot = broker_execution_snapshot()
    storage = inspect_isolated_storage(Path(settings.storage_root))
    script_http_status, _ = _cleanup_request(
        client,
        "GET",
        f"/v3api/scripts/{registry.script_id}",
    )
    remote = {
        job_id: {
            "outcome": task.outcome,
            "retryable": task.retryable,
            "attempts": task.attempts,
            "terminal": task.terminal,
        }
        for job_id, task in sorted(registry.remote_video_tasks.items())
    }
    active_outbox = active_outbox_snapshot()
    media_active = media_storage_active_count()
    if redis_baseline is not None:
        assert require_worker_absent, "Redis delta 只能在本轮 worker shutdown 后清理"
        try:
            redis_report = cleanup_run_redis_delta(
                redis_baseline,
                owned_backend_keys=set(PAID_BUDGET.owned_backend_keys),
            )
        except Exception as exc:
            redis_report = {"completed": False, "error_type": type(exc).__name__}
    checks = evaluate_cleanup_gate(
        remote_terminal=all(task.terminal for task in registry.remote_video_tasks.values()),
        active_tasks=active_tasks,
        active_outbox=active_outbox,
        media_active=media_active,
        celery_snapshot=celery_snapshot,
        broker_snapshot=broker_snapshot,
        storage=storage,
        script_http_status=script_http_status,
        require_script_missing=require_script_missing,
        require_storage_empty=require_storage_empty,
        require_media_quiet=require_media_quiet,
        process_snapshot=process_snapshot,
        redis_clients=redis_clients,
        require_worker_absent=require_worker_absent,
        registry_integrity=(
            True if registry_report is None else registry_report.get("completed") is True
        ),
        redis_clean=(True if redis_report is None else redis_report.get("completed") is True),
    )
    return {
        "completed": all(checks.values()),
        "checks": checks,
        "remote": remote,
        "active_tasks": active_tasks,
        "active_outbox": active_outbox,
        "media_storage_active": media_active,
        "celery": celery_snapshot,
        "broker": broker_snapshot,
        "process": process_snapshot,
        "redis_clients": redis_clients,
        "worker_shutdown": worker_shutdown,
        "storage": storage,
        "script_http_status": script_http_status,
        "registry_integrity": registry_report,
        "redis_cleanup": redis_report,
    }


def best_effort_failure_cleanup(
    client: httpx.Client,
    registry: CleanupRegistry,
    redis_baseline: dict[str, set[str]],
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "armed": registry.cleanup_armed,
        "script_id": registry.script_id,
        "authorization_sha256": registry.cleanup_authorization_sha256,
        "worker_name_prefix": WORKER_NAME_PREFIX,
    }
    if not registry.cleanup_armed:
        report["status"] = "skipped_preflight_not_completed"
        return report

    if registry.worker_name is None or registry.worker_root_pid is None:
        report.update(
            recover_armed_preflight_failure(
                registry,
                redis_baseline,
                recover_worker_after_authorization_failure,
            )
        )
        return report

    script_status, _ = _cleanup_request(
        client,
        "GET",
        f"/v3api/scripts/{registry.script_id}",
    )
    report["initial_script_http_status"] = script_status
    report["plan"] = failure_cleanup_plan(script_status)

    if script_status == 200:
        report["active_pipeline_runs"] = _discover_active_script_jobs(registry)
        report["jobs"] = _cancel_registered_jobs(client, registry)
        _discover_active_script_jobs(registry)

    if registry.ledger_sealed:
        report["remote_cleanup"] = {
            "status": "skipped_ledger_sealed",
            "ledger_preserved": True,
        }
    else:
        try:
            report["remote_cleanup"] = reconcile_registered_remote_video_tasks(registry)
        except Exception as exc:
            report["remote_cleanup_error"] = type(exc).__name__

    if script_status == 200:
        predelete = cleanup_gate_snapshot(
            client,
            registry,
            require_script_missing=False,
            require_storage_empty=False,
            require_media_quiet=False,
        )
        report["predelete_gate"] = predelete
        if predelete["completed"]:
            delete_status, _ = _cleanup_request(
                client,
                "DELETE",
                f"/v3api/scripts/{registry.script_id}",
                params={"confirm": "true"},
            )
            report["script_delete_http_status"] = delete_status
        else:
            report["local_records_retained"] = True
    elif script_status == 404:
        report["script_delete_http_status"] = 404
    else:
        report["local_records_retained"] = True

    if registry.worker_shutdown_evidence is not None:
        report["media_recovery"] = {"status": "skipped_worker_shutdown"}
    elif not PAID_BUDGET.worker_prerequisites_verified:
        report["media_recovery"] = {"status": "skipped_before_paid_dispatch"}
    else:
        try:
            report["media_recovery"] = _recover_media_after_cleanup(registry)
        except Exception as exc:
            report["media_recovery_error"] = type(exc).__name__

    try:
        registry_report = registry_integrity_snapshot(registry, PAID_BUDGET)
    except Exception as exc:
        registry_report = {"completed": False, "error_type": type(exc).__name__}
    report["registry_integrity"] = registry_report
    final_gate = cleanup_gate_snapshot(
        client,
        registry,
        require_script_missing=True,
        require_storage_empty=True,
        require_media_quiet=True,
        require_worker_absent=True,
        registry_report=registry_report,
        redis_baseline=redis_baseline,
    )
    report["redis_cleanup"] = final_gate["redis_cleanup"]
    report["final_gate"] = final_gate
    report["status"] = "completed" if final_gate["completed"] else "residual"
    return report


@contextmanager
def cleanup_on_failure(
    client: httpx.Client,
    registry: CleanupRegistry,
    redis_baseline: dict[str, set[str]],
) -> Iterator[None]:
    try:
        yield
    except BaseException as original:
        try:
            report = best_effort_failure_cleanup(client, registry, redis_baseline)
        except BaseException as cleanup_error:
            report = {
                "armed": registry.cleanup_armed,
                "script_id": registry.script_id,
                "status": "cleanup_failed",
                "error_type": type(cleanup_error).__name__,
            }
            original.add_note(f"失败清理异常类型: {type(cleanup_error).__name__}")
        else:
            if report.get("status") != "completed":
                original.add_note("失败清理未通过最终门禁: residual")
        print(
            json.dumps({"failure_cleanup": report}, ensure_ascii=False, indent=2),
            flush=True,
        )
        raise
    else:
        if not registry.success:
            report = best_effort_failure_cleanup(client, registry, redis_baseline)
            raise AssertionError(f"主流程未标记成功，失败清理状态={report.get('status')}")


def poll_job(
    client: httpx.Client,
    path: str,
    *,
    timeout_seconds: int,
    label: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    previous: tuple[Any, Any, Any] | None = None
    while time.monotonic() < deadline:
        body = call(client, "GET", path)
        current = (body.get("status"), body.get("progress"), body.get("progress_stage"))
        if current != previous:
            print(f"{label}: status={current[0]} progress={current[1]} stage={current[2]}", flush=True)
            previous = current
        if body.get("status") in TERMINAL:
            return body
        time.sleep(3)
    raise TimeoutError(f"{label} 未在 {timeout_seconds}s 内进入终态")


def poll_until(
    client: httpx.Client,
    path: str,
    predicate,
    *,
    timeout_seconds: int,
    label: str,
    interval_seconds: float = 0.5,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        body = call(client, "GET", path)
        if predicate(body):
            return body
        if body.get("status") in TERMINAL:
            raise AssertionError(f"{label} 在满足条件前进入终态: {body}")
        time.sleep(interval_seconds)
    raise TimeoutError(f"{label} 未在 {timeout_seconds}s 内满足等待条件")


def poll_pipeline(client: httpx.Client, script_id: str, *, timeout_seconds: int = 900) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    previous: tuple[Any, ...] | None = None
    path = f"/v3api/scripts/{script_id}/pipeline-status"
    while time.monotonic() < deadline:
        body = call(client, "GET", path)
        current = tuple((stage["stage"], stage["status"]) for stage in body["stages"])
        if current != previous:
            print(f"pipeline: {current}", flush=True)
            previous = current
        if body["all_done"]:
            return body
        if body["failed"]:
            raise AssertionError(f"pipeline failed: {body}")
        time.sleep(3)
    raise TimeoutError("流水线未在超时前完成")


def assert_media(client: httpx.Client, storage_key: str, *, label: str) -> bytes:
    path = "/api/x/cctv/zhihui/flow-app/" + storage_key.lstrip("/")
    full = client.get(path)
    assert full.status_code == 200 and full.content, f"{label} 完整读取失败: {full.status_code}"
    media_type = full.headers.get("content-type", "").split(";", 1)[0]
    assert media_type.startswith(("image/", "audio/", "video/")), f"{label} MIME 非媒体类型: {media_type}"
    assert int(full.headers["content-length"]) == len(full.content)
    partial = client.get(path, headers={"Range": "bytes=0-31"})
    assert partial.status_code == 206 and 1 <= len(partial.content) <= 32
    assert partial.headers["content-range"].startswith("bytes 0-")
    assert partial.content == full.content[: len(partial.content)]
    assert partial.headers.get("content-type", "").split(";", 1)[0] == media_type
    EVENTS.extend(
        [
            {
                "method": "GET",
                "path": path,
                "template": operation_template("GET", path),
                "status": full.status_code,
                "bytes": len(full.content),
            },
            {
                "method": "GET",
                "path": path,
                "template": operation_template("GET", path),
                "status": partial.status_code,
                "bytes": len(partial.content),
            },
        ]
    )
    return full.content


def ffprobe_duration(content: bytes) -> float:
    with tempfile.NamedTemporaryFile(suffix=".mp4") as handle:
        handle.write(content)
        handle.flush()
        completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                handle.name,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    duration = float(completed.stdout.strip())
    assert math.isfinite(duration) and duration > 0
    return duration


def wav_bytes(duration_seconds: float = 1.0) -> bytes:
    target = BytesIO()
    frame_count = int(16_000 * duration_seconds)
    with wave.open(target, "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(16_000)
        output.writeframes(b"\x00\x00" * frame_count)
    return target.getvalue()


def png_bytes() -> bytes:
    return base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
    )


def list_subjects(client: httpx.Client, script_id: str) -> dict[str, dict[str, Any]]:
    payload = call(
        client,
        "GET",
        f"/v3api/scripts/{script_id}/asset-library/subjects",
        params={"limit": 500},
    )
    return {item["subject_id"]: item for item in payload["items"]}


def approve_generated_image(
    client: httpx.Client,
    *,
    script_id: str,
    subject_id: str,
    image_output: dict[str, Any],
) -> str:
    subjects = list_subjects(client, script_id)
    current_entry = subjects[subject_id].get("image_entry")
    current_asset_id = current_entry.get("asset_id") if current_entry else None
    approval_path = f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-image"
    payload = {
        "candidate_id": image_output["assetId"],
        "expected_image_entry_id": current_asset_id,
    }
    if current_asset_id is not None:
        impact = call(
            client,
            "GET",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/replace-impact",
        )
        if impact["referenced"] is True:
            dry_run = call(client, "POST", approval_path, json={**payload, "confirm": False})
            assert dry_run["impact"] == impact
            assert dry_run["subject"]["image_entry"]["asset_id"] == current_asset_id
            assert list_subjects(client, script_id)[subject_id]["image_entry"]["asset_id"] == current_asset_id
            approved = call(client, "POST", approval_path, json={**payload, "confirm": True})
            assert approved["impact"] == impact
        else:
            approved = call(client, "POST", approval_path, json={**payload, "confirm": False})
            assert approved["impact"] == impact
    else:
        approved = call(client, "POST", approval_path, json={**payload, "confirm": False})
        assert approved["impact"] is None
    assert approved["subject"]["image_entry"]["asset_id"] == image_output["assetId"]
    return str(image_output["storageKey"])


def credit_reservation_snapshot(source_type: str, source_id: str) -> dict[str, Any]:
    with SYNC_ENGINE.connect() as connection:
        row = connection.execute(
            select(
                CreditReservation.id,
                CreditReservation.account_id,
                CreditReservation.status,
                CreditReservation.reserved_points,
                CreditReservation.actual_points,
            ).where(
                CreditReservation.source_type == source_type,
                CreditReservation.source_id == source_id,
            )
        ).one()
    return {
        "reservation_id": str(row.id),
        "account_id": str(row.account_id),
        "status": str(row.status),
        "reserved_points": int(row.reserved_points),
        "actual_points": None if row.actual_points is None else int(row.actual_points),
    }


def assert_credit_lifecycle(
    logs: list[dict[str, Any]],
    source_type: str,
    source_id: str,
    terminal_type: str,
    *,
    expected_reserve_actor_user_id: str | None,
    expected_terminal_actor_user_id: str | None,
) -> dict[str, Any]:
    assert terminal_type in {"settle", "release"}
    matched = [
        item
        for item in logs
        if item.get("source_type") == source_type and item.get("source_id") == source_id
    ]
    assert len(matched) == 2, (source_id, [item["log_type"] for item in matched])
    reserves = [item for item in matched if item["log_type"] == "reserve"]
    terminals = [item for item in matched if item["log_type"] == terminal_type]
    opposite_terminal = "release" if terminal_type == "settle" else "settle"
    assert len(reserves) == 1 and len(terminals) == 1, (
        source_id,
        [item["log_type"] for item in matched],
    )
    assert all(item["log_type"] != opposite_terminal for item in matched)
    reserve = reserves[0]
    terminal = terminals[0]
    assert reserve.get("actor_user_id") == expected_reserve_actor_user_id
    assert terminal.get("actor_user_id") == expected_terminal_actor_user_id
    reservation_id = reserve.get("reservation_id")
    assert isinstance(reservation_id, str) and reservation_id
    assert terminal.get("reservation_id") == reservation_id
    assert reserve["points_delta"] == 0
    assert reserve["frozen_delta"] > 0
    assert terminal["frozen_delta"] == -reserve["frozen_delta"]
    if terminal_type == "settle":
        assert terminal["points_delta"] < 0
    else:
        assert terminal["points_delta"] == 0
    reservation = credit_reservation_snapshot(source_type, source_id)
    assert reservation["account_id"] == OWNER_CREDIT_ID
    assert reserve["frozen_delta"] == reservation["reserved_points"]
    if terminal_type == "settle":
        assert reservation["status"] == "captured"
        assert reservation["actual_points"] == -terminal["points_delta"]
    else:
        assert reservation["status"] == "released"
        assert reservation["actual_points"] is None
    return {
        "source_type": source_type,
        "source_id": source_id,
        "terminal_type": terminal_type,
        "reserve_actor_user_id": expected_reserve_actor_user_id,
        "terminal_actor_user_id": expected_terminal_actor_user_id,
        **reservation,
        "settle_points_delta": terminal["points_delta"] if terminal_type == "settle" else 0,
    }


def flatten_unique_subjects(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        subject_id = item.get("subject_id")
        assert isinstance(subject_id, str) and subject_id, item
        if subject_id in seen:
            continue
        seen.add(subject_id)
        result.append(item)
    return result


def prepare_worker_start_authorization(
    *,
    started_at: datetime,
    live_result: dict[str, Any],
    script_id: str,
    episode_id: str,
) -> dict[str, Any]:
    """第一阶段必须在 worker 启动前运行；签名授权后才允许人工启动 worker。"""
    claim_async_result(started_at)
    stable_seconds = MIN_HTTP_STABLE_WINDOW_SECONDS + 0.25
    first_process = collect_celery_process_evidence(expect_worker=False)
    first_clients = assert_redis_client_ownership(
        allowed_pids={os.getpid()},
        probe_only=True,
    )
    first_redis = capture_redis_key_shapes()
    assert all(first_redis[label] == {} for label in ("app", "broker", "backend"))
    initial_pipeline = initial_pipeline_expectation(script_id, episode_id)
    time.sleep(stable_seconds)
    second_process = collect_celery_process_evidence(expect_worker=False)
    second_clients = assert_redis_client_ownership(
        allowed_pids={os.getpid()},
        probe_only=True,
    )
    second_redis = capture_redis_key_shapes()
    assert second_redis == first_redis, "worker 启动授权窗口内 Redis 状态发生变化"
    assert initial_pipeline_expectation(script_id, episode_id) == initial_pipeline
    authorization = signed_payload(
        {
            "run_id": VALIDATION_RUN_ID,
            "http_result_sha256": live_result["sha256"],
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "stable_window_seconds": stable_seconds,
            "redis_baseline_empty": {"app": True, "broker": True, "backend": True},
            "initial_pipeline": {
                "task_type": initial_pipeline[0],
                "job_id": initial_pipeline[1],
                "publish_attempts": 0,
            },
            "required_worker": {
                "app": EXPECTED_CELERY_APP,
                "worker_name_prefix": WORKER_NAME_PREFIX,
                "beat_allowed": False,
            },
            "first": {
                "process": first_process,
                "redis_clients": first_clients,
                "redis_key_counts": {label: len(keys) for label, keys in first_redis.items()},
            },
            "second": {
                "process": second_process,
                "redis_clients": second_clients,
                "redis_key_counts": {label: len(keys) for label, keys in second_redis.items()},
            },
        }
    )
    result = replace_signed_claim(
        ASYNC_RESULT_PATH,
        {
            "status": "awaiting_worker",
            "run_id": VALIDATION_RUN_ID,
            "started_at": started_at.isoformat(),
            "finished_at": None,
            "worker_start_authorization": authorization,
        },
    )
    assert read_signed_json(ASYNC_RESULT_PATH, label="worker start authorization claim") == result
    print(
        json.dumps(
            {
                "status": "awaiting_worker",
                "run_id": VALIDATION_RUN_ID,
                "worker_name_prefix": WORKER_NAME_PREFIX,
                "authorization_sha256": authorization["sha256"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return authorization


def load_worker_start_authorization(
    *,
    live_result: dict[str, Any],
    script_id: str,
    episode_id: str,
    now: datetime,
) -> dict[str, Any]:
    assert not REMOTE_LEDGER_PATH.exists() and not REMOTE_LEDGER_PATH.is_symlink()
    claim = read_signed_json(ASYNC_RESULT_PATH, label="worker start authorization claim")
    assert claim.get("status") == "awaiting_worker" and claim.get("run_id") == VALIDATION_RUN_ID
    authorization = claim.get("worker_start_authorization")
    assert isinstance(authorization, dict)
    verify_signed_payload(authorization, label="worker start authorization")
    assert authorization.get("run_id") == VALIDATION_RUN_ID
    assert authorization.get("http_result_sha256") == live_result["sha256"]
    assert authorization.get("redis_baseline_empty") == {
        "app": True,
        "broker": True,
        "backend": True,
    }
    assert authorization.get("required_worker") == {
        "app": EXPECTED_CELERY_APP,
        "worker_name_prefix": WORKER_NAME_PREFIX,
        "beat_allowed": False,
    }
    finished_at = datetime.fromisoformat(str(authorization["finished_at"]))
    assert finished_at.tzinfo is not None
    age_seconds = (now - finished_at).total_seconds()
    assert 0 <= age_seconds <= MAX_WORKER_START_AUTH_AGE_SECONDS, "worker 启动授权已过期"
    assert float(authorization.get("stable_window_seconds", 0)) >= MIN_HTTP_STABLE_WINDOW_SECONDS
    for sample_name in ("first", "second"):
        sample = authorization.get(sample_name)
        assert isinstance(sample, dict)
        process = sample.get("process")
        redis_clients = sample.get("redis_clients")
        assert isinstance(process, dict) and process.get("worker_root_count") == 0
        assert process.get("beat_process_count") == 0
        assert isinstance(redis_clients, dict) and redis_clients.get("external_client_count") == 0
        assert sample.get("redis_key_counts") == {"app": 0, "broker": 0, "backend": 0}
    expected_pipeline = authorization.get("initial_pipeline")
    assert isinstance(expected_pipeline, dict) and expected_pipeline.get("publish_attempts") == 0
    assert initial_pipeline_expectation(script_id, episode_id) == (
        expected_pipeline.get("task_type"),
        expected_pipeline.get("job_id"),
    )
    return authorization


def main() -> None:
    started_at = datetime.now(UTC)
    seed_manifest, live_result = load_verified_live_result(started_at)
    context = live_result["context"]
    script_id = context["script_id"]
    episode_id = context["episode_id"]
    if not ASYNC_RESULT_PATH.exists() and not ASYNC_RESULT_PATH.is_symlink():
        prepare_worker_start_authorization(
            started_at=started_at,
            live_result=live_result,
            script_id=script_id,
            episode_id=episode_id,
        )
        return
    material_id = context["material_id"]
    subject_id = context["subject_id"]
    episode_version = context["episode_version"]
    client_save_seq = context["client_save_seq"]
    tracked_storage_keys = {str(key) for key in context["storage_keys"]}
    registry = CleanupRegistry(
        script_id=script_id,
        episode_id=episode_id,
        material_id=material_id,
        storage_keys=tracked_storage_keys,
    )
    redis_baseline = validate_http_isolation_evidence(live_result)
    worker_start_authorization = load_worker_authorization_fail_closed(
        lambda: load_worker_start_authorization(
            live_result=live_result,
            script_id=script_id,
            episode_id=episode_id,
            now=started_at,
        ),
        lambda: recover_worker_after_authorization_failure(registry, redis_baseline),
        lambda report: print(
            json.dumps(
                {"worker_authorization_failure_cleanup": report},
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        ),
    )
    arm_cleanup_after_worker_authorization(registry, worker_start_authorization)
    workflow_started_at = datetime.fromisoformat(str(worker_start_authorization["started_at"]))

    with httpx.Client(base_url=BASE_URL, timeout=120.0) as client, cleanup_on_failure(
        client,
        registry,
        redis_baseline,
    ):
        load_openapi(client)
        recorded_operations = parse_operation_set(
            live_result["operation_coverage"]["operation_set"],
            label="HTTP operation_set",
        )
        assert OPENAPI_OPERATIONS == recorded_operations, "异步阶段 OpenAPI 与 HTTP 阶段集合不一致"
        call(client, "POST", "/v3api/auth/login", json=LOGIN)
        offline_guard_evidence, preflight_redis_baseline = assert_real_flow_preconditions(
            live_result=live_result,
            context=context,
        )
        assert preflight_redis_baseline == redis_baseline
        worker_evidence = assert_isolated_worker_before_first_dispatch()
        registry.worker_name = str(worker_evidence["worker_name"])
        registry.worker_root_pid = int(worker_evidence["worker_root_pid"])
        initial_pipeline = initial_pipeline_expectation(script_id, episode_id)
        assert initial_pipeline == (
            worker_start_authorization["initial_pipeline"]["task_type"],
            worker_start_authorization["initial_pipeline"]["job_id"],
        )
        async_preflight = persist_async_preflight(
            live_result=live_result,
            worker_evidence=worker_evidence,
            redis_baseline=redis_baseline,
        )
        assert async_preflight["worker_start_authorization_sha256"] == worker_start_authorization["sha256"]
        PAID_BUDGET.worker_prerequisites_verified = True
        PAID_BUDGET.workflow_prerequisites_verified = True
        PAID_BUDGET.text_prerequisites_verified = True

        initial_pipeline_reservation = credit_reservation_snapshot(
            "pipeline",
            initial_pipeline[1],
        )
        owner_accounts_before = call(client, "GET", "/v3api/credits/accounts")["items"]
        owner_account_before = next(
            item for item in owner_accounts_before if item["id"] == OWNER_CREDIT_ID
        )
        owner_balance_before = int(owner_account_before["balance_points"])
        owner_frozen_before = int(owner_account_before["frozen_points"])
        assert owner_frozen_before == initial_pipeline_reservation["reserved_points"]

        dispatch_expected(script_id, {initial_pipeline}, registry=registry)
        pipeline = poll_pipeline(client, script_id)
        extract = call(client, "GET", f"/v3api/scripts/{script_id}/extract-subjects-status")
        assert extract["all_extracted"] is True and extract["merge_status"] == "succeeded"
        subjects = list_subjects(client, script_id)
        assert len(subjects) >= 4 and subject_id in subjects

        locked_subject = call(
            client,
            "PATCH",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}",
            json={
                "description": "人工锁定验收描述：林舟穿着紫色宇航服，禁止流水线直接覆盖。",
                "expected_subject_version": subjects[subject_id]["subject_version"],
            },
        )
        manual_payload = {
            "mode": "manual_reextract",
            "episode_version": episode_version,
            "client_save_seq": client_save_seq,
        }
        manual = call(
            client,
            "POST",
            f"/v3api/scripts/{script_id}/episodes/{episode_id}/pipeline-runs",
            202,
            json=manual_payload,
            headers={"Idempotency-Key": idempotency_key("pipeline-manual")},
        )
        manual_replay = call(
            client,
            "POST",
            f"/v3api/scripts/{script_id}/episodes/{episode_id}/pipeline-runs",
            202,
            json=manual_payload,
            headers={"Idempotency-Key": idempotency_key("pipeline-manual")},
        )
        assert manual_replay["run_id"] == manual["run_id"]
        assert manual_replay["idempotent_replay"] is True
        dispatch_expected(
            script_id,
            {("pipeline.dag", manual["run_id"])},
            registry=registry,
        )
        pipeline = poll_pipeline(client, script_id)
        assert len(pipeline["stages"]) == 5 and pipeline["all_done"] is True

        suggestions = call(
            client,
            "GET",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions",
        )
        assert suggestions["total"] >= 1, "人工锁字段应产生非破坏性差异建议"
        matching_suggestions = [
            item
            for item in suggestions["items"]
            if item.get("source_run_id") == manual["run_id"]
            and item.get("field") == "description"
            and item.get("status") == "pending"
            and item.get("current_value") == locked_subject["description"]
            and isinstance(item.get("suggested_value"), str)
            and bool(item["suggested_value"].strip())
            and item["suggested_value"] != item["current_value"]
        ]
        assert len(matching_suggestions) == 1, (
            f"应唯一命中本次人工重提取的描述差异建议，实际 {len(matching_suggestions)} 条"
        )
        target_suggestion = matching_suggestions[0]
        current_subject = list_subjects(client, script_id)[subject_id]
        resolved_suggestion = call(
            client,
            "POST",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions/{target_suggestion['suggestion_id']}/resolve",
            json={
                "action": "ignore",
                "expected_subject_version": current_subject["subject_version"],
            },
        )
        assert resolved_suggestion["status"] == "ignored"
        assert list_subjects(client, script_id)[subject_id]["description"] == locked_subject["description"]

        canceled_storyboard_id = context["canceled_storyboard_job_id"]
        storyboard_path = (
            f"/v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{canceled_storyboard_id}/retry"
        )
        storyboard = call(
            client,
            "POST",
            storyboard_path,
            headers={"Idempotency-Key": idempotency_key("storyboard")},
        )
        storyboard_replay = call(
            client,
            "POST",
            storyboard_path,
            headers={"Idempotency-Key": idempotency_key("storyboard")},
        )
        assert storyboard_replay["job_id"] == storyboard["job_id"]
        registry.storyboard_jobs.add(storyboard["job_id"])
        dispatch_expected(
            script_id,
            {("storyboard.generate", storyboard["job_id"])},
            registry=registry,
        )
        storyboard = poll_job(
            client,
            f"/v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{storyboard['job_id']}",
            timeout_seconds=600,
            label="storyboard",
        )
        assert storyboard["status"] == "succeeded", storyboard
        storyboard = call(
            client,
            "PATCH",
            f"/v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{storyboard['job_id']}",
            json={"progress_message": "真实验收完成"},
        )
        assert storyboard["progress_message"] == "真实验收完成"
        hierarchy = call(client, "GET", f"/v3api/scripts/{script_id}/episodes/{episode_id}/shot-hierarchy")
        groups = [group for scene in hierarchy["scenes"] for group in scene["groups"]]
        shots = [shot for group in groups for shot in group["shots"]]
        assert groups and shots
        for group in groups:
            assert group["prompt"].endswith(CONSTRAINT_LOCK)
            assert 2 <= len(group["shots"]) <= 5
            assert set(group["reference_frames"]) == {
                "first_frame",
                "last_frame",
                "reference_frame",
            }
            ordered_shots = sorted(group["shots"], key=lambda item: item["display_no"])
            assert ordered_shots[0]["start_seconds"] == 0
            for previous, current in zip(ordered_shots, ordered_shots[1:], strict=False):
                assert previous["end_seconds"] == current["start_seconds"]
            prompt_offsets = [group["prompt"].index(item["shot_line"]) for item in ordered_shots]
            assert prompt_offsets == sorted(prompt_offsets)
            assert all("在【" in item["description"] and item["shot_line"] for item in ordered_shots)

        first_group_id = groups[0]["shot_group_id"]
        workbench = call(client, "GET", f"/v3api/shots/{script_id}/episodes/{episode_id}/video-workbench")
        workbench_groups = [group for scene in workbench["scenes"] for group in scene["shot_groups"]]
        first_workbench_group = next(
            group for group in workbench_groups if group["shot_group_id"] == first_group_id
        )
        unit_runtime_key = first_workbench_group["unit_runtime_key"]
        assert unit_runtime_key
        assert 3 <= first_workbench_group["duration_seconds"] <= 15

        visual_items = flatten_unique_subjects(
            [
                item
                for category in ("character", "background", "prop", "other")
                for item in (first_workbench_group.get("materials") or {}).get(category, [])
            ]
        )
        audio_items = flatten_unique_subjects(first_workbench_group.get("audio_elements") or [])
        assert visual_items, "真实视频镜头组必须至少引用一个视觉主体"
        missing_before = call(
            client,
            "GET",
            "/v3api/video-generation/check-materials",
            params={"script_id": script_id, "shot_group_id": first_group_id},
        )
        has_missing_materials = bool(missing_before["missing_subjects"] or missing_before["missing_audio"])
        assert missing_before["ready"] is (not has_missing_materials)
        current_subject_ids = set(list_subjects(client, script_id))
        referenced_subject_ids = {item["subject_id"] for item in [*visual_items, *audio_items]}
        assert referenced_subject_ids <= current_subject_ids
        assert all(group["shots"] for group in groups)
        assert all(
            validate_key_prefix(str(key)).startswith("upload_images/") for key in context["storage_keys"]
        )
        PAID_BUDGET.media_prerequisites_verified = True

        primary_subject_id = visual_items[0]["subject_id"]
        generation_payload = {
            "material_id": material_id,
            "subject_id": primary_subject_id,
            "model": "qwen-image",
            "size": "2k",
            "aspect": "16:9",
            "count": 1,
        }
        canceled_generation = call(
            client,
            "POST",
            "/v3api/generation-jobs",
            201,
            json=generation_payload,
            headers={"Idempotency-Key": idempotency_key("generation-cancel")},
        )
        registry.generation_jobs.add(canceled_generation["job_id"])
        registry.reservation_sources.add(("generation_job", canceled_generation["job_id"]))
        canceled_generation = call(
            client,
            "POST",
            f"/v3api/generation-jobs/{canceled_generation['job_id']}/cancel",
        )
        assert canceled_generation["status"] == "canceled"
        assert active_outbox_rows(canceled_generation["job_id"]) == []

        generation = call(
            client,
            "POST",
            "/v3api/generation-jobs",
            201,
            json=generation_payload,
            headers={"Idempotency-Key": idempotency_key("generation-single")},
        )
        registry.generation_jobs.add(generation["job_id"])
        generation_replay = call(
            client,
            "POST",
            "/v3api/generation-jobs",
            201,
            json=generation_payload,
            headers={"Idempotency-Key": idempotency_key("generation-single")},
        )
        assert generation_replay["job_id"] == generation["job_id"]
        assert_job_artifacts(
            task_type="generation.generate_images",
            job_id=generation["job_id"],
            reservation_source_type="generation_job",
        )
        generation_input = json_object(generation["input_json"], label="生图 input_json")
        resolved_prompt = generation_input["resolved_prompts"]["final"]
        assert len(re.findall(r"^【[^】]+】", resolved_prompt, flags=re.MULTILINE)) == 12
        dispatch_expected(
            script_id,
            {("generation.generate_images", generation["job_id"])},
            registry=registry,
        )
        generation = poll_job(
            client,
            f"/v3api/generation-jobs/{generation['job_id']}",
            timeout_seconds=600,
            label="generation-single",
        )
        assert generation["status"] == "succeeded", generation
        image_outputs = json_object(generation["output_json"], label="生图 output_json")["imageOutputs"]
        assert len(image_outputs) == 1
        tracked_storage_keys.add(
            approve_generated_image(
                client,
                script_id=script_id,
                subject_id=primary_subject_id,
                image_output=image_outputs[0],
            )
        )
        assert_media(client, image_outputs[0]["storageKey"], label="真实单主体生图")

        batch_subject_ids = [visual_items[1]["subject_id"] if len(visual_items) > 1 else primary_subject_id]
        batch_payload = {
            "material_id": material_id,
            "subject_ids": batch_subject_ids,
            "model": "qwen-image",
            "size": "2k",
            "aspect": "16:9",
            "count": 1,
        }
        generation_batch = call(
            client,
            "POST",
            "/v3api/generation-jobs/batch",
            201,
            json=batch_payload,
            headers={"Idempotency-Key": idempotency_key("generation-batch")},
        )
        generation_batch_replay = call(
            client,
            "POST",
            "/v3api/generation-jobs/batch",
            201,
            json=batch_payload,
            headers={"Idempotency-Key": idempotency_key("generation-batch")},
        )
        assert generation_batch["failed"] == [] and generation_batch["skipped"] == []
        assert [item["job_id"] for item in generation_batch_replay["created"]] == [
            item["job_id"] for item in generation_batch["created"]
        ]
        assert len(generation_batch["created"]) == len(batch_subject_ids) == 1
        batch_generation_ids = {job["job_id"] for job in generation_batch["created"]}
        registry.generation_jobs.update(batch_generation_ids)
        for batch_job_id in batch_generation_ids:
            assert_job_artifacts(
                task_type="generation.generate_images",
                job_id=batch_job_id,
                reservation_source_type="generation_job",
            )
        dispatch_expected(
            script_id,
            {("generation.generate_images", job_id) for job_id in batch_generation_ids},
            registry=registry,
        )
        batch_generation_jobs: list[dict[str, Any]] = []
        for job in generation_batch["created"]:
            finished = poll_job(
                client,
                f"/v3api/generation-jobs/{job['job_id']}",
                timeout_seconds=900,
                label=f"generation-batch-{job['job_id']}",
            )
            assert finished["status"] == "succeeded", finished
            output = json_object(finished["output_json"], label="批量生图 output_json")["imageOutputs"][0]
            tracked_storage_keys.add(
                approve_generated_image(
                    client,
                    script_id=script_id,
                    subject_id=finished["subject_id"],
                    image_output=output,
                )
            )
            assert_media(client, output["storageKey"], label="真实批量生图")
            batch_generation_jobs.append(finished)

        paid_image_subject_ids = {primary_subject_id, *batch_subject_ids}
        for index, item in enumerate(visual_items):
            fixture_subject_id = item["subject_id"]
            if fixture_subject_id in paid_image_subject_ids:
                continue
            current = list_subjects(client, script_id)[fixture_subject_id]
            if current.get("image_entry"):
                continue
            fixture = call(
                client,
                "POST",
                f"/api/x/cctv/zhihui/flow-app/upload_images/{material_id}/fixture-{index}.png",
                params={"subject_id": fixture_subject_id},
                files={"file": (f"fixture-{index}.png", png_bytes(), "image/png")},
            )
            tracked_storage_keys.add(str(fixture["storage_key"]))
            approved_fixture = call(
                client,
                "POST",
                f"/v3api/scripts/{script_id}/asset-library/subjects/{fixture_subject_id}/approve-image",
                json={"candidate_id": fixture["asset_id"], "expected_image_entry_id": None},
            )
            assert approved_fixture["subject"]["image_entry"]["asset_id"] == fixture["asset_id"]

        generation_listing = call(
            client,
            "GET",
            "/v3api/generation-jobs",
            params={"material_id": material_id, "limit": 500},
        )
        listed_generation_jobs = {item["job_id"]: item for item in generation_listing["items"]}
        expected_generation_subjects = {
            generation["job_id"]: primary_subject_id,
            **{job["job_id"]: job["subject_id"] for job in batch_generation_jobs},
        }
        assert expected_generation_subjects.keys() <= listed_generation_jobs.keys()
        assert all(
            listed_generation_jobs[job_id]["subject_id"] == expected_subject_id
            for job_id, expected_subject_id in expected_generation_subjects.items()
        )

        for index, item in enumerate(audio_items):
            audio_upload = call(
                client,
                "POST",
                f"/api/x/cctv/zhihui/flow-app/upload_images/{material_id}/reference-{index}.wav",
                params={"subject_id": item["subject_id"]},
                files={
                    "file": (
                        f"reference-{index}.wav",
                        wav_bytes(),
                        "audio/wav",
                    )
                },
            )
            tracked_storage_keys.add(str(audio_upload["storage_key"]))
            approved_audio = call(
                client,
                "POST",
                f"/v3api/scripts/{script_id}/asset-library/subjects/{item['subject_id']}/approve-audio",
                json={"candidate_id": audio_upload["asset_id"]},
            )
            assert any(
                entry["asset_id"] == audio_upload["asset_id"] for entry in approved_audio["audio_entries"]
            )

        materials_ready = call(
            client,
            "GET",
            "/v3api/video-generation/check-materials",
            params={"script_id": script_id, "shot_group_id": first_group_id},
        )
        assert materials_ready == {"missing_subjects": [], "missing_audio": [], "ready": True}

        video_payload = {
            "script_id": script_id,
            "episode_id": episode_id,
            "shot_group_id": first_group_id,
        }
        active_provider = call(client, "GET", "/v3api/video-generation/active-provider")
        assert active_provider["provider_key"] == "volcengine"
        video = call(
            client,
            "POST",
            "/v3api/video-generation-jobs",
            201,
            json=video_payload,
            headers={"Idempotency-Key": idempotency_key("video-primary-a")},
        )
        registry.video_jobs.add(video["job_id"])
        video_same_key_replay = call(
            client,
            "POST",
            "/v3api/video-generation-jobs",
            201,
            json=video_payload,
            headers={"Idempotency-Key": idempotency_key("video-primary-a")},
        )
        assert video_same_key_replay["job_id"] == video["job_id"]
        assert_job_artifacts(
            task_type="video.generate",
            job_id=video["job_id"],
            reservation_source_type="generation_job",
        )
        assert_video_idempotency_request_count(video["job_id"], 1)
        active_replay = call(
            client,
            "POST",
            "/v3api/video-generation-jobs",
            201,
            json=video_payload,
            headers={"Idempotency-Key": idempotency_key("video-primary-b")},
        )
        assert active_replay["job_id"] == video["job_id"]
        assert_video_idempotency_request_count(video["job_id"], 2)
        batch_replay = call(
            client,
            "POST",
            "/v3api/video-generation-jobs/batch",
            201,
            json={"script_id": script_id, "episode_id": episode_id, "shot_group_ids": [first_group_id]},
            headers={"Idempotency-Key": idempotency_key("video-batch")},
        )
        assert batch_replay["success"][0]["job_id"] == video["job_id"]
        batch_same_key_replay = call(
            client,
            "POST",
            "/v3api/video-generation-jobs/batch",
            201,
            json={
                "script_id": script_id,
                "episode_id": episode_id,
                "shot_group_ids": [first_group_id],
            },
            headers={"Idempotency-Key": idempotency_key("video-batch")},
        )
        assert batch_same_key_replay["success"][0]["job_id"] == video["job_id"]
        assert_video_idempotency_request_count(video["job_id"], 3)
        assert_job_artifacts(
            task_type="video.generate",
            job_id=video["job_id"],
            reservation_source_type="generation_job",
        )
        assert video["provider_key"] == active_provider["provider_key"]
        video_input = video["input_json"]
        assert video_input["model"] == active_provider["model"]
        assert video["job_type"] == "all_reference"
        assert video_input["missing_subjects"] == [] and video_input["missing_audio"] == []
        assert len(video_input["reference_images"]) == len(video_input["reference_image_names"])
        assert len(video_input["reference_audios"]) == len(video_input["reference_audio_names"])
        assert video_input["reference_images"]
        assert all(
            reference["storage_key"]
            and reference["mime_type"].startswith("image/")
            and reference["size_bytes"] > 0
            and len(reference["sha256"]) == 64
            for reference in video_input["reference_images"]
        )
        dispatch_expected(
            script_id,
            {("video.generate", video["job_id"])},
            registry=registry,
        )
        video_with_remote_id = poll_until(
            client,
            f"/v3api/video-generation/{video['job_id']}",
            lambda body: bool((body.get("output_json") or {}).get("provider_task_id")),
            timeout_seconds=180,
            label="video-provider-task-success",
        )
        registry.register_remote_video_task(
            job_id=video["job_id"],
            provider_key=video_with_remote_id["provider_key"],
            provider_task_id=str(video_with_remote_id["output_json"]["provider_task_id"]),
            local_status=video_with_remote_id["status"],
        )
        video = poll_job(
            client,
            f"/v3api/video-generation/{video['job_id']}",
            timeout_seconds=900,
            label="video",
        )
        assert video["status"] == "succeeded", video
        video_output = json_object(video["output_json"], label="真实视频 output_json")
        success_provider_task_id = str(video_output.get("provider_task_id") or "")
        assert success_provider_task_id, "成功视频缺少火山远端 task ID"
        registry.register_remote_video_task(
            job_id=video["job_id"],
            provider_key=video["provider_key"],
            provider_task_id=success_provider_task_id,
            local_status=video["status"],
        )
        video_storage_key = video_output["video_storage_key"]
        tracked_storage_keys.add(video_storage_key)
        video_content = assert_media(client, video_storage_key, label="真实视频")
        video_duration = ffprobe_duration(video_content)
        assert video["metrics"]["reference_count"] == len(video_input["reference_images"])
        listing = call(
            client,
            "GET",
            "/v3api/video-generation-jobs",
            params={"script_id": script_id, "episode_id": episode_id},
        )
        assert any(item["job_id"] == video["job_id"] for item in listing["items"])

        refreshed_workbench = call(
            client,
            "GET",
            f"/v3api/shots/{script_id}/episodes/{episode_id}/video-workbench",
        )
        refreshed_groups = [
            group for scene in refreshed_workbench["scenes"] for group in scene["shot_groups"]
        ]
        refreshed_first = next(
            group for group in refreshed_groups if group["shot_group_id"] == first_group_id
        )
        last_frame = refreshed_first["reference_frames"]["last_frame"]
        assert last_frame
        tracked_storage_keys.add(last_frame)
        assert_media(client, last_frame, label="真实视频尾帧")

        same_scene_next = next(
            (
                group
                for group in refreshed_groups
                if group["scene_no"] == refreshed_first["scene_no"]
                and group["shot_group_id"] != first_group_id
            ),
            None,
        )
        assert same_scene_next is not None, (
            "真实分镜未产生同场景下一镜头组，无法在第二次付费视频调用前验证尾帧到首帧链"
        )
        cancel_group = same_scene_next
        cancel_video_payload = {
            "script_id": script_id,
            "episode_id": episode_id,
            "shot_group_id": cancel_group["shot_group_id"],
        }
        cancel_video = call(
            client,
            "POST",
            "/v3api/video-generation-jobs",
            201,
            json=cancel_video_payload,
            headers={"Idempotency-Key": idempotency_key("video-cancel")},
        )
        registry.video_jobs.add(cancel_video["job_id"])
        cancel_input = cancel_video["input_json"]
        if same_scene_next is not None:
            assert cancel_input["first_frame"] == last_frame
        dispatch_expected(
            script_id,
            {("video.generate", cancel_video["job_id"])},
            registry=registry,
        )
        cancel_video = poll_until(
            client,
            f"/v3api/video-generation/{cancel_video['job_id']}",
            lambda body: (
                body.get("status") == "processing"
                and bool((body.get("output_json") or {}).get("provider_task_id"))
            ),
            timeout_seconds=180,
            label="video-provider-task",
        )
        cancel_provider_task_id = str(cancel_video["output_json"]["provider_task_id"])
        registry.register_remote_video_task(
            job_id=cancel_video["job_id"],
            provider_key=cancel_video["provider_key"],
            provider_task_id=cancel_provider_task_id,
            local_status=cancel_video["status"],
        )
        cancel_video = call(
            client,
            "POST",
            f"/v3api/video-generation/{cancel_video['job_id']}/cancel",
        )
        assert cancel_video["status"] == "canceled"
        provider_cancellation = json_object(
            (cancel_video.get("output_json") or {}).get("provider_cancellation"),
            label="真实视频远端取消结果",
        )
        assert provider_cancellation["provider_task_id"] == cancel_provider_task_id
        persisted_cancellation = await_final_provider_cancellation(
            registry=registry,
            job_id=cancel_video["job_id"],
            provider_task_id=cancel_provider_task_id,
            initial=provider_cancellation,
        )
        assert int(persisted_cancellation["attempts"]) >= int(provider_cancellation["attempts"])
        assert registry.remote_video_tasks[cancel_video["job_id"]].terminal, (
            asdict(registry.remote_video_tasks[cancel_video["job_id"]])
        )
        assert active_outbox_rows(cancel_video["job_id"]) == []

        first_arrangement = call(
            client,
            "POST",
            "/v3api/video-timeline-arrangements",
            201,
            params={"script_id": script_id, "episode_id": episode_id},
            json={"unit_runtime_key": unit_runtime_key, "job_id": video["job_id"]},
        )
        trim_midpoint = video_duration / 2
        first_arrangement = call(
            client,
            "PATCH",
            f"/v3api/video-timeline-arrangements/{first_arrangement['id']}",
            json={
                "trim_start_seconds": 0.1,
                "trim_end_seconds": trim_midpoint,
                "expected_version": first_arrangement["version"],
            },
        )
        second_arrangement = call(
            client,
            "POST",
            "/v3api/video-timeline-arrangements",
            201,
            params={"script_id": script_id, "episode_id": episode_id},
            json={"unit_runtime_key": unit_runtime_key, "job_id": video["job_id"]},
        )
        second_arrangement = call(
            client,
            "PATCH",
            f"/v3api/video-timeline-arrangements/{second_arrangement['id']}",
            json={
                "is_selected": False,
                "trim_start_seconds": trim_midpoint,
                "trim_end_seconds": video_duration - 0.1,
                "expected_version": second_arrangement["version"],
            },
        )
        second_arrangement = call(
            client,
            "PATCH",
            f"/v3api/video-timeline-arrangements/{second_arrangement['id']}",
            json={"is_selected": True, "expected_version": second_arrangement["version"]},
        )
        reordered = call(
            client,
            "PUT",
            "/v3api/video-timeline-arrangements/reorder",
            params={"script_id": script_id, "episode_id": episode_id},
            json={
                "items": [
                    {
                        "id": second_arrangement["id"],
                        "row_order": 0,
                        "expected_version": second_arrangement["version"],
                    },
                    {
                        "id": first_arrangement["id"],
                        "row_order": 1,
                        "expected_version": first_arrangement["version"],
                    },
                ]
            },
        )
        assert [item["id"] for item in reordered["items"]] == [
            second_arrangement["id"],
            first_arrangement["id"],
        ]
        disposable_arrangement = call(
            client,
            "POST",
            "/v3api/video-timeline-arrangements",
            201,
            params={"script_id": script_id, "episode_id": episode_id},
            json={"unit_runtime_key": unit_runtime_key, "job_id": video["job_id"]},
        )
        call(client, "DELETE", f"/v3api/video-timeline-arrangements/{disposable_arrangement['id']}", 204)
        timeline = call(
            client,
            "GET",
            "/v3api/video-timeline-arrangements",
            params={"script_id": script_id, "episode_id": episode_id},
        )
        assert [item["id"] for item in timeline["items"]] == [
            second_arrangement["id"],
            first_arrangement["id"],
        ]

        render_payload = {"script_id": script_id, "episode_id": episode_id}
        render = call(
            client,
            "POST",
            "/v3api/video-timeline-arrangements/render",
            202,
            json=render_payload,
            headers={"Idempotency-Key": idempotency_key("timeline-primary")},
        )
        registry.render_jobs.add(render["job_id"])
        render_replay = call(
            client,
            "POST",
            "/v3api/video-timeline-arrangements/render",
            202,
            json=render_payload,
            headers={"Idempotency-Key": idempotency_key("timeline-primary")},
        )
        assert render_replay["job_id"] == render["job_id"]
        assert render_replay.get("output_storage_key") == render.get("output_storage_key")
        assert_job_artifacts(
            task_type="timeline.render",
            job_id=render["job_id"],
            reservation_source_type="timeline_render_job",
        )
        render_snapshot = render_job_snapshot(render["job_id"])
        segments = list(render_snapshot.input_json["segments"])
        assert [segment["row_order"] for segment in segments] == [0, 1]
        assert [segment["arrangement_id"] for segment in segments] == [
            second_arrangement["id"],
            first_arrangement["id"],
        ]
        dispatch_expected(
            script_id,
            {("timeline.render", render["job_id"])},
            registry=registry,
        )
        render = poll_job(
            client,
            f"/v3api/video-timeline-arrangements/render/{render['job_id']}",
            timeout_seconds=600,
            label="timeline",
        )
        assert render["status"] == "succeeded", render
        render_storage_key = render.get("storage_key") or render.get("output_storage_key")
        assert render_storage_key
        tracked_storage_keys.add(render_storage_key)
        render_content = assert_media(client, render_storage_key, label="时间线两片段成片")
        rendered_duration = ffprobe_duration(render_content)
        expected_duration = video_duration - 0.2
        assert abs(rendered_duration - expected_duration) <= max(0.6, expected_duration * 0.15)

        extra_arrangements: list[dict[str, Any]] = []
        for index in range(24):
            extra_arrangements.append(
                call(
                    client,
                    "POST",
                    "/v3api/video-timeline-arrangements",
                    201,
                    params={"script_id": script_id, "episode_id": episode_id},
                    json={"unit_runtime_key": unit_runtime_key, "job_id": video["job_id"]},
                )
            )
        cancel_render = call(
            client,
            "POST",
            "/v3api/video-timeline-arrangements/render",
            202,
            json=render_payload,
            headers={"Idempotency-Key": idempotency_key("timeline-cancel")},
        )
        registry.render_jobs.add(cancel_render["job_id"])
        dispatch_expected(
            script_id,
            {("timeline.render", cancel_render["job_id"])},
            registry=registry,
        )
        cancel_render = poll_until(
            client,
            f"/v3api/video-timeline-arrangements/render/{cancel_render['job_id']}",
            lambda body: body.get("status") == "processing",
            timeout_seconds=60,
            label="timeline-running",
            interval_seconds=0.1,
        )
        cancel_render = call(
            client,
            "POST",
            f"/v3api/video-timeline-arrangements/render/{cancel_render['job_id']}/cancel",
        )
        assert cancel_render["status"] == "canceled"
        assert active_outbox_rows(cancel_render["job_id"]) == []
        call(
            client,
            "POST",
            f"/v3api/video-timeline-arrangements/render/{cancel_render['job_id']}/retry",
            409,
        )
        for item in extra_arrangements:
            call(
                client,
                "DELETE",
                f"/v3api/video-timeline-arrangements/{item['id']}",
                204,
            )

        video_path = Path(settings.storage_root) / video_storage_key.lstrip("/")
        assert settings.storage_driver == "local" and video_path.is_file()
        failed_render = call(
            client,
            "POST",
            "/v3api/video-timeline-arrangements/render",
            202,
            json=render_payload,
            headers={"Idempotency-Key": idempotency_key("timeline-failed")},
        )
        registry.render_jobs.add(failed_render["job_id"])
        video_backup = video_path.read_bytes()
        video_path.unlink()
        try:
            dispatch_expected(
                script_id,
                {("timeline.render", failed_render["job_id"])},
                registry=registry,
            )
            failed_render = poll_job(
                client,
                f"/v3api/video-timeline-arrangements/render/{failed_render['job_id']}",
                timeout_seconds=120,
                label="timeline-intentional-failure",
            )
            assert failed_render["status"] == "failed" and failed_render["retryable"] is True
        finally:
            restore_path = video_path.with_name(video_path.name + ".restore")
            restore_path.write_bytes(video_backup)
            restore_path.replace(video_path)

        retry_render = call(
            client,
            "POST",
            f"/v3api/video-timeline-arrangements/render/{failed_render['job_id']}/retry",
            202,
        )
        registry.render_jobs.add(retry_render["job_id"])
        dispatch_expected(
            script_id,
            {("timeline.render", retry_render["job_id"])},
            registry=registry,
        )
        retry_render = poll_job(
            client,
            f"/v3api/video-timeline-arrangements/render/{retry_render['job_id']}",
            timeout_seconds=600,
            label="timeline-retry",
        )
        assert retry_render["status"] == "succeeded", retry_render
        retry_render_storage_key = retry_render.get("storage_key") or retry_render.get("output_storage_key")
        assert retry_render_storage_key
        tracked_storage_keys.add(retry_render_storage_key)
        assert_media(client, retry_render_storage_key, label="时间线失败重试成片")

        accounts = call(client, "GET", "/v3api/credits/accounts")["items"]
        owner_account = next(item for item in accounts if item["id"] == OWNER_CREDIT_ID)
        assert int(owner_account["frozen_points"]) == 0, owner_account
        logs = call(
            client,
            "GET",
            "/v3api/credits/logs",
            params={"account_id": OWNER_CREDIT_ID, "limit": 500},
        )
        credit_logs = logs["items"]
        expected_credit_lifecycles = {
            ("pipeline", initial_pipeline[1]): ("settle", None, None),
            ("pipeline", manual["run_id"]): ("settle", OWNER_ID, None),
            ("storyboard", storyboard["job_id"]): ("settle", OWNER_ID, OWNER_ID),
            ("generation_job", canceled_generation["job_id"]): (
                "release",
                OWNER_ID,
                OWNER_ID,
            ),
            ("generation_job", generation["job_id"]): ("settle", OWNER_ID, None),
            **{
                ("generation_job", job["job_id"]): ("settle", OWNER_ID, None)
                for job in batch_generation_jobs
            },
            ("generation_job", video["job_id"]): ("settle", OWNER_ID, OWNER_ID),
            ("generation_job", cancel_video["job_id"]): ("release", OWNER_ID, OWNER_ID),
            ("timeline_render_job", render["job_id"]): ("settle", OWNER_ID, OWNER_ID),
            ("timeline_render_job", cancel_render["job_id"]): (
                "release",
                OWNER_ID,
                OWNER_ID,
            ),
            ("timeline_render_job", failed_render["job_id"]): (
                "release",
                OWNER_ID,
                OWNER_ID,
            ),
            ("timeline_render_job", retry_render["job_id"]): (
                "settle",
                OWNER_ID,
                OWNER_ID,
            ),
        }
        assert set(expected_credit_lifecycles) == registry.reservation_sources
        run_credit_logs = [
            item
            for item in credit_logs
            if (item.get("source_type"), item.get("source_id")) in expected_credit_lifecycles
        ]
        assert len(run_credit_logs) == 2 * len(expected_credit_lifecycles)
        credit_lifecycle_evidence = [
            assert_credit_lifecycle(
                credit_logs,
                source_type,
                source_id,
                terminal_type,
                expected_reserve_actor_user_id=reserve_actor_user_id,
                expected_terminal_actor_user_id=terminal_actor_user_id,
            )
            for (
                source_type,
                source_id,
            ), (
                terminal_type,
                reserve_actor_user_id,
                terminal_actor_user_id,
            ) in sorted(expected_credit_lifecycles.items())
        ]
        settle_points_delta = sum(item["settle_points_delta"] for item in credit_lifecycle_evidence)
        assert int(owner_account["balance_points"]) == owner_balance_before + settle_points_delta

        PAID_BUDGET.assert_business_final()
        call(client, "DELETE", f"/v3api/scripts/{script_id}", 409)
        _discover_active_script_jobs(registry)
        remote_cleanup = reconcile_registered_remote_video_tasks(registry)
        verify_signed_payload(remote_cleanup, label="remote video cleanup")
        assert remote_cleanup["status"] == "settled" and remote_cleanup["settled"] is True, (
            remote_cleanup
        )
        predelete_gate = cleanup_gate_snapshot(
            client,
            registry,
            require_script_missing=False,
            require_storage_empty=False,
            require_media_quiet=False,
        )
        assert predelete_gate["completed"], predelete_gate
        deleted = call(client, "DELETE", f"/v3api/scripts/{script_id}", 200, params={"confirm": "true"})
        assert deleted["detail"] == "剧本已删除"
        assert_no_active_outbox()
        recovery_runs: list[dict[str, Any]] = []
        media_recovery_stop_reason: str | None = None
        for _ in range(20):
            recovery = send_task_with_runtime_gate(
                registry,
                "app.workers.media_storage.recover_media_storage_jobs",
                purpose="media_recovery",
                dispatch_attempts=frozenset(),
                kwargs={"limit": 100},
            )
            recovery_result = recovery.get(timeout=120)
            register_async_result_tree(recovery, ledger=PAID_BUDGET)
            assert_known_backend_keys()
            recovery_runs.append(recovery_result)
            assert recovery_result["retried"] == [] and recovery_result["failed"] == []
            if media_storage_active_count() == 0:
                media_recovery_stop_reason = "active_count_zero"
                break
            time.sleep(0.5)
        assert media_storage_active_count() == 0
        assert active_outbox_count() == 0
        call(client, "GET", f"/v3api/scripts/{script_id}", 404)
        registry_integrity = registry_integrity_snapshot(registry, PAID_BUDGET)
        final_cleanup_gate = cleanup_gate_snapshot(
            client,
            registry,
            require_script_missing=True,
            require_storage_empty=True,
            require_media_quiet=True,
            require_worker_absent=True,
            registry_report=registry_integrity,
            redis_baseline=redis_baseline,
        )
        assert final_cleanup_gate["completed"], final_cleanup_gate
        PAID_BUDGET.assert_runtime_audit()
        assert media_recovery_stop_reason is not None
        final_budget_summary = PAID_BUDGET.assert_final(
            media_recovery_runs=len(recovery_runs),
            media_recovery_stop_reason=media_recovery_stop_reason,
            remote_task_count=len(registry.remote_video_tasks),
        )
        dispatch_integrity = assert_dispatch_integrity(
            PAID_BUDGET,
            media_recovery_runs=len(recovery_runs),
            media_recovery_stop_reason=media_recovery_stop_reason,
            remote_task_count=len(registry.remote_video_tasks),
        )
        assert all(
            dispatch_integrity[key] == value
            for key, value in final_budget_summary.items()
        )

        async_happy = {
            (event["method"], event["template"])
            for event in EVENTS
            if event.get("template") is not None and 200 <= event["status"] < 300
        }
        live_happy = parse_operation_set(
            live_result["operation_coverage"]["happy_path_set"],
            label="HTTP happy_path_set",
        )
        assert live_happy <= recorded_operations == OPENAPI_OPERATIONS
        assert async_happy <= OPENAPI_OPERATIONS
        combined_happy = live_happy | async_happy
        combined_missing_happy = OPENAPI_OPERATIONS - combined_happy
        assert combined_missing_happy <= {("POST", "/v3api/video-generation/active-provider")}

        registry.ledger_sealed = True
        try:
            ledger_verification = verify_remote_ledger_settlement(
                REMOTE_LEDGER_PATH,
                script_id=script_id,
                expected_tasks=registry.remote_video_tasks,
                settled_report=remote_cleanup,
            )
        except BaseException as exc:
            running_claim = read_signed_json(ASYNC_RESULT_PATH, label="async preflight claim")
            replace_signed_claim(
                ASYNC_RESULT_PATH,
                {
                    "status": "failed",
                    "run_id": VALIDATION_RUN_ID,
                    "started_at": running_claim["started_at"],
                    "finished_at": datetime.now(UTC).isoformat(),
                    "failure_stage": "remote_ledger_verification",
                    "error_type": type(exc).__name__,
                    "remote_ledger_preserved": True,
                    "worker_start_authorization": running_claim.get(
                        "worker_start_authorization"
                    ),
                    "preflight": running_claim.get("preflight"),
                    "dispatch_delivery_ledger": running_claim.get(
                        "dispatch_delivery_ledger",
                        [],
                    ),
                },
            )
            raise

        finished_at = datetime.now(UTC)
        expected_ledger_delete = {
            "deleted": True,
            "sha256": ledger_verification["ledger_sha256"],
            "file_identity": dict(ledger_verification["ledger_file_identity"]),
            "fd_held_through_unlink": True,
            "quarantine_dirfd_held_through_unlink": True,
            "quarantine_directory_unlinked_verified": True,
            "same_user_directory_adversary": "fail_closed_on_detected_identity_change",
            "pre_unlink_link_count": 1,
            "post_unlink_link_count": 0,
        }
        dispatch_claim = read_signed_json(ASYNC_RESULT_PATH, label="final dispatch claim")
        dispatch_delivery_ledger = dispatch_claim.get("dispatch_delivery_ledger")
        assert isinstance(dispatch_delivery_ledger, list)
        assert all(record.get("state") == "confirmed" for record in dispatch_delivery_ledger)
        assert all(record.get("retry_allowed") is False for record in dispatch_delivery_ledger)
        assert {
            str(record["task_id"]) for record in dispatch_delivery_ledger
        } == PAID_BUDGET.celery_dispatch_task_ids
        persisted_business_attempts = [
            attempt
            for record in dispatch_delivery_ledger
            if record.get("purpose") == "business_outbox"
            for attempt in record.get("dispatch_attempts", [])
        ]
        expected_business_attempts = [
            {"task_type": task_type, "job_id": job_id, "attempt": attempt}
            for (task_type, job_id), attempt in sorted(PAID_BUDGET.dispatch_attempts.items())
        ]
        assert sorted(
            persisted_business_attempts,
            key=lambda item: (item["task_type"], item["job_id"], item["attempt"]),
        ) == expected_business_attempts
        assert all(
            record.get("dispatch_attempts") == []
            for record in dispatch_delivery_ledger
            if record.get("purpose") != "business_outbox"
        )
        output = {
            "status": "finalizing",
            "run_id": VALIDATION_RUN_ID,
            "started_at": workflow_started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "seed_manifest_sha256": seed_manifest["sha256"],
            "http_result_sha256": live_result["sha256"],
            "pipeline_stages": len(pipeline["stages"]),
            "subject_count": len(subjects),
            "manual_pipeline_run_id": manual["run_id"],
            "resolved_suggestion_id": resolved_suggestion["suggestion_id"],
            "generation_job_id": generation["job_id"],
            "batch_generation_job_ids": [job["job_id"] for job in batch_generation_jobs],
            "storyboard_job_id": storyboard["job_id"],
            "scene_count": len(hierarchy["scenes"]),
            "shot_group_count": len(groups),
            "shot_count": len(shots),
            "video_job_id": video["job_id"],
            "video_storage_key": video_storage_key,
            "video_duration_seconds": video_duration,
            "running_canceled_video_job_id": cancel_video["job_id"],
            "first_frame_chain_verified": same_scene_next is not None,
            "render_job_id": render["job_id"],
            "render_storage_key": render_storage_key,
            "render_duration_seconds": rendered_duration,
            "running_canceled_render_job_id": cancel_render["job_id"],
            "failed_render_job_id": failed_render["job_id"],
            "retry_render_job_id": retry_render["job_id"],
            "tracked_storage_key_count": len(tracked_storage_keys),
            "media_recovery_runs": recovery_runs,
            "offline_guard_self_tests": offline_guard_evidence,
            "signed_worker_start_authorization": worker_start_authorization,
            "signed_async_preflight": async_preflight,
            "isolated_worker": worker_evidence,
            "paid_budget": {
                "worker_prerequisites_verified": PAID_BUDGET.worker_prerequisites_verified,
                "workflow_prerequisites_verified": PAID_BUDGET.workflow_prerequisites_verified,
                "text_prerequisites_verified": PAID_BUDGET.text_prerequisites_verified,
                "media_prerequisites_verified": PAID_BUDGET.media_prerequisites_verified,
                "limits": dict(PAID_BUDGET.LIMITS),
                "business_dispatch_attempts": [
                    {"task_type": task_type, "job_id": job_id, "attempt": attempt}
                    for (task_type, job_id), attempt in sorted(PAID_BUDGET.dispatch_attempts.items())
                ],
                "celery_dispatch_task_ids": sorted(PAID_BUDGET.celery_dispatch_task_ids),
                "celery_dispatch_records": list(PAID_BUDGET.celery_dispatch_records),
                "owned_backend_key_count": len(PAID_BUDGET.owned_backend_keys),
                "runtime_gate_count": len(PAID_BUDGET.runtime_gate_evidence),
                "runtime_gates": PAID_BUDGET.runtime_gate_evidence,
                "evidence_scope": "all script-initiated Celery business and recovery dispatches",
                "provider_sdk_internal_http_retries": "not_observable",
            },
            "dispatch_integrity": dispatch_integrity,
            "dispatch_delivery_ledger": dispatch_delivery_ledger,
            "credit_reconciliation": {
                "account_id": OWNER_CREDIT_ID,
                "balance_before": owner_balance_before,
                "frozen_before": owner_frozen_before,
                "balance_after": int(owner_account["balance_points"]),
                "settle_points_delta": settle_points_delta,
                "frozen_after": int(owner_account["frozen_points"]),
                "lifecycles": credit_lifecycle_evidence,
            },
            "remote_video_cleanup": remote_cleanup,
            "remote_ledger_verification": ledger_verification,
            "remote_ledger_sha256": ledger_verification["ledger_sha256"],
            "settled_remote_report_sha256": ledger_verification["settled_report_sha256"],
            "remote_ledger_delete": {
                "deleted": False,
                "state": "pending_verified_delete",
                "expected_sha256": ledger_verification["ledger_sha256"],
                "expected_file_identity": ledger_verification["ledger_file_identity"],
            },
            "predelete_cleanup_gate": predelete_gate,
            "registry_integrity": registry_integrity,
            "final_cleanup_gate": final_cleanup_gate,
            "combined_happy_operation_coverage": {
                "covered": len(combined_happy),
                "total": len(OPENAPI_OPERATIONS),
                "operation_set": sorted(f"{method} {path}" for method, path in OPENAPI_OPERATIONS),
                "http_happy_set": sorted(f"{method} {path}" for method, path in live_happy),
                "async_happy_set": sorted(f"{method} {path}" for method, path in async_happy),
                "combined_happy_set": sorted(f"{method} {path}" for method, path in combined_happy),
                "missing": sorted(f"{method} {path}" for method, path in combined_missing_happy),
            },
            "event_count": len(EVENTS),
            "finalization": {
                "settled": True,
                "remote_ledger_sha256": ledger_verification["ledger_sha256"],
                "remote_ledger_file_identity": ledger_verification[
                    "ledger_file_identity"
                ],
                "remote_ledger_delete_state": "pending",
            },
        }
        output = replace_signed_claim(ASYNC_RESULT_PATH, output)
        assert read_signed_json(ASYNC_RESULT_PATH, label="finalizing async result") == output
        delete_completed = False
        try:
            verified_ledger = verify_signed_file_identity(
                REMOTE_LEDGER_PATH,
                label="passed remote video ledger",
                expected_sha256=ledger_verification["ledger_sha256"],
                expected_identity=ledger_verification["ledger_file_identity"],
            )
            assert verified_ledger["sha256"] == ledger_verification["ledger_sha256"]
            actual_ledger_delete = delete_verified_signed_file(
                REMOTE_LEDGER_PATH,
                label="passed remote video ledger",
                expected_sha256=ledger_verification["ledger_sha256"],
                expected_identity=ledger_verification["ledger_file_identity"],
            )
            assert actual_ledger_delete == expected_ledger_delete
            delete_completed = True
            delete_artifacts = _verified_delete_artifact_paths(REMOTE_LEDGER_PATH)
            assert not REMOTE_LEDGER_PATH.exists() and not REMOTE_LEDGER_PATH.is_symlink()
            assert all(
                not artifact.exists() and not artifact.is_symlink()
                for artifact in delete_artifacts.values()
            )
            passed_output = dict(output)
            passed_output.pop("sha256", None)
            passed_output["status"] = "passed"
            passed_output["finished_at"] = datetime.now(UTC).isoformat()
            passed_output["remote_ledger_delete"] = actual_ledger_delete
            passed_output["finalization"] = {
                **passed_output["finalization"],
                "remote_ledger_delete_state": "verified_deleted",
            }
            output = replace_signed_claim(ASYNC_RESULT_PATH, passed_output)
            assert read_signed_json(ASYNC_RESULT_PATH, label="passed async result") == output
        except BaseException as exc:
            delete_artifacts = _verified_delete_artifact_paths(REMOTE_LEDGER_PATH)
            failed_output = dict(output)
            failed_output.pop("sha256", None)
            failed_output.update(
                {
                    "status": "failed",
                    "finished_at": datetime.now(UTC).isoformat(),
                    "failure_stage": (
                        "final_pass_commit" if delete_completed else "remote_ledger_delete"
                    ),
                    "error_type": type(exc).__name__,
                    "remote_ledger_preserved": (
                        REMOTE_LEDGER_PATH.exists()
                        or REMOTE_LEDGER_PATH.is_symlink()
                        or any(
                            artifact.exists() or artifact.is_symlink()
                            for artifact in delete_artifacts.values()
                        )
                    ),
                    "remote_ledger_delete": {
                        "deleted": delete_completed,
                        "expected_sha256": ledger_verification["ledger_sha256"],
                        "expected_file_identity": ledger_verification[
                            "ledger_file_identity"
                        ],
                    },
                }
            )
            try:
                replace_signed_claim(ASYNC_RESULT_PATH, failed_output)
            except BaseException as failure_write_error:
                exc.add_note(
                    f"failed claim 持久化失败，保留上一签名状态: "
                    f"{type(failure_write_error).__name__}"
                )
            exc.add_note("最终化失败，async result 未提前宣称成功")
            raise
        registry.success = True
        print(json.dumps(output, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
