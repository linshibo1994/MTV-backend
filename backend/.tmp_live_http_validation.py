from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import re
import shlex
import stat
import subprocess
import sys
import tempfile
import time
import wave
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx
from redis import Redis

BASE_URL = "http://127.0.0.1:18080"
SELF_TEST_MODE = sys.argv[1:] == ["--self-test"]
RUN_ID_PATTERN = re.compile(
    r"20\d{6}-[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)
VALIDATION_TIMEZONE = ZoneInfo("Asia/Shanghai")
EXPECTED_REDIS_HOST = "127.0.0.1"
EXPECTED_REDIS_PORT = 56380
EXPECTED_CELERY_APP = "app.workers.celery_app"
HTTP_STABLE_WINDOW_SECONDS = 5.25
EXPECTED_PATH_COUNT = 65
EXPECTED_OPERATION_COUNT = 79


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
RESULT_PATH = ARTIFACT_PATHS["http_result"]
OWNER_LOGIN = {"username": SEED_IDS["users"]["owner"]["username"], "password": "MtvValidation!2026"}
MEMBER_LOGIN = {"username": SEED_IDS["users"]["member"]["username"], "password": "MtvValidation!2026"}
ADMIN_LOGIN = {"username": SEED_IDS["users"]["admin"]["username"], "password": "MtvValidation!2026"}
FOREIGN_LOGIN = {"username": SEED_IDS["users"]["foreign"]["username"], "password": "MtvValidation!2026"}
ORG_ID = SEED_IDS["organizations"]["primary"]
FOREIGN_ORG_ID = SEED_IDS["organizations"]["foreign"]
OWNER_ID = SEED_IDS["users"]["owner"]["user_id"]
MEMBER_ID = SEED_IDS["users"]["member"]["user_id"]
ADMIN_ID = SEED_IDS["users"]["admin"]["user_id"]
ORG_CREDIT_ID = SEED_IDS["credit_accounts"]["org"]
OWNER_CREDIT_ID = SEED_IDS["credit_accounts"]["owner"]
MEMBER_CREDIT_ID = SEED_IDS["credit_accounts"]["member"]
NODE_A_ID = f"node-a-{RUN_HASH}"
NODE_B_ID = f"node-b-{RUN_HASH}"
EDGE_ID = f"edge-{RUN_HASH}"
RESULTS: list[dict[str, Any]] = []
OPENAPI_OPERATIONS: set[tuple[str, str]] = set()
OPENAPI_SPEC: dict[str, Any] = {}


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


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_signed_exclusive(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    result = signed_payload(payload)
    _write_bytes_exclusive(path, json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8"))
    _fsync_directory(path.parent)
    return result


def replace_signed_claim(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    assert stat.S_ISREG(path.lstat().st_mode), "HTTP 批次 claim 不是普通文件"
    temporary = path.with_suffix(path.suffix + ".tmp")
    result = write_signed_exclusive(temporary, payload)
    os.replace(temporary, path)
    _fsync_directory(path.parent)
    return result


def load_verified_seed_manifest(path: Path, *, http_started_at: datetime) -> dict[str, Any]:
    assert path.is_file() and not path.is_symlink(), "seed manifest 缺失或是符号链接"
    temporary = path.with_suffix(path.suffix + ".tmp")
    assert not temporary.exists() and not temporary.is_symlink(), "seed manifest 有未完成临时文件"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    verify_signed_payload(payload, label="seed manifest")
    assert payload.get("status") == "passed"
    assert payload.get("run_id") == VALIDATION_RUN_ID
    assert payload.get("seed_ids") == SEED_IDS
    started_at = datetime.fromisoformat(str(payload["started_at"]))
    finished_at = datetime.fromisoformat(str(payload["finished_at"]))
    assert started_at.tzinfo is not None and finished_at.tzinfo is not None
    assert started_at <= finished_at <= http_started_at
    assert (http_started_at - finished_at).total_seconds() <= 4 * 60 * 60, "seed manifest 已过期"
    fingerprint = payload.get("fingerprint")
    assert isinstance(fingerprint, dict)
    assert fingerprint.get("server", {}).get("api_base_url") == BASE_URL
    database = fingerprint.get("database", {})
    assert database.get("host") == "127.0.0.1" and database.get("port") == 55438
    assert database.get("database") == "mtv_validation" and database.get("user") == "mtv_validation"
    return payload


def claim_http_result(started_at: datetime) -> dict[str, Any]:
    for label in ("http_result", "async_result", "remote_ledger"):
        path = ARTIFACT_PATHS[label]
        assert not path.exists() and not path.is_symlink(), f"本 nonce 已有 {label}，拒绝复用"
        temporary = path.with_suffix(path.suffix + ".tmp")
        assert not temporary.exists() and not temporary.is_symlink(), f"本 nonce 有未完成 {label}"
    return write_signed_exclusive(
        RESULT_PATH,
        {
            "status": "running",
            "run_id": VALIDATION_RUN_ID,
            "started_at": started_at.isoformat(),
            "finished_at": None,
        },
    )


def parse_redis_target(label: str, raw_url: str, expected_db: int) -> dict[str, Any]:
    parsed = urlsplit(raw_url)
    assert parsed.scheme == "redis", f"{label} scheme 非法"
    assert parsed.username is None and parsed.password is None, f"{label} 禁止用户名或密码"
    assert not parsed.query and not parsed.fragment, f"{label} 禁止 query/fragment"
    assert parsed.netloc == f"{EXPECTED_REDIS_HOST}:{EXPECTED_REDIS_PORT}", f"{label} authority 非法"
    assert parsed.hostname == EXPECTED_REDIS_HOST and parsed.port == EXPECTED_REDIS_PORT
    assert parsed.path == f"/{expected_db}", f"{label} 必须使用 Redis DB {expected_db}"
    return {"scheme": parsed.scheme, "host": parsed.hostname, "port": parsed.port, "db": expected_db}


def isolated_redis_urls(environ: dict[str, str] | os._Environ[str]) -> dict[str, str]:
    raw = {
        "app": environ.get("REDIS_URL", ""),
        "broker": environ.get("CELERY_BROKER_URL", ""),
        "backend": environ.get("CELERY_RESULT_BACKEND", ""),
    }
    parsed = {
        "app": parse_redis_target("应用 Redis", raw["app"], 0),
        "broker": parse_redis_target("Celery broker", raw["broker"], 1),
        "backend": parse_redis_target("Celery result backend", raw["backend"], 2),
    }
    assert len({(item["scheme"], item["host"], item["port"]) for item in parsed.values()}) == 1
    return raw


def parse_ps_processes(output: str) -> list[dict[str, Any]]:
    """解析固定 ps 格式；任何坏行都失败关闭，避免静默漏掉 Celery 进程。"""
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
    """从进程表识别 worker/beat；命令证据只输出摘要，不泄露参数。"""
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
        assert not celery_processes and not roots, "HTTP 阶段禁止 Celery worker"
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


def _empty_redis_snapshot(urls: dict[str, str]) -> dict[str, Any]:
    sizes: dict[str, int] = {}
    for label, raw_url in urls.items():
        client: Redis | None = None
        try:
            client = Redis.from_url(raw_url, decode_responses=True)
            assert client.ping() is True, f"{label} Redis PING 失败"
            sizes[label] = int(client.dbsize())
            assert sizes[label] == 0, f"{label} Redis DB 不是空实例"
            if label == "broker":
                assert not any(
                    client.type(key) == "list" and int(client.llen(key)) > 0
                    for key in client.scan_iter()
                ), "Celery broker 出现排队任务"
                assert int(client.hlen("unacked")) == 0
                assert int(client.zcard("unacked_index")) == 0
        finally:
            if client is not None:
                client.close()
    return {"captured_at": datetime.now(UTC).isoformat(), "redis_db_sizes": sizes}


def assert_http_phase_isolation(*, stage: str) -> dict[str, Any]:
    """跨越生产 5 秒调度周期证明 HTTP 阶段无 worker、beat 或 broker 投递。"""
    assert stage in {"start", "end"}
    urls = isolated_redis_urls(os.environ)
    started_at = datetime.now(UTC)
    first_process = collect_celery_process_evidence(expect_worker=False)
    first_redis = _empty_redis_snapshot(urls)
    time.sleep(HTTP_STABLE_WINDOW_SECONDS)
    second_process = collect_celery_process_evidence(expect_worker=False)
    second_redis = _empty_redis_snapshot(urls)
    return {
        "stage": stage,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "stable_window_seconds": HTTP_STABLE_WINDOW_SECONDS,
        "first": {"process": first_process, "redis": first_redis},
        "second": {"process": second_process, "redis": second_redis},
    }


def is_application_error_contract(status: int, body: Any) -> bool:
    if not (400 <= status < 500):
        return False
    if not (
        isinstance(body, dict)
        and set(body) == {"detail"}
        and isinstance(body["detail"], str)
        and bool(body["detail"].strip())
    ):
        return False
    return not (status == 404 and body["detail"] == "Not Found")


def is_handler_evidence(status: int, body: Any) -> bool:
    """只把明确进入应用 handler 的响应计为 Route 证据。"""
    if status in {401, 405} or status >= 500:
        return False
    if 200 <= status < 400:
        return True
    return is_application_error_contract(status, body)


def path_matches_template(template: str, actual_path: str) -> bool:
    cursor = 0
    pieces: list[str] = ["^"]
    for match in re.finditer(r"\{([^}]+)\}", template):
        pieces.append(re.escape(template[cursor : match.start()]))
        pieces.append(r".+" if match.group(1) == "storage_key" else r"[^/]+")
        cursor = match.end()
    pieces.extend((re.escape(template[cursor:]), "$"))
    return re.fullmatch("".join(pieces), actual_path) is not None


def operation_template(method: str, actual_path: str) -> str | None:
    """将真实请求路径映射回 OpenAPI 模板，静态路径优先于参数路径。"""
    candidates = sorted(
        (path for candidate_method, path in OPENAPI_OPERATIONS if candidate_method == method),
        key=lambda path: (path.count("{"), -len(path)),
    )
    for template in candidates:
        if path_matches_template(template, actual_path):
            return template
    return None


def record(
    label: str,
    response: httpx.Response,
    expected: set[int],
    *,
    coverage: str = "behavior",
    operation: tuple[str, str] | None = None,
) -> Any:
    body: Any
    try:
        body = response.json()
    except ValueError:
        body = None
    method = response.request.method
    actual_path = response.request.url.path
    template = operation_template(method, actual_path)
    if operation is not None:
        assert operation[0] == method and path_matches_template(operation[1], actual_path)
        assert operation in OPENAPI_OPERATIONS
        template = operation[1]
    handler_evidence = is_handler_evidence(response.status_code, body)
    RESULTS.append(
        {
            "label": label,
            "method": method,
            "path": actual_path,
            "template": template,
            "status": response.status_code,
            "elapsed_ms": round(response.elapsed.total_seconds() * 1000, 3),
            "coverage": coverage,
            "handler_evidence": handler_evidence,
        }
    )
    assert response.status_code in expected, (
        f"{label}: expected={sorted(expected)} actual={response.status_code} body_type={type(body).__name__}"
    )
    assert response.status_code < 500, f"{label}: server error {response.status_code} {body}"
    if response.status_code >= 400:
        assert is_application_error_contract(response.status_code, body), (
            f"{label}: invalid application error contract status={response.status_code}"
        )
    return body


def call(
    client: httpx.Client,
    method: str,
    path: str,
    expected: int | set[int],
    *,
    label: str | None = None,
    coverage: str = "behavior",
    operation: tuple[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    expected_set = {expected} if isinstance(expected, int) else expected
    return record(
        label or f"{method} {path}",
        client.request(method, path, **kwargs),
        expected_set,
        coverage=coverage,
        operation=operation,
    )


def substitute_path(path: str) -> str:
    values = {
        "storage_key": "upload_images/missing/probe.png",
        "script_id": "missing-script",
        "episode_id": "missing-episode",
        "subject_id": "missing-subject",
        "suggestion_id": "missing-suggestion",
        "candidate_id": "missing-candidate",
        "member_id": "missing-member",
        "material_id": "missing-material",
        "image_id": "missing-image",
        "job_id": "missing-job",
        "arrangement_id": "missing-arrangement",
        "mid": "missing-material",
        "filename": "probe.png",
        "sid": "missing-script",
        "eid": "missing-episode",
    }
    return re.sub(r"\{([^}:]+)(?::path)?\}", lambda match: values.get(match.group(1), "missing"), path)


def unauthenticated_route_matrix() -> None:
    global OPENAPI_OPERATIONS, OPENAPI_SPEC
    with httpx.Client(base_url=BASE_URL, timeout=30.0) as anonymous:
        spec = call(anonymous, "GET", "/v3api/openapi.json", 200, label="OpenAPI")
        assert isinstance(spec, dict) and len(spec.get("paths", {})) == EXPECTED_PATH_COUNT
        operations: list[tuple[str, str]] = []
        operation_ids: list[str] = []
        for path, methods in spec["paths"].items():
            for method, operation in methods.items():
                if method.upper() in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
                    operations.append((method.upper(), path))
                    operation_id = operation.get("operationId")
                    assert isinstance(operation_id, str) and operation_id
                    operation_ids.append(operation_id)
        assert len(operations) == EXPECTED_OPERATION_COUNT, f"unexpected operation count: {len(operations)}"
        assert len(set(operations)) == EXPECTED_OPERATION_COUNT
        assert len(operation_ids) == len(set(operation_ids)) == EXPECTED_OPERATION_COUNT
        OPENAPI_OPERATIONS = set(operations)
        OPENAPI_SPEC = spec
        public = {
            ("GET", "/v3api/health"),
            ("POST", "/v3api/auth/login"),
            ("POST", "/v3api/auth/logout"),
        }
        protected = [operation for operation in operations if operation not in public]
        assert len(protected) == 76
        for method, templated_path in protected:
            path = substitute_path(templated_path)
            response = anonymous.request(method, path, json={})
            body = record(
                f"未登录边界 {method} {templated_path}",
                response,
                {401},
                operation=(method, templated_path),
            )
            assert body == {"detail": "未登录"}
        call(anonymous, "GET", "/v3api/health", 200, label="健康检查")
        call(
            anonymous,
            "POST",
            "/v3api/auth/login",
            401,
            label="错误密码",
            json={"username": OWNER_LOGIN["username"], "password": "wrong-password"},
        )
        call(anonymous, "POST", "/v3api/auth/logout", 200, label="匿名登出")


def _resolve_schema(schema: dict[str, Any], *, seen: frozenset[str] = frozenset()) -> dict[str, Any]:
    reference = schema.get("$ref")
    if not isinstance(reference, str):
        return schema
    assert reference.startswith("#/components/schemas/") and reference not in seen
    name = reference.rsplit("/", 1)[-1]
    resolved = OPENAPI_SPEC["components"]["schemas"][name]
    assert isinstance(resolved, dict)
    return _resolve_schema(resolved, seen=seen | {reference})


def _schema_sample(schema: dict[str, Any]) -> Any:
    schema = _resolve_schema(schema)
    if "default" in schema:
        return schema["default"]
    if "example" in schema:
        return schema["example"]
    enum = schema.get("enum")
    if isinstance(enum, list) and enum:
        return enum[0]
    for union_key in ("anyOf", "oneOf"):
        choices = schema.get(union_key)
        if isinstance(choices, list):
            choice = next((item for item in choices if item.get("type") != "null"), choices[0])
            return _schema_sample(choice)
    value_type = schema.get("type")
    if value_type == "string":
        if schema.get("format") == "uuid":
            return "00000000-0000-4000-8000-000000000000"
        return "validation"
    if value_type == "integer":
        return max(1, int(schema.get("minimum", 1)))
    if value_type == "number":
        return max(1.0, float(schema.get("minimum", 1.0)))
    if value_type == "boolean":
        return True
    if value_type == "array":
        count = max(1, int(schema.get("minItems", 1)))
        return [_schema_sample(schema.get("items", {})) for _ in range(count)]
    if value_type == "object" or "properties" in schema:
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        return {name: _schema_sample(properties[name]) for name in required}
    return "validation"


def route_probe_kwargs(method: str, template: str) -> dict[str, Any]:
    operation = OPENAPI_SPEC["paths"][template][method.lower()]
    kwargs: dict[str, Any] = {}
    parameters = [*OPENAPI_SPEC["paths"][template].get("parameters", []), *operation.get("parameters", [])]
    query = {
        parameter["name"]: _schema_sample(parameter.get("schema", {}))
        for parameter in parameters
        if parameter.get("in") == "query" and parameter.get("required") is True
    }
    if query:
        kwargs["params"] = query
    request_body = operation.get("requestBody", {})
    content = request_body.get("content", {})
    if "application/json" in content:
        kwargs["json"] = _schema_sample(content["application/json"].get("schema", {}))
    elif "multipart/form-data" in content:
        kwargs["files"] = {"file": ("probe.png", png_bytes(), "image/png")}
    return kwargs


def reached_operations() -> set[tuple[str, str]]:
    return {
        (result["method"], result["template"])
        for result in RESULTS
        if result.get("template") is not None and result.get("handler_evidence") is True
    }


def authenticated_route_matrix(client: httpx.Client) -> None:
    """以登录态触达主流程尚未覆盖的 OpenAPI 操作，验证路由和错误契约。"""
    covered = reached_operations()
    missing = sorted(OPENAPI_OPERATIONS - covered)
    accepted = set(range(200, 500)) - {401, 405}
    for method, template in missing:
        path = substitute_path(template)
        response = client.request(method, path, **route_probe_kwargs(method, template))
        record(
            f"登录态路由补齐 {method} {template}",
            response,
            accepted,
            coverage="route_only",
            operation=(method, template),
        )

    final_covered = reached_operations()
    assert final_covered == OPENAPI_OPERATIONS, sorted(OPENAPI_OPERATIONS - final_covered)


def percentile(values: list[float], ratio: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * ratio)))
    return round(ordered[index], 3)


def png_bytes() -> bytes:
    return base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
    )


def wav_bytes() -> bytes:
    target = io.BytesIO()
    with wave.open(target, "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(8000)
        output.writeframes(b"\x00\x00" * 800)
    return target.getvalue()


def authenticated_business_flow() -> dict[str, Any]:
    owner = httpx.Client(base_url=BASE_URL, timeout=60.0)
    member = httpx.Client(base_url=BASE_URL, timeout=60.0)
    admin = httpx.Client(base_url=BASE_URL, timeout=60.0)
    foreign = httpx.Client(base_url=BASE_URL, timeout=60.0)
    try:
        owner_user = call(owner, "POST", "/v3api/auth/login", 200, json=OWNER_LOGIN)
        member_user = call(member, "POST", "/v3api/auth/login", 200, json=MEMBER_LOGIN)
        admin_user = call(admin, "POST", "/v3api/auth/login", 200, json=ADMIN_LOGIN)
        foreign_user = call(foreign, "POST", "/v3api/auth/login", 200, json=FOREIGN_LOGIN)
        assert owner_user["user_id"] == OWNER_ID
        assert member_user["user_id"] == MEMBER_ID
        assert admin_user["is_admin"] is True
        assert foreign_user["org_id"] == FOREIGN_ORG_ID
        assert call(owner, "GET", "/v3api/auth/me", 200)["org_id"] == ORG_ID
        assert call(owner, "POST", "/v3api/auth/switch-org", 200, json={"org_id": ORG_ID})["org_id"] == ORG_ID

        accounts = call(owner, "GET", "/v3api/credits/accounts", 200)["items"]
        account_ids = {item["id"] for item in accounts}
        assert {ORG_CREDIT_ID, OWNER_CREDIT_ID} <= account_ids
        balances_before = {item["id"]: item["balance_points"] for item in accounts}
        member_accounts = call(member, "GET", "/v3api/credits/accounts", 200)["items"]
        member_balances_before = {item["id"]: item["balance_points"] for item in member_accounts}
        assert MEMBER_CREDIT_ID in member_balances_before
        grant_payload = {
            "from_account_id": ORG_CREDIT_ID,
            "to_account_id": MEMBER_CREDIT_ID,
            "points": 25,
            "note": "全流程真实验收",
        }
        grant_headers = {"Idempotency-Key": idempotency_key("grant")}
        call(owner, "POST", "/v3api/credits/grant", 200, json=grant_payload, headers=grant_headers)
        call(
            owner,
            "POST",
            "/v3api/credits/grant",
            200,
            label="积分下发幂等重放",
            json=grant_payload,
            headers=grant_headers,
        )
        balances_after = {
            item["id"]: item["balance_points"]
            for item in call(owner, "GET", "/v3api/credits/accounts", 200)["items"]
        }
        member_balances_after = {
            item["id"]: item["balance_points"]
            for item in call(member, "GET", "/v3api/credits/accounts", 200)["items"]
        }
        assert balances_after[ORG_CREDIT_ID] == balances_before[ORG_CREDIT_ID] - 25
        assert (
            member_balances_after[MEMBER_CREDIT_ID]
            == member_balances_before[MEMBER_CREDIT_ID] + 25
        )
        logs = call(owner, "GET", "/v3api/credits/logs", 200, params={"account_id": ORG_CREDIT_ID})
        assert len(logs["items"]) == 1
        assert logs["items"][0]["log_type"] == "grant" and logs["items"][0]["points_delta"] == -25
        call(
            member,
            "POST",
            "/v3api/credits/grant",
            403,
            json=grant_payload,
            headers={"Idempotency-Key": idempotency_key("grant-forbidden")},
        )

        personal = call(
            owner,
            "POST",
            "/v3api/scripts",
            201,
            json={"title": f"验收个人剧本 {RUN_HASH}", "scope": "personal"},
        )
        team = call(
            owner,
            "POST",
            "/v3api/scripts",
            201,
            json={"title": f"验收团队剧本 {RUN_HASH}", "scope": "team"},
        )
        personal_id = personal["id"]
        script_id = team["id"]
        disposable_episode = call(
            owner,
            "POST",
            f"/v3api/scripts/{personal_id}/episodes",
            201,
            json={"title": "待复制与删除分集", "script_text": "需要完整复制的分集正文。", "episode_no": 1},
        )
        copied = call(
            owner,
            "POST",
            f"/v3api/scripts/{personal_id}/copy",
            201,
            json={"title": f"验收个人剧本副本 {RUN_HASH}", "scope": "personal"},
        )
        copied_episodes = call(owner, "GET", f"/v3api/scripts/{copied['id']}/episodes", 200)
        assert len(copied_episodes) == 1
        copied_breakdown = call(
            owner,
            "GET",
            f"/v3api/scripts/{copied['id']}/episodes/{copied_episodes[0]['episode_id']}/breakdown",
            200,
        )
        assert copied_breakdown["episode"]["script_text"] == "需要完整复制的分集正文。"
        assert call(owner, "GET", "/v3api/scripts/page", 200, params={"scope": "personal"})["total"] >= 1
        assert call(owner, "GET", "/v3api/scripts/page", 200, params={"scope": "team"})["total"] >= 1
        call(member, "GET", f"/v3api/scripts/{script_id}", 403, label="非项目成员读取拒绝")
        call(admin, "GET", f"/v3api/scripts/{script_id}", 403, label="平台管理员不隐式读取项目")
        call(foreign, "GET", f"/v3api/scripts/{script_id}", 403, label="跨组织项目读取拒绝")
        member_record = call(
            owner,
            "POST",
            f"/v3api/scripts/{script_id}/members",
            201,
            json={"user_id": MEMBER_ID, "project_role": "reviewer"},
        )
        project_member_id = member_record["member_id"]
        assert call(owner, "GET", f"/v3api/scripts/{script_id}/members", 200)["total"] >= 2
        call(member, "GET", f"/v3api/scripts/{script_id}", 200, label="Reviewer 读取")
        call(member, "PATCH", f"/v3api/scripts/{script_id}", 403, json={"title": "越权修改"})
        call(member, "GET", f"/v3api/scripts/{script_id}/members", 403, label="Reviewer 成员管理拒绝")
        call(
            owner,
            "PATCH",
            f"/v3api/scripts/{script_id}/members/{project_member_id}",
            200,
            json={"project_role": "editor"},
        )
        call(member, "PATCH", f"/v3api/scripts/{script_id}", 200, json={"title": "验收团队剧本-成员编辑"})
        call(owner, "PATCH", f"/v3api/scripts/{script_id}", 200, json={"title": "验收团队剧本-完整流程"})
        call(owner, "GET", f"/v3api/scripts/{copied['id']}", 200)
        call(owner, "GET", "/v3api/scripts/news-broadcast/jobs", 200)

        script_text = (
            "雨夜的旧车站里，年轻记者林舟握着一把红色雨伞走进候车厅。"
            "广播突然中断，桌上的黑色录音机发出沙沙声。林舟说：我们必须在列车到达前找到证据。"
            "随后他推开站台铁门，远处灯光亮起。"
        )
        episode = call(
            owner,
            "POST",
            f"/v3api/scripts/{script_id}/episodes",
            201,
            json={"title": "第一集", "script_text": "初稿", "episode_no": 1},
        )
        episode_id = episode["episode_id"]
        assert len(call(owner, "GET", f"/v3api/scripts/{script_id}/episodes", 200)) == 1
        save_payload = {
            "title": "第一集：雨夜车站",
            "script_text": script_text,
            "episode_version": episode["episode_version"],
            "client_save_seq": episode["client_save_seq"],
        }
        saved = call(
            owner, "PUT", f"/v3api/scripts/{script_id}/episodes/{episode_id}", 200, json=save_payload
        )
        assert saved["episode_version"] > episode["episode_version"]
        assert saved["client_save_seq"] > episode["client_save_seq"]
        call(
            owner,
            "PUT",
            f"/v3api/scripts/{script_id}/episodes/{episode_id}",
            409,
            label="分集旧版本冲突",
            json=save_payload,
        )
        call(owner, "GET", f"/v3api/scripts/{script_id}/episodes/{episode_id}/breakdown", 200)
        call(owner, "GET", f"/v3api/scripts/{script_id}/pipeline-status", 200)
        call(owner, "GET", f"/v3api/scripts/{script_id}/extract-subjects-status", 200)
        pipeline_payload = {
            "mode": "manual_reextract",
            "episode_version": saved["episode_version"],
            "client_save_seq": saved["client_save_seq"],
        }
        call(
            owner,
            "POST",
            f"/v3api/scripts/{script_id}/episodes/{episode_id}/pipeline-runs",
            409,
            label="活动流水线并发拒绝",
            json=pipeline_payload,
            headers={"Idempotency-Key": idempotency_key("pipeline-concurrent")},
        )

        character = call(
            owner,
            "POST",
            f"/v3api/scripts/{script_id}/asset-library/subjects",
            201,
            json={
                "type": "character",
                "name": "林舟",
                "description": "二十多岁的记者，深色风衣，神情专注，手持红色雨伞。",
                "episode_nos": [1],
            },
        )
        sound = call(
            owner,
            "POST",
            f"/v3api/scripts/{script_id}/asset-library/subjects",
            201,
            json={
                "type": "sound",
                "name": "车站广播",
                "description": "老旧广播的沙沙声和中断提示音。",
                "episode_nos": [1],
            },
        )
        disposable = call(
            owner,
            "POST",
            f"/v3api/scripts/{script_id}/asset-library/subjects",
            201,
            json={
                "type": "prop",
                "name": "临时道具",
                "description": "仅用于删除流程验收的道具。",
                "episode_nos": [1],
            },
        )
        subject_id = character["subject_id"]
        sound_id = sound["subject_id"]
        assert call(owner, "GET", f"/v3api/scripts/{script_id}/asset-library/subjects", 200)["total"] == 3
        call(owner, "GET", f"/v3api/scripts/{script_id}/asset-library/parent-subjects", 200)
        call(owner, "GET", f"/v3api/scripts/{script_id}/asset/entries", 200)
        call(owner, "GET", f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions", 200)
        call(
            owner,
            "POST",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions/missing-suggestion/resolve",
            404,
            label="不存在差异建议错误契约",
            json={"action": "ignore", "expected_subject_version": character["subject_version"]},
        )
        call(
            owner,
            "DELETE",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{disposable['subject_id']}",
            200,
        )
        updated_subject = call(
            owner,
            "PATCH",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}",
            200,
            json={
                "description": "二十多岁的调查记者，深色防水风衣，神情专注，手持红色雨伞。",
                "expected_subject_version": character["subject_version"],
            },
        )
        call(
            owner,
            "PATCH",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}",
            409,
            label="主体旧版本冲突",
            json={"name": "旧版本覆盖", "expected_subject_version": character["subject_version"]},
        )

        ensured = call(owner, "GET", f"/v3api/scripts/{script_id}/canvas-project/ensure", 200)
        material_id = ensured["materials_id"]
        assert (
            call(owner, "GET", f"/v3api/scripts/{script_id}/canvas-project/ensure", 200)["materials_id"]
            == material_id
        )
        call(owner, "GET", f"/v3api/materials/{material_id}", 200)
        canvas = call(owner, "GET", f"/v3api/materials/{material_id}/canvas", 200)
        canvas = call(
            owner,
            "POST",
            f"/v3api/materials/{material_id}/canvas/delta",
            200,
            json={
                "version": canvas["version"],
                "upsert_nodes": [
                    {
                        "id": NODE_A_ID,
                        "type": "text",
                        "position": {"x": 10, "y": 20},
                        "data": {"content": "雨夜车站"},
                    },
                    {
                        "id": NODE_B_ID,
                        "type": "imageUpload",
                        "position": {"x": 220, "y": 20},
                        "data": {"subjectId": subject_id, "prompt": "客户端临时提示词"},
                    },
                ],
                "upsert_edges": [
                    {"id": EDGE_ID, "source": NODE_A_ID, "target": NODE_B_ID}
                ],
            },
        )
        assert len(canvas["nodes"]) == 2 and len(canvas["edges"]) == 1
        moved = call(
            owner,
            "POST",
            f"/v3api/materials/{material_id}/canvas/delta",
            200,
            json={
                "version": canvas["version"],
                "move_nodes": [{"id": NODE_A_ID, "position": {"x": 30, "y": 40}}],
            },
        )
        assert next(node for node in moved["nodes"] if node["id"] == NODE_A_ID)["position"] == {
            "x": 30,
            "y": 40,
        }
        persisted_canvas = call(owner, "GET", f"/v3api/materials/{material_id}/canvas", 200)
        assert next(node for node in persisted_canvas["nodes"] if node["id"] == NODE_A_ID)[
            "position"
        ] == {"x": 30, "y": 40}

        upload_path = f"/api/x/cctv/zhihui/flow-app/upload_images/{material_id}/portrait.png"
        expected_png = png_bytes()
        uploaded_image = call(
            owner,
            "POST",
            upload_path,
            200,
            params={"subject_id": subject_id},
            files={"file": ("portrait.png", expected_png, "image/png")},
        )
        assert uploaded_image["mime_type"] == "image/png"
        assert uploaded_image["size_bytes"] == len(expected_png)
        assert uploaded_image["sha256"] == hashlib.sha256(expected_png).hexdigest()
        image_asset_id = uploaded_image["asset_id"]
        storage_path = "/api/x/cctv/zhihui/flow-app" + uploaded_image["storage_key"]
        full_response = owner.get(storage_path)
        record("媒体完整读取", full_response, {200})
        assert full_response.content == expected_png
        assert full_response.headers["content-type"].split(";", 1)[0] == "image/png"
        assert int(full_response.headers["content-length"]) == len(expected_png)
        range_response = owner.get(storage_path, headers={"Range": "bytes=0-7"})
        record("媒体 Range", range_response, {206})
        assert range_response.content == expected_png[:8]
        assert range_response.headers["content-type"].split(";", 1)[0] == "image/png"
        assert range_response.headers["content-range"].startswith("bytes 0-7/")
        call(owner, "GET", storage_path, 416, label="媒体非法 Range", headers={"Range": "bytes=999999-"})
        call(
            member,
            "GET",
            f"/api/x/cctv/zhihui/flow-app/upload_images/{material_id}/missing.png",
            404,
            label="成员合法范围随机媒体键",
        )
        call(foreign, "GET", storage_path, 403, label="跨组织真实媒体键拒绝")
        call(
            foreign,
            "POST",
            upload_path,
            403,
            label="跨组织向他人 Materials 上传拒绝",
            params={"subject_id": subject_id},
            files={"file": ("foreign.png", expected_png, "image/png")},
        )
        call(
            owner,
            "POST",
            f"/api/x/cctv/zhihui/flow-app/upload_images/{material_id}/fake.png",
            400,
            label="伪造图片 MIME 拒绝",
            files={"file": ("fake.png", b"not an image", "image/png")},
        )
        candidates = call(
            owner, "GET", f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates", 200
        )
        assert {item["candidate_id"] for item in candidates["image_candidates"]} == {image_asset_id}
        call(
            owner,
            "GET",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/replace-impact",
            200,
        )
        call(
            owner,
            "PATCH",
            f"/v3api/scripts/{script_id}/members/{project_member_id}",
            200,
            json={"project_role": "reviewer"},
        )
        approved = call(
            member,
            "POST",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-image",
            200,
            json={"candidate_id": image_asset_id, "expected_image_entry_id": None},
        )
        assert approved["subject"]["image_entry"]["asset_id"] == image_asset_id
        image_meta = call(owner, "GET", f"/v3api/materials/{material_id}/image/{image_asset_id}", 200)
        assert image_meta["storage_key"] == uploaded_image["storage_key"]

        call(
            owner,
            "PATCH",
            f"/v3api/scripts/{script_id}/members/{project_member_id}",
            200,
            json={"project_role": "viewer"},
        )
        second_candidate = call(
            owner,
            "POST",
            f"/api/x/cctv/zhihui/flow-app/upload_images/{material_id}/second.png",
            200,
            params={"subject_id": subject_id},
            files={"file": ("second.png", png_bytes(), "image/png")},
        )
        call(
            member,
            "POST",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-image",
            403,
            label="Viewer 审核资产拒绝",
            json={"candidate_id": second_candidate["asset_id"], "expected_image_entry_id": image_asset_id},
        )
        generation_payload = {
            "material_id": material_id,
            "subject_id": subject_id,
            "model": "qwen-image",
            "size": "2k",
            "aspect": "16:9",
            "count": 1,
        }
        call(
            member,
            "POST",
            "/v3api/generation-jobs",
            403,
            label="Viewer 运行 AI 拒绝",
            json=generation_payload,
            headers={"Idempotency-Key": idempotency_key("gen-viewer-deny")},
        )
        call(
            owner,
            "DELETE",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates/{second_candidate['asset_id']}",
            200,
        )
        assert (
            call(
                owner,
                "GET",
                f"/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates",
                200,
            )["image_candidates"]
            == []
        )
        expected_wav = wav_bytes()
        uploaded_audio = call(
            owner,
            "POST",
            f"/api/x/cctv/zhihui/flow-app/upload_images/{material_id}/station.wav",
            200,
            params={"subject_id": sound_id},
            files={"file": ("station.wav", expected_wav, "audio/wav")},
        )
        assert uploaded_audio["mime_type"] in {"audio/wav", "audio/x-wav"}
        assert uploaded_audio["size_bytes"] == len(expected_wav)
        assert uploaded_audio["sha256"] == hashlib.sha256(expected_wav).hexdigest()
        audio_path = "/api/x/cctv/zhihui/flow-app" + uploaded_audio["storage_key"]
        audio_response = owner.get(audio_path)
        record("音频媒体完整读取", audio_response, {200})
        assert audio_response.content == expected_wav
        assert audio_response.headers["content-type"].split(";", 1)[0] in {
            "audio/wav",
            "audio/x-wav",
        }
        audio_range = owner.get(audio_path, headers={"Range": "bytes=4-19"})
        record("音频媒体 Range", audio_range, {206})
        assert audio_range.content == expected_wav[4:20]
        assert audio_range.headers["content-range"].startswith("bytes 4-19/")
        call(
            owner,
            "PATCH",
            f"/v3api/scripts/{script_id}/members/{project_member_id}",
            200,
            json={"project_role": "editor"},
        )
        call(
            member,
            "POST",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{sound_id}/approve-audio",
            403,
            label="Editor 默认审核资产拒绝",
            json={"candidate_id": uploaded_audio["asset_id"]},
        )
        approved_audio = call(
            owner,
            "POST",
            f"/v3api/scripts/{script_id}/asset-library/subjects/{sound_id}/approve-audio",
            200,
            json={"candidate_id": uploaded_audio["asset_id"]},
        )
        assert approved_audio["audio_entries"]
        assert call(owner, "GET", f"/v3api/scripts/{script_id}/asset/entries", 200)["total"] >= 2

        call(
            member,
            "POST",
            "/v3api/generation-jobs",
            409,
            label="Editor 可运行 AI 但 enrich 未完成",
            json=generation_payload,
            headers={"Idempotency-Key": idempotency_key("gen-pre-enrich")},
        )
        batch = call(
            owner,
            "POST",
            "/v3api/generation-jobs/batch",
            201,
            json={"material_id": material_id, "subject_ids": [subject_id], "model": "qwen-image", "count": 1},
            headers={"Idempotency-Key": idempotency_key("gen-batch-pre")},
        )
        assert batch["created"] == [] and batch["skipped"] == []
        assert [item["subject_id"] for item in batch["failed"]] == [subject_id]

        storyboard_path = f"/v3api/scripts/{script_id}/episodes/{episode_id}/generate-storyboard"
        storyboard_headers = {"Idempotency-Key": idempotency_key("storyboard-cancel")}
        storyboard = call(
            owner,
            "POST",
            storyboard_path,
            200,
            params={"episode_version": saved["episode_version"]},
            headers=storyboard_headers,
        )
        storyboard_replay = call(
            owner,
            "POST",
            storyboard_path,
            200,
            label="分镜幂等重放",
            params={"episode_version": saved["episode_version"]},
            headers=storyboard_headers,
        )
        assert storyboard_replay["job_id"] == storyboard["job_id"]
        call(
            owner,
            "POST",
            storyboard_path,
            409,
            label="分镜活动任务去重",
            params={"episode_version": saved["episode_version"]},
            headers={"Idempotency-Key": idempotency_key("storyboard-concurrent")},
        )
        job_path = f"/v3api/scripts/{script_id}/episodes/{episode_id}/storyboard/{storyboard['job_id']}"
        call(owner, "GET", f"/v3api/scripts/{script_id}/episodes/{episode_id}/storyboard-jobs", 200)
        call(owner, "GET", job_path, 200)
        call(
            owner,
            "PATCH",
            job_path,
            409,
            label="运行中分镜不可编辑",
            json={"progress_message": "验收展示字段"},
        )
        call(
            owner,
            "POST",
            job_path + "/retry",
            409,
            label="运行中分镜不可重试",
            headers={"Idempotency-Key": idempotency_key("storyboard-run-retry")},
        )
        canceled_storyboard = call(owner, "POST", job_path + "/cancel", 200)
        assert canceled_storyboard["status"] == "canceled"
        call(owner, "GET", f"/v3api/scripts/{script_id}/episodes/{episode_id}/storyboard-status", 200)
        call(owner, "GET", f"/v3api/scripts/{script_id}/episodes/{episode_id}/shot-hierarchy", 200)
        call(owner, "GET", f"/v3api/shots/{script_id}/episodes/{episode_id}/video-workbench", 200)
        call(owner, "GET", f"/v3api/shots/{script_id}/cover", 200)

        call(
            owner,
            "PATCH",
            f"/v3api/scripts/{script_id}/members/{project_member_id}",
            200,
            json={"project_role": "manager"},
        )
        managed_member = call(
            member,
            "POST",
            f"/v3api/scripts/{script_id}/members",
            201,
            label="ProjectManager 添加成员",
            json={"user_id": ADMIN_ID, "project_role": "viewer"},
        )
        call(
            member,
            "DELETE",
            f"/v3api/scripts/{script_id}/members/{managed_member['member_id']}",
            200,
            label="ProjectManager 移除成员",
        )

        active_provider = call(owner, "GET", "/v3api/video-generation/active-provider", 200)
        assert active_provider["provider_key"] == "volcengine"
        assert active_provider["model"]
        call(
            owner,
            "POST",
            "/v3api/video-generation/active-provider",
            403,
            json={"provider_key": "modelsai", "job_type": "all_reference"},
        )
        assert call(admin, "GET", "/v3api/video-generation/active-provider", 200) == active_provider
        call(
            admin,
            "POST",
            "/v3api/video-generation/active-provider",
            409,
            label="env 模式拒绝运行时切换视频 Provider",
            json={"provider_key": "modelsai", "job_type": "all_reference"},
        )
        assert call(owner, "GET", "/v3api/video-generation/active-provider", 200) == active_provider
        call(
            owner,
            "GET",
            "/v3api/video-generation-jobs",
            200,
            params={"script_id": script_id, "episode_id": episode_id},
        )
        call(
            owner,
            "GET",
            "/v3api/video-timeline-arrangements",
            200,
            params={"script_id": script_id, "episode_id": episode_id},
        )

        call(owner, "GET", f"/v3api/scripts/{personal_id}", 200)
        call(member, "GET", f"/v3api/scripts/{personal_id}", 403, label="个人项目 IDOR 拒绝")
        call(
            member,
            "GET",
            f"/api/x/cctv/zhihui/flow-app{uploaded_image['storage_key']}",
            200,
            label="团队成员媒体读取",
        )
        call(
            owner,
            "DELETE",
            f"/v3api/scripts/{personal_id}/episodes/{disposable_episode['episode_id']}",
            200,
        )
        call(
            owner,
            "DELETE",
            f"/v3api/scripts/{copied['id']}",
            409,
            label="含分集的剧本副本默认拒绝删除",
        )
        call(
            owner,
            "DELETE",
            f"/v3api/scripts/{copied['id']}",
            200,
            params={"confirm": "true"},
        )
        call(owner, "DELETE", f"/v3api/scripts/{personal_id}", 200)
        call(owner, "DELETE", f"/v3api/scripts/{script_id}/members/{project_member_id}", 200)
        call(member, "GET", f"/v3api/scripts/{script_id}", 403, label="成员移除后访问撤销")
        authenticated_route_matrix(owner)
        call(owner, "POST", "/v3api/auth/logout", 200)
        call(owner, "GET", "/v3api/auth/me", 401, label="登出后会话失效")

        return {
            "script_id": script_id,
            "episode_id": episode_id,
            "episode_version": saved["episode_version"],
            "client_save_seq": saved["client_save_seq"],
            "material_id": material_id,
            "subject_id": subject_id,
            "image_asset_id": image_asset_id,
            "storage_keys": [
                uploaded_image["storage_key"],
                second_candidate["storage_key"],
                uploaded_audio["storage_key"],
            ],
            "canceled_storyboard_job_id": storyboard["job_id"],
            "member_record_id": project_member_id,
            "subject_version": updated_subject["subject_version"],
        }
    finally:
        owner.close()
        member.close()
        admin.close()
        foreign.close()


def run_offline_self_tests() -> None:
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
    assert not is_handler_evidence(401, {"detail": "未登录"})
    assert not is_handler_evidence(405, {"detail": "Method Not Allowed"})
    assert not is_handler_evidence(404, {"detail": "Not Found"})
    assert not is_handler_evidence(422, {"detail": [{"type": "missing"}]})
    assert is_handler_evidence(404, {"detail": "剧本不存在"})
    assert is_handler_evidence(422, {"detail": "请求参数非法"})
    assert is_handler_evidence(200, None)

    urls = isolated_redis_urls(
        {
            "REDIS_URL": "redis://127.0.0.1:56380/0",
            "CELERY_BROKER_URL": "redis://127.0.0.1:56380/1",
            "CELERY_RESULT_BACKEND": "redis://127.0.0.1:56380/2",
        }
    )
    assert set(urls) == {"app", "broker", "backend"}
    for invalid in (
        "redis://127.0.0.1:6379/0",
        "redis://user:secret@127.0.0.1:56380/0",
        "redis://127.0.0.1:56380/0?db=2",
    ):
        try:
            parse_redis_target("Redis", invalid, 0)
        except (AssertionError, ValueError):
            continue
        raise AssertionError("Redis guard 未拒绝错误目标")

    process_table = parse_ps_processes(
        "1 0 /sbin/launchd\n"
        "100 1 /opt/mtv/.venv/bin/celery -A app.workers.celery_app worker -l INFO\n"
        "101 100 [celeryd: worker@host:ForkPoolWorker-1]\n"
    )
    process_evidence = evaluate_celery_processes(process_table, expect_worker=True)
    assert process_evidence["worker_root_pids"] == [100]
    assert process_evidence["worker_process_count"] == 2
    assert evaluate_celery_processes(process_table[:1], expect_worker=False)["beat_process_count"] == 0
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
        "/opt/mtv/.venv/bin/celery 'unterminated",
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

    now = datetime.now(UTC)
    with tempfile.TemporaryDirectory(prefix="mtv-http-self-test-") as temporary:
        root = Path(temporary)
        paths = run_artifact_paths(VALIDATION_RUN_ID, root=root)
        seed = write_signed_exclusive(
            paths["seed_manifest"],
            {
                "status": "passed",
                "run_id": VALIDATION_RUN_ID,
                "started_at": now.isoformat(),
                "finished_at": now.isoformat(),
                "seed_ids": SEED_IDS,
                "fingerprint": {
                    "database": {
                        "host": "127.0.0.1",
                        "port": 55438,
                        "database": "mtv_validation",
                        "user": "mtv_validation",
                    },
                    "server": {"api_base_url": BASE_URL},
                },
            },
        )
        assert load_verified_seed_manifest(paths["seed_manifest"], http_started_at=now) == seed
        tampered = dict(seed)
        tampered["status"] = "failed"
        try:
            verify_signed_payload(tampered, label="tampered")
        except AssertionError:
            pass
        else:
            raise AssertionError("SHA 篡改未被拒绝")
        write_signed_exclusive(paths["http_result"], {"status": "running"})
        try:
            write_signed_exclusive(paths["http_result"], {"status": "running"})
        except FileExistsError:
            pass
        else:
            raise AssertionError("同 nonce HTTP 结果被重复占用")
        fsync_failure = root / "fsync-failure.json"
        with mock.patch(
            f"{__name__}._fsync_directory",
            side_effect=OSError("injected directory fsync failure"),
        ):
            try:
                write_signed_exclusive(fsync_failure, {"status": "running"})
            except OSError:
                pass
            else:
                raise AssertionError("目录 fsync 失败被错误报告为 HTTP claim 成功")
        assert fsync_failure.exists()
    print("live HTTP offline self-tests passed")


def main() -> None:
    started_at = datetime.now(UTC)
    seed_manifest = load_verified_seed_manifest(SEED_MANIFEST_PATH, http_started_at=started_at)
    claim_http_result(started_at)
    start_isolation = assert_http_phase_isolation(stage="start")
    unauthenticated_route_matrix()
    context = authenticated_business_flow()
    end_isolation = assert_http_phase_isolation(stage="end")
    isolation_evidence = {
        "http_start": start_isolation,
        "http_end": end_isolation,
        "baseline_empty": {"app": True, "broker": True, "backend": True},
    }
    route_reached = reached_operations()
    assert route_reached == OPENAPI_OPERATIONS, sorted(OPENAPI_OPERATIONS - route_reached)
    behavior_verified = {
        (result["method"], result["template"])
        for result in RESULTS
        if result.get("template") is not None
        and result.get("handler_evidence") is True
        and result["coverage"] == "behavior"
    }
    happy_path_verified = {
        (result["method"], result["template"])
        for result in RESULTS
        if result.get("template") is not None
        and 200 <= result["status"] < 300
        and result["coverage"] == "behavior"
    }
    expected_error_verified = {
        (result["method"], result["template"])
        for result in RESULTS
        if result.get("template") is not None
        and 400 <= result["status"] < 500
        and result.get("handler_evidence") is True
        and result["coverage"] == "behavior"
    }
    latencies = [float(result["elapsed_ms"]) for result in RESULTS]
    finished_at = datetime.now(UTC)
    output = {
        "status": "passed",
        "run_id": VALIDATION_RUN_ID,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "base_url": BASE_URL,
        "seed_manifest_sha256": seed_manifest["sha256"],
        "isolation_evidence": isolation_evidence,
        "assertions": len(RESULTS),
        "operation_coverage": {
            "route_reached": len(route_reached),
            "behavior_verified": len(behavior_verified),
            "happy_path_verified": len(happy_path_verified),
            "expected_error_verified": len(expected_error_verified),
            "total": len(OPENAPI_OPERATIONS),
            "path_total": len(OPENAPI_SPEC["paths"]),
            "operation_id_total": EXPECTED_OPERATION_COUNT,
            "operation_set": sorted(f"{method} {path}" for method, path in OPENAPI_OPERATIONS),
            "route_reached_set": sorted(f"{method} {path}" for method, path in route_reached),
            "behavior_verified_set": sorted(f"{method} {path}" for method, path in behavior_verified),
            "happy_path_set": sorted(f"{method} {path}" for method, path in happy_path_verified),
            "expected_error_set": sorted(f"{method} {path}" for method, path in expected_error_verified),
            "route_only": sorted(f"{method} {path}" for method, path in route_reached - behavior_verified),
            "missing_routes": sorted(
                f"{method} {path}" for method, path in OPENAPI_OPERATIONS - route_reached
            ),
            "missing_happy_paths": sorted(
                f"{method} {path}" for method, path in OPENAPI_OPERATIONS - happy_path_verified
            ),
        },
        "latency_ms": {
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
        },
        "status_counts": {
            str(status): sum(1 for result in RESULTS if result["status"] == status)
            for status in sorted({result["status"] for result in RESULTS})
        },
        "context": context,
    }
    output = replace_signed_claim(RESULT_PATH, output)
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    if SELF_TEST_MODE:
        run_offline_self_tests()
    else:
        main()
