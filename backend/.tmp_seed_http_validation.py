from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from sqlalchemy.engine import make_url

SELF_TEST_MODE = sys.argv[1:] == ["--self-test"]
RUN_ID_PATTERN = re.compile(
    r"20\d{6}-[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)
VALIDATION_TIMEZONE = ZoneInfo("Asia/Shanghai")
BASE_URL = "http://127.0.0.1:18080"
EXPECTED_DATABASE_HOST = "127.0.0.1"
EXPECTED_DATABASE_PORT = 55438
EXPECTED_DATABASE = "mtv_validation"
EXPECTED_DATABASE_USER = "mtv_validation"
EXPECTED_REDIS_HOST = "127.0.0.1"
EXPECTED_REDIS_PORT = 56380
EXPECTED_ALEMBIC_HEAD = "f4a8c2e1d907"
CRITICAL_BUSINESS_TABLES = {
    "asset_entries",
    "audit_events",
    "canvas_edges",
    "canvas_nodes",
    "credit_accounts",
    "credit_logs",
    "credit_reservations",
    "departments",
    "episodes",
    "generation_batch_requests",
    "generation_jobs",
    "materials",
    "media_storage_jobs",
    "media_uploads",
    "organizations",
    "parent_subjects",
    "pipeline_jobs",
    "pipeline_stages",
    "project_members",
    "scenes",
    "scripts",
    "shot_groups",
    "shots",
    "storyboard_jobs",
    "subject_change_suggestions",
    "subjects",
    "task_dispatch_outbox",
    "timeline_render_jobs",
    "user_organizations",
    "users",
    "video_generation_idempotency_requests",
    "video_generation_jobs",
    "video_provider_configs",
    "video_timeline_arrangements",
}


def require_validation_run_id(value: str | None, *, now: datetime | None = None) -> str:
    """批次必须是日期加标准 UUID，避免人工短标签被重复使用。"""
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
    users = {
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
    }
    credit_accounts = {
        "org": f"crd-val-org-{suffix}",
        "owner": f"crd-val-o-{suffix}",
        "member": f"crd-val-m-{suffix}",
        "admin": f"crd-val-a-{suffix}",
    }
    assert all(len(value) <= 36 for value in [*organizations.values(), *credit_accounts.values()])
    assert all(len(item["user_id"]) <= 36 and len(item["username"]) <= 128 for item in users.values())
    return {
        "run_hash": suffix,
        "organizations": organizations,
        "users": users,
        "credit_accounts": credit_accounts,
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


def assert_no_run_artifacts(paths: dict[str, Path]) -> None:
    existing = [
        label
        for label, path in paths.items()
        if path.exists()
        or path.is_symlink()
        or path.with_suffix(path.suffix + ".tmp").exists()
        or path.with_suffix(path.suffix + ".tmp").is_symlink()
    ]
    assert not existing, f"本批次已有制品，拒绝复用 nonce: {sorted(existing)}"


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
    content = json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8")
    _write_bytes_exclusive(path, content)
    _fsync_directory(path.parent)
    return result


def replace_signed_claim(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    claim_stat = path.lstat()
    assert stat.S_ISREG(claim_stat.st_mode), "批次 claim 不是普通文件"
    temporary = path.with_suffix(path.suffix + ".tmp")
    result = write_signed_exclusive(temporary, payload)
    os.replace(temporary, path)
    _fsync_directory(path.parent)
    return result


def assert_database_url(label: str, raw_url: str) -> dict[str, Any]:
    """结构化校验固定 PG authority，任何 query 覆盖都失败关闭。"""
    url = make_url(raw_url)
    assert url.get_backend_name() == "postgresql", f"{label} 不是 PostgreSQL"
    assert url.drivername in {"postgresql+asyncpg", "postgresql+psycopg2"}, f"{label} driver 非法"
    assert not url.query, f"{label} 禁止 query 参数覆盖连接身份"
    assert url.host == EXPECTED_DATABASE_HOST, f"{label} host 不是隔离实例"
    assert url.port == EXPECTED_DATABASE_PORT, f"{label} port 不是隔离实例"
    assert url.database == EXPECTED_DATABASE, f"{label} database 不是隔离库"
    assert url.username == EXPECTED_DATABASE_USER, f"{label} user 不是隔离用户"
    return {
        "backend": "postgresql",
        "driver": url.drivername.split("+", 1)[-1],
        "host": url.host,
        "port": url.port,
        "database": url.database,
        "user": url.username,
    }


def parse_redis_target(label: str, raw_url: str, expected_db: int) -> dict[str, Any]:
    """只接受无凭据、无 query 的本机 Redis 0/1/2。"""
    parsed = urlsplit(raw_url)
    assert parsed.scheme == "redis", f"{label} scheme 非法"
    assert parsed.username is None and parsed.password is None, f"{label} 禁止用户名或密码"
    assert not parsed.query and not parsed.fragment, f"{label} 禁止 query/fragment 覆盖"
    assert parsed.netloc == f"{EXPECTED_REDIS_HOST}:{EXPECTED_REDIS_PORT}", f"{label} authority 非法"
    assert parsed.hostname == EXPECTED_REDIS_HOST and parsed.port == EXPECTED_REDIS_PORT
    assert parsed.path == f"/{expected_db}", f"{label} 必须使用 Redis DB {expected_db}"
    return {
        "scheme": parsed.scheme,
        "host": parsed.hostname,
        "port": parsed.port,
        "db": expected_db,
    }


def assert_redis_urls(redis_url: str, broker_url: str, backend_url: str) -> dict[str, dict[str, Any]]:
    targets = {
        "app": parse_redis_target("应用 Redis", redis_url, 0),
        "broker": parse_redis_target("Celery broker", broker_url, 1),
        "backend": parse_redis_target("Celery result backend", backend_url, 2),
    }
    instances = {(item["scheme"], item["host"], item["port"]) for item in targets.values()}
    assert len(instances) == 1, "Redis app/broker/backend 必须是同一隔离实例"
    assert {item["db"] for item in targets.values()} == {0, 1, 2}
    return targets


def run_offline_self_tests() -> None:
    sample_run_id = "20260719-123e4567-e89b-42d3-a456-426614174000"
    shanghai_boundary = datetime(2026, 7, 18, 16, 30, tzinfo=UTC)
    assert require_validation_run_id(sample_run_id, now=shanghai_boundary) == sample_run_id
    for invalid in (
        "20260718-final",
        "20260230-123e4567-e89b-42d3-a456-426614174000",
        "20260718-123e4567-e89b-42d3-a456-426614174000",
        "20260720-123e4567-e89b-42d3-a456-426614174000",
        "20260719-123E4567-e89b-42d3-a456-426614174000",
        "../escape",
    ):
        try:
            require_validation_run_id(invalid, now=shanghai_boundary)
        except AssertionError:
            continue
        raise AssertionError(f"nonce 门禁未拒绝: {invalid!r}")

    valid_pg = "postgresql+asyncpg://mtv_validation:x@127.0.0.1:55438/mtv_validation"
    assert_database_url("DATABASE_URL", valid_pg)
    for invalid_pg in (
        valid_pg + "?host=other",
        valid_pg.replace("55438", "5432"),
        valid_pg.replace("mtv_validation:x", "postgres:x"),
    ):
        try:
            assert_database_url("DATABASE_URL", invalid_pg)
        except AssertionError:
            continue
        raise AssertionError("PG 隔离 guard 未失败关闭")

    redis_targets = assert_redis_urls(
        "redis://127.0.0.1:56380/0",
        "redis://127.0.0.1:56380/1",
        "redis://127.0.0.1:56380/2",
    )
    assert [redis_targets[label]["db"] for label in ("app", "broker", "backend")] == [0, 1, 2]
    for invalid_redis in (
        "redis://127.0.0.1:6379/0",
        "redis://user:secret@127.0.0.1:56380/0",
        "redis://127.0.0.1:56380/0?db=2",
        "rediss://127.0.0.1:56380/0",
    ):
        try:
            parse_redis_target("Redis", invalid_redis, 0)
        except (AssertionError, ValueError):
            continue
        raise AssertionError("Redis 隔离 guard 未失败关闭")

    with tempfile.TemporaryDirectory(prefix="mtv-seed-self-test-") as temporary:
        paths = run_artifact_paths(sample_run_id, root=Path(temporary))
        assert_no_run_artifacts(paths)
        write_signed_exclusive(paths["http_result"], {"status": "failed"})
        try:
            assert_no_run_artifacts(paths)
        except AssertionError:
            pass
        else:
            raise AssertionError("旧 nonce 制品未被拒绝")
        fsync_failure = Path(temporary) / "fsync-failure.json"
        with mock.patch(
            f"{__name__}._fsync_directory",
            side_effect=OSError("injected directory fsync failure"),
        ):
            try:
                write_signed_exclusive(fsync_failure, {"status": "running"})
            except OSError:
                pass
            else:
                raise AssertionError("目录 fsync 失败被错误报告为 claim 首写成功")
        assert fsync_failure.exists(), "失败后保留已写 claim 供人工判定"
    print("seed offline self-tests passed")


if SELF_TEST_MODE:
    run_offline_self_tests()
    raise SystemExit(0)


VALIDATION_RUN_ID = require_validation_run_id(os.environ.get("MTV_VALIDATION_RUN_ID"))
SEED_IDS = build_seed_ids(VALIDATION_RUN_ID)
ARTIFACT_PATHS = run_artifact_paths(VALIDATION_RUN_ID)
SEED_MANIFEST_PATH = ARTIFACT_PATHS["seed_manifest"]
ORG_ID = SEED_IDS["organizations"]["primary"]
FOREIGN_ORG_ID = SEED_IDS["organizations"]["foreign"]

from sqlalchemy import text  # noqa: E402

from app.core.config import settings  # noqa: E402
from app.core.database import async_session_factory  # noqa: E402
from app.core.security import hash_password  # noqa: E402
from app.models.credit import CreditAccount  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.user import User  # noqa: E402
from app.models.user_organization import UserOrganization  # noqa: E402


async def assert_pristine_validation_database() -> dict[str, Any]:
    """首写前验证运行身份、迁移版本，以及 public 下所有业务表均为空。"""
    async_fingerprint = assert_database_url("DATABASE_URL", settings.database_url)
    sync_fingerprint = assert_database_url("DATABASE_SYNC_URL", settings.database_sync_url)
    assert async_fingerprint | {"driver": sync_fingerprint["driver"]} == sync_fingerprint
    redis_fingerprint = assert_redis_urls(
        settings.redis_url,
        settings.celery_broker_url,
        settings.celery_result_backend,
    )

    async with async_session_factory() as db:
        row = (
            await db.execute(
                text(
                    "SELECT current_database(), current_user, "
                    "inet_server_addr()::text, inet_server_port()"
                )
            )
        ).one()
        current_database, current_user, server_host, server_port = tuple(row)
        assert current_database == EXPECTED_DATABASE, "SQL current_database() 不匹配"
        assert current_user == EXPECTED_DATABASE_USER, "SQL current_user 不匹配"
        assert server_host == EXPECTED_DATABASE_HOST, "SQL server address 不匹配"
        assert int(server_port) == EXPECTED_DATABASE_PORT, "SQL server port 不匹配"

        versions = list((await db.execute(text("SELECT version_num FROM alembic_version"))).scalars())
        assert versions == [EXPECTED_ALEMBIC_HEAD], (
            f"Alembic head 不匹配: expected={EXPECTED_ALEMBIC_HEAD} actual={versions}"
        )
        table_names = {
            str(value)
            for value in (
                await db.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
                        "AND table_name <> 'alembic_version'"
                    )
                )
            ).scalars()
        }
        missing_tables = sorted(CRITICAL_BUSINESS_TABLES - table_names)
        assert not missing_tables, f"关键业务表缺失: {missing_tables}"

        nonempty: dict[str, int] = {}
        for table_name in sorted(table_names):
            assert re.fullmatch(r"[a-z][a-z0-9_]*", table_name), f"拒绝检查异常 public 表名: {table_name!r}"
            count = int(await db.scalar(text(f'SELECT count(*) FROM "{table_name}"')) or 0)
            if count:
                nonempty[table_name] = count
        assert not nonempty, f"任务数据库不是空库，拒绝写入: {nonempty}"
        await db.rollback()

    return {
        "database": async_fingerprint | {"sync_driver": sync_fingerprint["driver"]},
        "server": {
            "database_host": server_host,
            "database_port": int(server_port),
            "api_base_url": BASE_URL,
            "redis_host": EXPECTED_REDIS_HOST,
            "redis_port": EXPECTED_REDIS_PORT,
        },
        "redis": redis_fingerprint,
        "alembic_head": EXPECTED_ALEMBIC_HEAD,
    }


async def main() -> None:
    started_at = datetime.now(UTC)
    assert_no_run_artifacts(ARTIFACT_PATHS)
    intended_fingerprint = {
        "database": assert_database_url("DATABASE_URL", settings.database_url),
        "server": {
            "api_base_url": BASE_URL,
            "database_host": EXPECTED_DATABASE_HOST,
            "database_port": EXPECTED_DATABASE_PORT,
            "redis_host": EXPECTED_REDIS_HOST,
            "redis_port": EXPECTED_REDIS_PORT,
        },
        "redis": assert_redis_urls(
            settings.redis_url,
            settings.celery_broker_url,
            settings.celery_result_backend,
        ),
    }
    write_signed_exclusive(
        SEED_MANIFEST_PATH,
        {
            "status": "running",
            "run_id": VALIDATION_RUN_ID,
            "started_at": started_at.isoformat(),
            "finished_at": None,
            "fingerprint": intended_fingerprint,
            "seed_ids": SEED_IDS,
        },
    )

    verified_fingerprint = await assert_pristine_validation_database()
    password_hash = hash_password("MtvValidation!2026")
    user_ids = {role: item["user_id"] for role, item in SEED_IDS["users"].items()}
    usernames = {role: item["username"] for role, item in SEED_IDS["users"].items()}
    credit_ids = SEED_IDS["credit_accounts"]
    async with async_session_factory() as db:
        db.add_all(
            [
                Organization(
                    id=ORG_ID,
                    org_name=f"MTV 验收组织 {SEED_IDS['run_hash']}",
                    org_code=SEED_IDS["org_codes"]["primary"],
                ),
                Organization(
                    id=FOREIGN_ORG_ID,
                    org_name=f"MTV 隔离验收组织 {SEED_IDS['run_hash']}",
                    org_code=SEED_IDS["org_codes"]["foreign"],
                ),
            ]
        )
        users = [
            User(
                user_id=user_ids["owner"],
                username=usernames["owner"],
                password_hash=password_hash,
                enabled=True,
                is_admin=False,
                account_type="team",
                org_id=ORG_ID,
                can_manage_accounts=True,
                management_scope="org",
            ),
            User(
                user_id=user_ids["member"],
                username=usernames["member"],
                password_hash=password_hash,
                enabled=True,
                is_admin=False,
                account_type="team",
                org_id=ORG_ID,
                can_manage_accounts=False,
                management_scope="none",
            ),
            User(
                user_id=user_ids["admin"],
                username=usernames["admin"],
                password_hash=password_hash,
                enabled=True,
                is_admin=True,
                account_type="team",
                org_id=ORG_ID,
                can_manage_accounts=True,
                management_scope="org",
            ),
            User(
                user_id=user_ids["foreign"],
                username=usernames["foreign"],
                password_hash=password_hash,
                enabled=True,
                is_admin=False,
                account_type="team",
                org_id=FOREIGN_ORG_ID,
                can_manage_accounts=False,
                management_scope="none",
            ),
        ]
        db.add_all(users)
        await db.flush()
        db.add_all(
            [
                UserOrganization(user_id=user.user_id, org_id=user.org_id)
                for user in users
            ]
        )
        db.add_all(
            [
                CreditAccount(
                    id=credit_ids["org"],
                    owner_type="org",
                    owner_id=ORG_ID,
                    org_id=ORG_ID,
                    balance_points=100_000,
                    frozen_points=0,
                    status="active",
                ),
                CreditAccount(
                    id=credit_ids["owner"],
                    owner_type="user",
                    owner_id=user_ids["owner"],
                    org_id=ORG_ID,
                    balance_points=10_000,
                    frozen_points=0,
                    status="active",
                ),
                CreditAccount(
                    id=credit_ids["member"],
                    owner_type="user",
                    owner_id=user_ids["member"],
                    org_id=ORG_ID,
                    balance_points=1_000,
                    frozen_points=0,
                    status="active",
                ),
                CreditAccount(
                    id=credit_ids["admin"],
                    owner_type="user",
                    owner_id=user_ids["admin"],
                    org_id=ORG_ID,
                    balance_points=10_000,
                    frozen_points=0,
                    status="active",
                ),
            ]
        )
        await db.commit()

    finished_at = datetime.now(UTC)
    replace_signed_claim(
        SEED_MANIFEST_PATH,
        {
            "status": "passed",
            "run_id": VALIDATION_RUN_ID,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "fingerprint": verified_fingerprint,
            "seed_ids": SEED_IDS,
        },
    )
    print(
        f"seeded run_hash={SEED_IDS['run_hash']} users=4 organizations=2 credit_accounts=4",
        flush=True,
    )


asyncio.run(main())
