#!/usr/bin/env bash

# 生产服务器一键部署：使用单机 Docker 栈中的 PostgreSQL、Redis 与受管对象存储。
# 单机本地媒体目录仅在环境文件显式确认后启用，避免误将生产媒体写入容器宿主机。
# 默认读取 /etc/mtv/production.env；该文件必须由运维或密钥管理系统预先创建。

set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
BACKEND_DIR="$PROJECT_ROOT/backend"
COMPOSE_FILE="$BACKEND_DIR/docker-compose.production.yml"
ACTION="${1:-deploy}"

require_docker() {
    command -v docker >/dev/null 2>&1 || {
        echo "未找到 docker，请先安装 Docker Engine。" >&2
        exit 1
    }
    docker compose version >/dev/null 2>&1 || {
        echo "未找到 Docker Compose v2，请先安装 docker compose 插件。" >&2
        exit 1
    }
    docker info >/dev/null 2>&1 || {
        echo "Docker 守护进程不可用，或当前用户没有 Docker 访问权限。" >&2
        exit 1
    }
}

validate_loaded_image_platform() {
    local component="$1"
    local image="$2"
    local expected_platform="linux/amd64"
    local actual_platform

    actual_platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image" 2>/dev/null || true)"
    [[ "$actual_platform" == "$expected_platform" ]] || {
        echo "$component 镜像平台不兼容：${actual_platform:-未知}，服务器离线部署要求 ${expected_platform}。" >&2
        exit 1
    }
}

load_prebuilt_component_image() {
    local component="$1"
    local archive_variable="$2"
    local image_variable="$3"
    local default_image="$4"
    local archive image

    archive="${!archive_variable-}"
    image="$(read_compose_image_name "$image_variable" "$default_image")"

    [[ -n "$archive" && -f "$archive" && -r "$archive" ]] || {
        echo "$component 预构建镜像包不可读：$archive" >&2
        echo "请设置 $archive_variable 为服务器上可读取的镜像归档路径。" >&2
        exit 1
    }

    echo "加载 $component 预构建镜像：$archive"
    docker load --input "$archive"

    docker image inspect "$image" >/dev/null 2>&1 || {
        echo "$component 镜像包中未找到预期镜像标签：$image" >&2
        echo "请确认本地 docker save 的镜像标签与 $image_variable 一致。" >&2
        exit 1
    }
    validate_loaded_image_platform "$component" "$image"

    echo "$component 预构建镜像已就绪：$image"
}

load_prebuilt_image() {
    load_prebuilt_component_image \
        "后端" "MTV_IMAGE_ARCHIVE" "MTV_PRODUCTION_IMAGE" "mtv-backend:production"
}

load_prebuilt_dependency_images() {
    load_prebuilt_component_image \
        "PostgreSQL" "MTV_POSTGRES_IMAGE_ARCHIVE" "MTV_POSTGRES_IMAGE" "postgres:16-alpine"
    load_prebuilt_component_image \
        "Redis" "MTV_REDIS_IMAGE_ARCHIVE" "MTV_REDIS_IMAGE" "redis:7-alpine"
}

load_deployment_images() {
    case "${MTV_USE_PREBUILT_IMAGE:-true}" in
        true)
            load_prebuilt_image
            ;;
        false)
            [[ "${MTV_ALLOW_SERVER_BUILD:-false}" == "true" ]] || {
                echo "为避免在生产服务器意外构建镜像，请设置 MTV_USE_PREBUILT_IMAGE=true。" >&2
                echo "仅在确有必要时，才可同时设置 MTV_USE_PREBUILT_IMAGE=false 和 MTV_ALLOW_SERVER_BUILD=true。" >&2
                exit 1
            }
            echo "显式允许在服务器构建后端生产镜像…"
            compose build
            ;;
        *)
            echo "MTV_USE_PREBUILT_IMAGE 只能是 true 或 false。" >&2
            exit 2
            ;;
    esac

    # 数据库和 Redis 在单机生产栈中必须来自离线归档，禁止部署时隐式拉取镜像。
    load_prebuilt_dependency_images
}

read_env_value() {
    local key="$1"
    awk -v key="$key" '
        $0 !~ /^[[:space:]]*#/ && $0 ~ "^[[:space:]]*" key "=" {
            sub(/^[^=]*=/, "")
            value = $0
        }
        END {
            sub(/\r$/, "", value)
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            if ((value ~ /^".*"$/) || (value ~ /^\047.*\047$/)) {
                value = substr(value, 2, length(value) - 2)
            }
            print value
        }
    ' "$ENV_FILE" | tr -d '\r'
}

read_compose_image_name() {
    local key="$1"
    local default_value="$2"
    local value="${!key-}"

    if [[ -z "$value" && -n "${ENV_FILE:-}" ]]; then
        value="$(read_env_value "$key")"
    fi
    printf '%s' "${value:-$default_value}"
}

require_env_value() {
    local key="$1"
    [[ -n "$(read_env_value "$key")" ]] || {
        echo "生产环境文件缺少非空配置：$key" >&2
        exit 1
    }
}

validate_persistent_directory_value() {
    local key="$1"
    local directory

    directory="$(read_env_value "$key")"
    [[ "$directory" == /* && "$directory" != "/" ]] || {
        echo "$key 必须是非根目录的绝对路径。" >&2
        exit 1
    }
}

ensure_persistent_directory() {
    local key="$1"
    local directory

    directory="$(read_env_value "$key")"
    [[ -e "$directory" && ! -d "$directory" ]] && {
        echo "$key 指向的路径不是目录：$directory" >&2
        exit 1
    }
    if [[ ! -d "$directory" ]]; then
        echo "创建持久化数据目录：$directory"
        mkdir -p -m 0750 -- "$directory"
    fi
    [[ -r "$directory" && -w "$directory" && -x "$directory" ]] || {
        echo "持久化数据目录不可由当前部署用户读写：$directory" >&2
        exit 1
    }
}

validate_single_host_service_urls() {
    local database_url database_sync_url redis_url broker_url result_backend_url

    database_url="$(read_env_value DATABASE_URL)"
    database_sync_url="$(read_env_value DATABASE_SYNC_URL)"
    redis_url="$(read_env_value REDIS_URL)"
    broker_url="$(read_env_value CELERY_BROKER_URL)"
    result_backend_url="$(read_env_value CELERY_RESULT_BACKEND)"

    [[ "$database_url" == postgresql+asyncpg://*@postgres:5432/* ]] || {
        echo "单机 Docker 栈要求 DATABASE_URL 使用 postgres:5432 服务名，禁止使用 localhost。" >&2
        exit 1
    }
    [[ "$database_sync_url" == postgresql+psycopg2://*@postgres:5432/* ]] || {
        echo "单机 Docker 栈要求 DATABASE_SYNC_URL 使用 postgres:5432 服务名，禁止使用 localhost。" >&2
        exit 1
    }
    [[ "$redis_url" == redis://:*@redis:6379/0* ]] || {
        echo "单机 Docker 栈要求 REDIS_URL 使用带认证的 redis:6379/0 服务地址。" >&2
        exit 1
    }
    [[ "$broker_url" == redis://:*@redis:6379/1* ]] || {
        echo "单机 Docker 栈要求 CELERY_BROKER_URL 使用带认证的 redis:6379/1 服务地址。" >&2
        exit 1
    }
    [[ "$result_backend_url" == redis://:*@redis:6379/2* ]] || {
        echo "单机 Docker 栈要求 CELERY_RESULT_BACKEND 使用带认证的 redis:6379/2 服务地址。" >&2
        exit 1
    }
}

validate_single_host_stack_env() {
    local key

    for key in \
        MTV_POSTGRES_USER MTV_POSTGRES_PASSWORD MTV_POSTGRES_DB \
        MTV_REDIS_PASSWORD MTV_POSTGRES_DATA_DIR MTV_REDIS_DATA_DIR; do
        require_env_value "$key"
    done
    validate_persistent_directory_value MTV_POSTGRES_DATA_DIR
    validate_persistent_directory_value MTV_REDIS_DATA_DIR
    validate_single_host_service_urls
}

ensure_single_host_data_directories() {
    ensure_persistent_directory MTV_POSTGRES_DATA_DIR
    ensure_persistent_directory MTV_REDIS_DATA_DIR
    ensure_local_storage_directory
}

validate_local_storage_env() {
    local storage_root legacy_host_path local_storage_allowed

    local_storage_allowed="$(read_env_value MTV_ALLOW_LOCAL_STORAGE)"
    [[ "$local_storage_allowed" == "true" ]] || {
        echo "单机本地媒体存储需要在生产环境文件中显式设置 MTV_ALLOW_LOCAL_STORAGE=true。" >&2
        exit 1
    }

    require_env_value STORAGE_ROOT
    require_env_value MTV_LEGACY_STORAGE_HOST_PATH
    storage_root="$(read_env_value STORAGE_ROOT)"
    legacy_host_path="$(read_env_value MTV_LEGACY_STORAGE_HOST_PATH)"
    [[ "$storage_root" == "/var/lib/mtv/storage" ]] || {
        echo "单机本地媒体存储要求容器内 STORAGE_ROOT=/var/lib/mtv/storage。" >&2
        exit 1
    }
    validate_persistent_directory_value MTV_LEGACY_STORAGE_HOST_PATH
    [[ "$legacy_host_path" == /opt/mtv/* ]] || {
        echo "单机本地媒体目录必须位于 /opt/mtv 下：$legacy_host_path" >&2
        exit 1
    }
}

ensure_local_storage_directory() {
    [[ "$(read_env_value STORAGE_DRIVER)" == "local" ]] || return 0
    ensure_persistent_directory MTV_LEGACY_STORAGE_HOST_PATH
}

select_env_file() {
    ENV_FILE="${MTV_PRODUCTION_ENV_FILE:-/etc/mtv/production.env}"
    [[ -f "$ENV_FILE" && -r "$ENV_FILE" ]] || {
        echo "生产环境文件不可读：$ENV_FILE" >&2
        echo "请在服务器受保护目录创建该文件，禁止放入仓库。" >&2
        exit 1
    }
    ENV_FILE="$(cd -- "$(dirname -- "$ENV_FILE")" && pwd -P)/$(basename -- "$ENV_FILE")"
    export MTV_ENV_FILE="$ENV_FILE"
}

check_env_permissions() {
    local mode other_bits
    mode="$(stat -c '%a' "$ENV_FILE" 2>/dev/null || true)"
    if [[ -n "$mode" ]]; then
        other_bits="${mode: -1}"
        if [[ "$other_bits" != "0" ]]; then
            echo "生产环境文件不能对其他用户开放读取权限（当前权限：${mode}）。" >&2
            exit 1
        fi
    fi
}

validate_production_env() {
    local app_env storage_driver session_secret
    for key in \
        APP_ENV SESSION_SECRET CORS_ORIGINS \
        DATABASE_URL DATABASE_SYNC_URL \
        REDIS_URL CELERY_BROKER_URL CELERY_RESULT_BACKEND \
        STORAGE_DRIVER; do
        require_env_value "$key"
    done

    app_env="$(read_env_value APP_ENV)"
    storage_driver="$(read_env_value STORAGE_DRIVER)"
    session_secret="$(read_env_value SESSION_SECRET)"
    [[ "$app_env" == "prod" || "$app_env" == "production" ]] || {
        echo "生产部署要求 APP_ENV=prod 或 production。" >&2
        exit 1
    }
    case "$storage_driver" in
        oss)
            for key in \
                MTV_INSTALL_ALIYUN_OSS \
                ALIYUN_OSS_ENDPOINT ALIYUN_OSS_ACCESS_KEY_ID \
                ALIYUN_OSS_ACCESS_KEY_SECRET ALIYUN_OSS_BUCKET \
                ALIYUN_OSS_REGION ALIYUN_OSS_DATA_MIGRATION_CONFIRMED \
                ALIYUN_OSS_CANARY_CONFIRMED; do
                require_env_value "$key"
            done
            [[ "$(read_env_value MTV_INSTALL_ALIYUN_OSS)" == "true" ]] || {
                echo "启用阿里云 OSS 时必须设置 MTV_INSTALL_ALIYUN_OSS=true，确保生产镜像包含官方 V2 SDK。" >&2
                exit 1
            }
            [[ "$(read_env_value ALIYUN_OSS_DATA_MIGRATION_CONFIRMED)" == "true" ]] || {
                echo "启用阿里云 OSS 前必须完成旧对象按 storage_key 的迁移校验、排空非终态存储任务，并设置 ALIYUN_OSS_DATA_MIGRATION_CONFIRMED=true。" >&2
                exit 1
            }
            [[ "$(read_env_value ALIYUN_OSS_CANARY_CONFIRMED)" == "true" ]] || {
                echo "启用阿里云 OSS 前必须确认专用 Bucket 已关闭版本控制，并设置 ALIYUN_OSS_CANARY_CONFIRMED=true。" >&2
                exit 1
            }
            ;;
        local)
            validate_local_storage_env
            ;;
        *)
            echo "生产部署要求 STORAGE_DRIVER=oss，或经显式确认的 local。" >&2
            exit 1
            ;;
    esac
    [[ ${#session_secret} -ge 32 ]] || {
        echo "SESSION_SECRET 至少需要 32 个字符。" >&2
        exit 1
    }
    validate_single_host_stack_env
}

prepare_compose() {
    select_env_file
    check_env_permissions
    validate_production_env
    COMPOSE=(
        docker compose
        --project-name "${MTV_PRODUCTION_PROJECT_NAME:-mtv-production}"
        --env-file "$ENV_FILE"
        -f "$COMPOSE_FILE"
    )
}

compose() {
    "${COMPOSE[@]}" "$@"
}

check_container_configuration() {
    compose run --rm --no-deps --entrypoint python api -c '
import os
from pathlib import Path

from app.core.config import settings
from app.providers.storage import get_storage

assert settings.is_prod, "APP_ENV 必须是生产环境"
if settings.storage_driver == "local":
    assert os.environ.get("MTV_ALLOW_LOCAL_STORAGE") == "true", "本地媒体目录未获显式授权"
    assert settings.storage_root == "/var/lib/mtv/storage", "本地媒体目录容器路径不符合部署约定"
    assert Path(settings.storage_root).is_dir(), "本地媒体目录未正确挂载"
else:
    assert settings.storage_driver == "oss", "生产环境存储类型无效"
get_storage()
print("生产容器配置校验通过")
'
}

check_external_dependencies() {
    compose run --rm --no-deps --entrypoint python api -c '
import asyncio
import asyncpg
from redis.asyncio import from_url

from app.core.config import settings
from app.providers.storage import get_storage

async def main() -> None:
    database_url = settings.database_url.replace(
        "postgresql+asyncpg://",
        "postgresql://",
        1,
    )
    connection = await asyncpg.connect(database_url, timeout=10)
    try:
        assert await connection.fetchval("SELECT 1") == 1
    finally:
        await connection.close()
    redis = from_url(settings.redis_url, decode_responses=True)
    try:
        assert await redis.ping()
    finally:
        await redis.aclose()
    if settings.storage_driver == "oss":
        storage = get_storage()
        await storage.exists("generate_images/_system/aliyun-oss-connectivity-probe")
        storage_name = "阿里云 OSS"
    else:
        assert settings.storage_driver == "local"
        storage_name = "本地媒体目录"
    print(f"单机 PostgreSQL、Redis 与 {storage_name} 连通性校验通过")

asyncio.run(main())
'
}

run_aliyun_oss_canary() {
    [[ "$(read_env_value STORAGE_DRIVER)" == "oss" ]] || return 0
    compose run --rm --no-deps --entrypoint python api -c '
import asyncio
import hashlib
import uuid
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from app.providers.storage import get_storage

CANARY_ID = uuid.uuid4().hex
SOURCE_A = f"generate_images/_system/aliyun-oss-canary-{CANARY_ID}-a.bin"
SOURCE_B = f"generate_images/_system/aliyun-oss-canary-{CANARY_ID}-b.bin"
FINAL = f"generate_images/_system/aliyun-oss-canary-{CANARY_ID}-final.bin"
PAYLOAD_A = b"mtv-aliyun-oss-canary-a-v1\\n"
PAYLOAD_B = b"mtv-aliyun-oss-canary-b-v1\\n"
ALL_KEYS = (SOURCE_A, SOURCE_B, FINAL)

def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)

def read_http_range(url: str) -> bytes:
    request = Request(url, headers={"Range": "bytes=0-0"})
    with urlopen(request, timeout=10) as response:
        return response.read()

def require_anonymous_read_denied(signed_url: str) -> None:
    parsed = urlsplit(signed_url)
    unsigned_url = parsed._replace(query="", fragment="").geturl()
    request = Request(unsigned_url, headers={"Range": "bytes=0-0"})
    try:
        with urlopen(request, timeout=10) as response:
            response.read(1)
    except HTTPError as exc:
        require(exc.code == 403, f"OSS 匿名读取返回异常状态：{exc.code}")
        return
    raise RuntimeError("OSS 对象允许匿名读取；请关闭 Bucket 公共访问")

async def cleanup(storage) -> list[str]:
    errors: list[str] = []
    for key in ALL_KEYS:
        try:
            await storage.delete(key)
        except Exception as exc:
            errors.append(f"delete {key}: {type(exc).__name__}")
    for key in ALL_KEYS:
        try:
            if await storage.exists(key):
                errors.append(f"object still exists: {key}")
        except Exception as exc:
            errors.append(f"verify {key}: {type(exc).__name__}")
    return errors

async def main() -> None:
    storage = get_storage()
    expected: bytes | None = None
    primary_error: BaseException | None = None
    try:
        await storage.put(SOURCE_A, PAYLOAD_A)
        await storage.put(SOURCE_B, PAYLOAD_B)
        result_a, result_b = await asyncio.gather(
            storage.promote_if_absent(SOURCE_A, FINAL),
            storage.promote_if_absent(SOURCE_B, FINAL),
        )
        require(
            sorted((result_a, result_b)) == [False, True],
            "OSS 目标防覆盖语义不成立；请确认 Bucket Versioning=Disabled",
        )

        expected = PAYLOAD_A if result_a else PAYLOAD_B
        winner = SOURCE_A if result_a else SOURCE_B
        loser = SOURCE_B if result_a else SOURCE_A
        payload = await storage.get(FINAL)
        require(payload == expected, "OSS canary 最终对象内容不匹配")
        require(
            await storage.get_size(FINAL) == len(expected),
            "OSS canary 最终对象大小不匹配",
        )
        ranged = b"".join(
            [
                chunk
                async for chunk in storage.iter_chunks(
                    FINAL,
                    chunk_size=7,
                    start=1,
                    end=len(expected) - 2,
                )
            ]
        )
        require(ranged == expected[1:-1], "OSS canary Range 读取内容不匹配")
        signed_url = await storage.signed_url(FINAL, expires_seconds=60)
        require(bool(signed_url), "OSS canary 未生成签名读取 URL")
        signed_payload = await asyncio.to_thread(read_http_range, signed_url)
        require(signed_payload == expected[:1], "OSS canary 签名 URL 读取内容不匹配")
        await asyncio.to_thread(require_anonymous_read_denied, signed_url)
        require(not await storage.exists(winner), "OSS canary 获胜源对象未删除")
        require(await storage.exists(loser), "OSS canary 失败方源对象未保留")
    except BaseException as exc:
        primary_error = exc

    cleanup_errors = await cleanup(storage)
    if primary_error is not None:
        if cleanup_errors:
            primary_error.add_note(f"OSS canary 清理异常：{cleanup_errors}")
        raise primary_error.with_traceback(primary_error.__traceback__)
    if cleanup_errors:
        raise RuntimeError(f"阿里云 OSS canary 清理失败：{cleanup_errors}")
    if expected is None:
        raise RuntimeError("阿里云 OSS canary 未产生预期对象")

    digest = hashlib.sha256(expected).hexdigest()
    print(f"阿里云 OSS 私有读写、签名 URL、Range、原子防覆盖与删除 canary 通过：sha256={digest}")

asyncio.run(main())
'
}

run_local_storage_canary() {
    [[ "$(read_env_value STORAGE_DRIVER)" == "local" ]] || return 0
    compose run --rm --no-deps --entrypoint python api -c '
import asyncio
import hashlib

from app.providers.storage import get_storage

CANARY_KEY = "generate_images/_system/local-storage-canary.txt"
CANARY_PAYLOAD = b"mtv-local-storage-canary-v1\\n"

async def main() -> None:
    storage = get_storage()
    stored = False
    try:
        stored_key = await storage.put(CANARY_KEY, CANARY_PAYLOAD)
        stored = True
        assert stored_key == CANARY_KEY
        payload = await storage.get(CANARY_KEY)
        assert payload == CANARY_PAYLOAD
        assert await storage.get_size(CANARY_KEY) == len(CANARY_PAYLOAD)
    finally:
        if stored:
            await storage.delete(CANARY_KEY)
    assert not await storage.exists(CANARY_KEY)
    digest = hashlib.sha256(CANARY_PAYLOAD).hexdigest()
    print(f"本地媒体目录读写删除 canary 通过：{len(CANARY_PAYLOAD)} bytes sha256={digest}")

asyncio.run(main())
'
}

wait_for_api() {
    local attempt
    for attempt in {1..30}; do
        if compose exec -T api python -c '
from urllib.request import urlopen
import json
payload = json.load(urlopen("http://127.0.0.1:8000/v3api/health", timeout=3))
assert payload == {"status": "ok", "env": "prod"} or payload == {"status": "ok", "env": "production"}, payload
' >/dev/null 2>&1; then
            echo "生产 API 健康检查通过（容器内）。"
            return 0
        fi
        sleep 2
    done
    echo "生产 API 未通过健康检查。" >&2
    compose logs --tail 100 postgres redis api celery-worker celery-beat >&2 || true
    return 1
}

wait_for_service_health() {
    local service="$1"
    local timeout_seconds="${MTV_DEPENDENCY_HEALTH_TIMEOUT_SECONDS:-120}"
    local started_at container health_status

    [[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]] || {
        echo "MTV_DEPENDENCY_HEALTH_TIMEOUT_SECONDS 必须是正整数。" >&2
        exit 2
    }
    started_at="$SECONDS"
    while ((SECONDS - started_at < timeout_seconds)); do
        container="$(compose ps -q "$service" 2>/dev/null || true)"
        if [[ -n "$container" ]]; then
            health_status="$(
                docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}missing-healthcheck{{end}}' \
                    "$container" 2>/dev/null || true
            )"
            case "$health_status" in
                healthy)
                    echo "$service 健康检查通过。"
                    return 0
                    ;;
                unhealthy|exited|dead)
                    echo "$service 未通过健康检查（状态：${health_status}）。" >&2
                    compose logs --tail 100 "$service" >&2 || true
                    return 1
                    ;;
            esac
        fi
        sleep 2
    done

    echo "等待 $service 健康检查超时（${timeout_seconds} 秒）；生产编排必须为该服务定义 healthcheck。" >&2
    compose ps "$service" >&2 || true
    compose logs --tail 100 "$service" >&2 || true
    return 1
}

start_and_wait_for_dependencies() {
    echo "启动单机 PostgreSQL 与 Redis，并等待健康检查…"
    compose up -d --no-build --remove-orphans postgres redis
    wait_for_service_health postgres
    wait_for_service_health redis
}

assert_running() {
    local service="$1"
    compose ps --status running --services | grep -Fx "$service" >/dev/null || {
        echo "服务未处于运行状态：$service" >&2
        compose logs --tail 100 "$service" >&2 || true
        return 1
    }
}

validate() {
    require_docker
    prepare_compose
    compose config --quiet
    echo "生产部署静态配置有效。环境文件：$ENV_FILE"
}

deploy() {
    validate
    load_deployment_images
    ensure_single_host_data_directories

    # 新镜像与 OSS 的所有失败关闭检查都在旧服务仍在线时完成，避免配置错误导致停机。
    start_and_wait_for_dependencies
    check_container_configuration
    check_external_dependencies
    run_aliyun_oss_canary

    echo "停止旧版 API、worker 与 beat，避免迁移期间并发写入…"
    compose stop api celery-worker celery-beat || true

    echo "执行一次性数据库迁移…"
    compose run --rm --no-deps migrate
    run_local_storage_canary
    echo "启动新版 API、worker 与唯一 beat…"
    compose up -d --no-build --remove-orphans api celery-worker celery-beat
    wait_for_api
    assert_running postgres
    assert_running redis
    assert_running api
    assert_running celery-worker
    assert_running celery-beat
    echo "生产部署完成。"
    compose ps postgres redis api celery-worker celery-beat
}

case "$ACTION" in
    deploy) deploy ;;
    validate) validate ;;
    status)
        require_docker
        prepare_compose
        compose ps postgres redis api celery-worker celery-beat
        ;;
    logs)
        require_docker
        prepare_compose
        compose logs --tail "${MTV_LOG_TAIL:-200}" postgres redis api celery-worker celery-beat
        ;;
    stop)
        require_docker
        prepare_compose
        compose stop api celery-worker celery-beat
        compose stop postgres redis || true
        echo "生产应用、PostgreSQL 与 Redis 容器已停止；持久化数据目录与外部对象存储未被删除。"
        ;;
    *)
        echo "用法：$0 [deploy|validate|status|logs|stop]" >&2
        exit 2
        ;;
esac
