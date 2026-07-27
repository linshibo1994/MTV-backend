#!/usr/bin/env bash

# 生产服务器一键部署：只使用外部 PostgreSQL、Redis 与私有 S3 桶。
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

require_env_value() {
    local key="$1"
    [[ -n "$(read_env_value "$key")" ]] || {
        echo "生产环境文件缺少非空配置：$key" >&2
        exit 1
    }
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
            echo "生产环境文件不能对其他用户开放读取权限（当前权限：$mode）。" >&2
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
        STORAGE_DRIVER S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET S3_REGION; do
        require_env_value "$key"
    done

    app_env="$(read_env_value APP_ENV)"
    storage_driver="$(read_env_value STORAGE_DRIVER)"
    session_secret="$(read_env_value SESSION_SECRET)"
    [[ "$app_env" == "prod" || "$app_env" == "production" ]] || {
        echo "生产部署要求 APP_ENV=prod 或 production。" >&2
        exit 1
    }
    [[ "$storage_driver" == "s3" ]] || {
        echo "生产部署要求 STORAGE_DRIVER=s3，禁止挂载服务器本地媒体目录。" >&2
        exit 1
    }
    [[ ${#session_secret} -ge 32 ]] || {
        echo "SESSION_SECRET 至少需要 32 个字符。" >&2
        exit 1
    }
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
    compose run --rm --no-deps --entrypoint python migrate -c '
from app.core.config import settings
from app.providers.storage import get_storage
assert settings.is_prod, "APP_ENV 必须是生产环境"
assert settings.storage_driver == "s3", "生产环境必须使用 S3"
get_storage()
print("生产容器配置校验通过")
'
}

check_external_dependencies() {
    compose run --rm --no-deps --entrypoint python migrate -c '
import asyncio
import boto3
import asyncpg
from redis.asyncio import from_url

from app.core.config import settings

async def main() -> None:
    connection = await asyncpg.connect(settings.database_url, timeout=10)
    try:
        assert await connection.fetchval("SELECT 1") == 1
    finally:
        await connection.close()
    redis = from_url(settings.redis_url, decode_responses=True)
    try:
        assert await redis.ping()
    finally:
        await redis.aclose()
    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint or None,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region or None,
    )
    await asyncio.to_thread(client.head_bucket, Bucket=settings.s3_bucket)
    print("外部 PostgreSQL、Redis 与 S3 连通性校验通过")

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
    compose logs --tail 100 api celery-worker celery-beat >&2 || true
    return 1
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
    echo "构建生产镜像（包含 S3 依赖）…"
    compose build
    check_container_configuration
    check_external_dependencies

    echo "停止旧版 API、worker 与 beat，避免迁移期间并发写入…"
    compose stop api celery-worker celery-beat || true
    echo "执行一次性数据库迁移…"
    compose run --rm --no-deps migrate
    echo "启动新版 API、worker 与唯一 beat…"
    compose up -d --remove-orphans api celery-worker celery-beat
    wait_for_api
    assert_running api
    assert_running celery-worker
    assert_running celery-beat
    echo "生产部署完成。"
    compose ps
}

case "$ACTION" in
    deploy) deploy ;;
    validate) validate ;;
    status)
        require_docker
        prepare_compose
        compose ps
        ;;
    logs)
        require_docker
        prepare_compose
        compose logs --tail "${MTV_LOG_TAIL:-200}" api celery-worker celery-beat
        ;;
    stop)
        require_docker
        prepare_compose
        compose stop api celery-worker celery-beat
        echo "生产应用容器已停止；外部 PostgreSQL、Redis 与 S3 未被修改。"
        ;;
    *)
        echo "用法：$0 [deploy|validate|status|logs|stop]" >&2
        exit 2
        ;;
esac
