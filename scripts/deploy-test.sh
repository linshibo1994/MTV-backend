#!/usr/bin/env bash

# 测试服务器一键部署：Docker 内 PostgreSQL/Redis + 宿主机本地媒体目录。
# 默认部署当前 Git 工作区；不拉取代码、不写入或覆盖服务器环境文件。

set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
BACKEND_DIR="$PROJECT_ROOT/backend"
COMPOSE_FILE="$BACKEND_DIR/docker-compose.test.yml"
DEFAULT_ENV_FILE="$BACKEND_DIR/deploy/test.env"
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

select_env_file() {
    if [[ -n "${MTV_TEST_ENV_FILE:-}" ]]; then
        ENV_FILE="$MTV_TEST_ENV_FILE"
    elif [[ -f /etc/mtv/test.env ]]; then
        ENV_FILE="/etc/mtv/test.env"
    else
        ENV_FILE="$DEFAULT_ENV_FILE"
    fi

    [[ -f "$ENV_FILE" && -r "$ENV_FILE" ]] || {
        echo "测试环境文件不可读：$ENV_FILE" >&2
        exit 1
    }
    ENV_FILE="$(cd -- "$(dirname -- "$ENV_FILE")" && pwd -P)/$(basename -- "$ENV_FILE")"
    export MTV_ENV_FILE="$ENV_FILE"
}

prepare_compose() {
    select_env_file
    MTV_TEST_STORAGE_DIR="${MTV_TEST_STORAGE_DIR:-/srv/mtv-test/storage}"
    case "$MTV_TEST_STORAGE_DIR" in
        /*) ;;
        *)
            echo "MTV_TEST_STORAGE_DIR 必须是绝对路径：$MTV_TEST_STORAGE_DIR" >&2
            exit 1
            ;;
    esac
    export MTV_TEST_STORAGE_DIR
    mkdir -p "$MTV_TEST_STORAGE_DIR"

    COMPOSE=(
        docker compose
        --project-name "${MTV_TEST_PROJECT_NAME:-mtv-test}"
        --env-file "$ENV_FILE"
        -f "$COMPOSE_FILE"
    )
}

compose() {
    "${COMPOSE[@]}" "$@"
}

wait_for_service_health() {
    local service="$1"
    local container_id status attempt
    for attempt in {1..30}; do
        container_id="$(compose ps -q "$service")"
        if [[ -n "$container_id" ]]; then
            status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id")"
            if [[ "$status" == "healthy" || "$status" == "running" ]]; then
                return 0
            fi
        fi
        sleep 2
    done
    echo "$service 未在 60 秒内就绪。" >&2
    compose logs --tail 100 "$service" >&2 || true
    return 1
}

wait_for_api() {
    local attempt
    for attempt in {1..30}; do
        if compose exec -T api python -c '
from urllib.request import urlopen
import json
payload = json.load(urlopen("http://127.0.0.1:8000/v3api/health", timeout=3))
assert payload == {"status": "ok", "env": "test"}, payload
' >/dev/null 2>&1; then
            echo "测试 API 健康检查通过：http://127.0.0.1:${MTV_TEST_API_PORT:-8000}/v3api/health"
            return 0
        fi
        sleep 2
    done
    echo "API 未通过健康检查。" >&2
    compose logs --tail 100 api celery-worker celery-beat >&2 || true
    return 1
}

validate() {
    require_docker
    prepare_compose
    compose config --quiet
    echo "测试部署配置有效。环境文件：$ENV_FILE"
}

deploy() {
    validate
    echo "构建测试镜像…"
    compose build
    echo "启动测试 PostgreSQL 与 Redis…"
    compose up -d postgres redis
    wait_for_service_health postgres
    wait_for_service_health redis
    echo "执行数据库迁移…"
    compose run --rm --no-deps migrate
    echo "启动测试 API、worker 与单实例 beat…"
    compose up -d --remove-orphans api celery-worker celery-beat
    wait_for_api
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
        compose stop
        echo "测试服务已停止；PostgreSQL、Redis、beat 与媒体持久化数据均已保留。"
        ;;
    *)
        echo "用法：$0 [deploy|validate|status|logs|stop]" >&2
        exit 2
        ;;
esac
