#!/usr/bin/env bash

# x86_64 Linux 测试服务器部署脚本。
#
# 推荐流程：在服务器执行 git pull --ff-only 后，运行本脚本的 deploy（默认）命令。
# 数据库、Redis 和 Celery Beat 状态保存在 Docker 命名卷中；本脚本不会执行
# docker compose down -v、docker volume rm 等会删除持久化数据的操作。

set -Eeuo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
BACKEND_DIR="$PROJECT_ROOT/backend"
COMPOSE_FILE="$BACKEND_DIR/docker-compose.test.yml"
DEFAULT_ENV_FILE="$BACKEND_DIR/deploy/test.env"

ACTION="${1:-deploy}"
if (( $# > 0 )); then
    shift
fi
ACTION_ARGS=("$@")

ENV_FILE=""
TEST_PROJECT_NAME=""
TEST_STORAGE_DIR=""
BACKEND_IMAGE=""
DEPLOY_TIMEOUT_SECONDS="${MTV_DEPLOY_TIMEOUT_SECONDS:-120}"
declare -a COMPOSE=()

info() {
    echo "==> $*"
}

warn() {
    echo "警告：$*" >&2
}

fail() {
    echo "错误：$*" >&2
    exit 1
}

on_error() {
    local exit_code="$1"
    echo "部署未完成（退出码：${exit_code}）。数据库卷和媒体目录没有被删除；请查看 logs 命令输出后重试。" >&2
}

trap 'on_error "$?"' ERR

usage() {
    cat <<'EOF'
用法：./scripts/deploy-test.sh <命令> [参数]

命令：
  deploy                 构建最新后端镜像、执行迁移并启动全部服务（默认）
  start                  使用已有后端镜像执行迁移并启动全部服务
  restart                等同于 deploy：重新构建最新代码后安全重启
  stop                   停止全部容器，但保留数据库、Redis、Beat 和媒体数据
  build | build-backend  仅构建并校验后端镜像；可附加 Docker Compose build 参数
  migrate                使用已有镜像安全执行迁移，并重新启动应用服务
  validate               校验 Git 工作区、.env、Docker、x86_64 平台和 Compose 配置
  status [服务名...]     查看服务状态
  logs [Compose 参数...] 查看日志；例如：logs -f api
  images                 查看并校验当前后端及运行中服务的镜像平台
  help                   显示本帮助

环境变量：
  MTV_TEST_ENV_FILE                  指定 .env 文件；相对路径以仓库根目录为准
  MTV_TEST_PROJECT_NAME              Docker Compose 项目名，默认 mtv-test
  MTV_TEST_STORAGE_DIR               宿主机媒体目录，默认 /srv/mtv-test/storage
  MTV_DEPLOY_TIMEOUT_SECONDS         PostgreSQL、Redis、API 等待超时秒数，默认 120
  MTV_LOG_TAIL                       logs 默认显示的日志行数，默认 200
  MTV_ALLOW_DIRTY_WORKTREE=true      明确允许部署含未提交或未跟踪构建文件的 Git 工作区
  MTV_ALLOW_DATABASE_MIGRATION=true  确认已备份数据库，并允许执行 Alembic 写迁移
  MTV_MIGRATION_BACKUP_REFERENCE     迁移前备份/快照的标识，仅用于部署日志记录

说明：测试 Compose 固定使用容器内 PostgreSQL、Redis 和本地媒体目录；其连接地址、
APP_ENV、SESSION_SECRET、STORAGE_DRIVER/ROOT 由 docker-compose.test.yml 强制设置。
所选 .env 用于其余应用配置（例如 CORS、AI Provider 与密钥），不会覆盖上述测试栈配置。

示例：
  git pull --ff-only
  MTV_MIGRATION_BACKUP_REFERENCE=backup-20260812-1200 \
    MTV_ALLOW_DATABASE_MIGRATION=true ./scripts/deploy-test.sh deploy
  ./scripts/deploy-test.sh logs -f api
  ./scripts/deploy-test.sh build --no-cache
EOF
}

require_no_arguments() {
    (( ${#ACTION_ARGS[@]} == 0 )) || fail "${ACTION} 命令不接受额外参数；请运行 help 查看用法。"
}

require_positive_integer() {
    local name="$1"
    local value="$2"
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || fail "${name} 必须是正整数，当前值：${value}"
}

require_docker() {
    command -v docker >/dev/null 2>&1 || fail "未找到 docker，请先安装 Docker Engine。"
    docker compose version >/dev/null 2>&1 || fail "未找到 Docker Compose v2，请先安装 docker compose 插件。"
    docker info >/dev/null 2>&1 || fail "Docker 守护进程不可用，或当前用户没有 Docker 访问权限。"
}

require_linux_amd64() {
    local daemon_platform

    [[ "$(uname -s)" == "Linux" ]] || fail "此部署脚本仅支持 Linux 服务器。"
    case "$(uname -m)" in
        x86_64|amd64) ;;
        *) fail "服务器 CPU 架构必须为 x86_64/amd64，当前为：$(uname -m)" ;;
    esac

    daemon_platform="$(docker info --format '{{.OSType}}/{{.Architecture}}' 2>/dev/null || true)"
    case "$daemon_platform" in
        linux/amd64|linux/x86_64) ;;
        *) fail "Docker 守护进程平台必须为 linux/amd64，当前为：${daemon_platform:-未知}" ;;
    esac
}

require_clean_git_checkout() {
    local revision dirty_state

    command -v git >/dev/null 2>&1 || fail "未找到 git；请在已更新的 Git 工作区中执行部署。"
    git -C "$PROJECT_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
        fail "部署目录不是 Git 工作区：$PROJECT_ROOT"
    }

    revision="$(git -C "$PROJECT_ROOT" rev-parse --short=12 HEAD)"
    dirty_state="$(git -C "$PROJECT_ROOT" status --porcelain)"
    if [[ -n "$dirty_state" && "${MTV_ALLOW_DIRTY_WORKTREE:-false}" != "true" ]]; then
        fail "Git 工作区存在未提交或未跟踪文件；请先提交/清理，或仅在明确知情时设置 MTV_ALLOW_DIRTY_WORKTREE=true。"
    fi

    info "本次部署 Git 版本：$revision"
}

resolve_env_file_path() {
    local candidate="$1"
    local parent_directory

    if [[ "$candidate" != /* ]]; then
        candidate="$PROJECT_ROOT/$candidate"
    fi
    [[ -f "$candidate" && -r "$candidate" ]] || fail "环境文件不可读：$candidate"
    parent_directory="$(cd -- "$(dirname -- "$candidate")" && pwd -P)"
    printf '%s/%s' "$parent_directory" "$(basename -- "$candidate")"
}

select_env_file() {
    local candidate=""

    if [[ -n "${MTV_TEST_ENV_FILE:-}" ]]; then
        candidate="$MTV_TEST_ENV_FILE"
    else
        # 显式指定优先；其次沿用服务器受保护配置文件，最后才回退到仓库内 .env。
        for candidate in /etc/mtv/test.env "$BACKEND_DIR/.env" "$PROJECT_ROOT/.env" "$DEFAULT_ENV_FILE"; do
            [[ -f "$candidate" ]] && break
        done
    fi

    [[ -n "$candidate" && -f "$candidate" ]] || {
        fail "未找到 .env 文件；请创建 backend/.env，或设置 MTV_TEST_ENV_FILE 指向服务器上的环境文件。"
    }
    ENV_FILE="$(resolve_env_file_path "$candidate")"
    export MTV_ENV_FILE="$ENV_FILE"
}

# 只读取部署变量，不 source 环境文件，避免 .env 中的内容被当作 Shell 代码执行。
read_env_value() {
    local key="$1"

    awk -v key="$key" '
        {
            line = $0
            sub(/\r$/, "", line)
            if (line ~ /^[[:space:]]*#/) {
                next
            }
            if (line ~ "^[[:space:]]*" key "=") {
                sub("^[[:space:]]*" key "=", "", line)
                value = line
            }
        }
        END {
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            if ((value ~ /^".*"$/) || (value ~ /^\047.*\047$/)) {
                value = substr(value, 2, length(value) - 2)
            }
            print value
        }
    ' "$ENV_FILE"
}

configure_runtime_paths() {
    local configured_storage configured_project_name

    configured_storage="$(read_env_value MTV_TEST_STORAGE_DIR)"
    configured_project_name="$(read_env_value MTV_TEST_PROJECT_NAME)"
    TEST_STORAGE_DIR="${MTV_TEST_STORAGE_DIR:-${configured_storage:-/srv/mtv-test/storage}}"
    TEST_PROJECT_NAME="${MTV_TEST_PROJECT_NAME:-${configured_project_name:-mtv-test}}"

    [[ "$TEST_STORAGE_DIR" == /* && "$TEST_STORAGE_DIR" != "/" ]] || {
        fail "MTV_TEST_STORAGE_DIR 必须是非根目录的绝对路径，当前为：$TEST_STORAGE_DIR"
    }
    [[ -e "$TEST_STORAGE_DIR" && ! -d "$TEST_STORAGE_DIR" ]] && {
        fail "MTV_TEST_STORAGE_DIR 指向的路径不是目录：$TEST_STORAGE_DIR"
    }
    [[ "$TEST_PROJECT_NAME" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || {
        fail "MTV_TEST_PROJECT_NAME 只能包含小写字母、数字、下划线和连字符，且必须以字母或数字开头。"
    }

    # 确保脚本创建的目录与 Compose 变量展开结果一致。
    export MTV_TEST_STORAGE_DIR="$TEST_STORAGE_DIR"
}

check_env_file_permissions() {
    local mode group_digit other_digit

    mode="$(stat -c '%a' "$ENV_FILE" 2>/dev/null || true)"
    [[ "$mode" =~ ^[0-7]{3,4}$ ]] || return 0
    group_digit="${mode: -2:1}"
    other_digit="${mode: -1}"
    if [[ "$group_digit" =~ [4-7] || "$other_digit" =~ [4-7] ]]; then
        warn ".env 可能含有密钥，建议执行 chmod 600 '$ENV_FILE'（当前权限：$mode）。"
    fi
}

validate_existing_compose_project() {
    local expected_volume label volume
    local -a persistent_volumes=("test_postgres_data" "test_redis_data" "test_beat_schedule")

    for volume in "${persistent_volumes[@]}"; do
        expected_volume="${TEST_PROJECT_NAME}_${volume}"
        if ! docker volume inspect "$expected_volume" >/dev/null 2>&1; then
            continue
        fi
        label="$(docker volume inspect --format '{{ index .Labels "com.docker.compose.project" }}' "$expected_volume")"
        [[ "$label" == "$TEST_PROJECT_NAME" ]] || {
            fail "检测到同名持久化卷 $expected_volume，但其 Compose 项目标签异常：${label:-空}。为保护已有数据，已中止。"
        }
    done
}

warn_on_existing_project_name_change() {
    local project_label volume_name

    while IFS=$'\t' read -r project_label volume_name; do
        [[ -n "$project_label" && "$project_label" != "$TEST_PROJECT_NAME" ]] || continue
        warn "检测到已有 MTV 测试持久化卷：${volume_name}（项目名 ${project_label}）；当前项目名为 ${TEST_PROJECT_NAME}。改动项目名会使用新的空数据库，而不会迁移旧数据。"
    done < <(
        docker volume ls --format '{{.Label "com.docker.compose.project"}}\t{{.Name}}' \
            | awk -F '\t' '$1 ~ /_(test_postgres_data|test_redis_data|test_beat_schedule)$/ { print }'
    )
}

prepare_compose() {
    [[ -f "$COMPOSE_FILE" ]] || fail "未找到测试 Compose 文件：$COMPOSE_FILE"
    select_env_file
    configure_runtime_paths
    COMPOSE=(
        docker compose
        --project-name "$TEST_PROJECT_NAME"
        --env-file "$ENV_FILE"
        -f "$COMPOSE_FILE"
    )
}

compose() {
    "${COMPOSE[@]}" "$@"
}

resolve_backend_image() {
    local resolved_image

    # Compose 输出会按服务名称排序；同一镜像也被 migrate、worker、beat 复用。
    resolved_image="$(compose config --images | awk '$0 != "postgres:16-alpine" && $0 != "redis:7-alpine" { print; exit }')"
    BACKEND_IMAGE="${resolved_image:-${MTV_TEST_IMAGE:-mtv-backend:test}}"
    [[ -n "$BACKEND_IMAGE" ]] || fail "无法从 Compose 配置解析后端镜像名称。"
}

validate_static_configuration() {
    require_docker
    require_linux_amd64
    require_clean_git_checkout
    require_positive_integer MTV_DEPLOY_TIMEOUT_SECONDS "$DEPLOY_TIMEOUT_SECONDS"
    prepare_compose
    check_env_file_permissions
    compose config --quiet
    validate_existing_compose_project
    warn_on_existing_project_name_change
    resolve_backend_image
    info "静态校验通过：环境文件=$ENV_FILE，Compose 项目=$TEST_PROJECT_NAME，后端镜像=$BACKEND_IMAGE"
}

ensure_storage_directory() {
    if [[ ! -d "$TEST_STORAGE_DIR" ]]; then
        info "创建媒体持久化目录：$TEST_STORAGE_DIR"
        mkdir -p -- "$TEST_STORAGE_DIR"
    fi
    [[ -r "$TEST_STORAGE_DIR" && -w "$TEST_STORAGE_DIR" && -x "$TEST_STORAGE_DIR" ]] || {
        fail "媒体持久化目录不可由当前部署用户读写：$TEST_STORAGE_DIR"
    }
}

validate_image_platform() {
    local component="$1"
    local image="$2"
    local platform

    docker image inspect "$image" >/dev/null 2>&1 || fail "$component 镜像不存在：$image"
    platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image" 2>/dev/null || true)"
    [[ "$platform" == "linux/amd64" ]] || {
        fail "$component 镜像平台不兼容：${platform:-未知}；服务器要求 linux/amd64。"
    }
}

show_image_info() {
    local component="$1"
    local image="$2"

    docker image inspect --format \
        "$component：标签={{.RepoTags}}；ID={{.Id}}；平台={{.Os}}/{{.Architecture}}；创建时间={{.Created}}" \
        "$image"
}

verify_backend_image() {
    validate_image_platform "后端" "$BACKEND_IMAGE"
    show_image_info "后端镜像校验通过" "$BACKEND_IMAGE"
}

get_service_container_id() {
    local service="$1"
    compose ps -q "$service" 2>/dev/null || true
}

verify_service_image_platform() {
    local service="$1"
    local container_id image_id

    container_id="$(get_service_container_id "$service")"
    [[ -n "$container_id" ]] || fail "未找到服务容器：$service"
    image_id="$(docker inspect --format '{{.Image}}' "$container_id" 2>/dev/null || true)"
    [[ -n "$image_id" ]] || fail "无法读取服务镜像：$service"
    validate_image_platform "$service 服务" "$image_id"
}

assert_mount_type() {
    local service="$1"
    local destination="$2"
    local expected_type="$3"
    local container_id actual_type

    container_id="$(get_service_container_id "$service")"
    [[ -n "$container_id" ]] || fail "无法检查 $service 持久化挂载：服务未运行。"
    actual_type="$(
        docker inspect --format '{{range .Mounts}}{{println .Destination .Type}}{{end}}' "$container_id" \
            | awk -v destination="$destination" '$1 == destination { print $2; exit }'
    )"
    [[ "$actual_type" == "$expected_type" ]] || {
        fail "$service 的 $destination 必须使用 $expected_type 挂载，当前为：${actual_type:-未挂载}"
    }
}

wait_for_service_health() {
    local service="$1"
    local started_at container_id health_status

    started_at="$SECONDS"
    while (( SECONDS - started_at < DEPLOY_TIMEOUT_SECONDS )); do
        container_id="$(get_service_container_id "$service")"
        if [[ -n "$container_id" ]]; then
            health_status="$(
                docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}missing-healthcheck{{end}}' \
                    "$container_id" 2>/dev/null || true
            )"
            case "$health_status" in
                healthy)
                    info "$service 健康检查通过。"
                    return 0
                    ;;
                unhealthy|exited|dead)
                    compose logs --tail 100 "$service" >&2 || true
                    fail "$service 未通过健康检查（状态：$health_status）。"
                    ;;
            esac
        fi
        sleep 2
    done

    compose ps "$service" >&2 || true
    compose logs --tail 100 "$service" >&2 || true
    fail "等待 $service 健康检查超时（${DEPLOY_TIMEOUT_SECONDS} 秒）。"
}

start_and_verify_dependencies() {
    info "启动 PostgreSQL 与 Redis，并等待其健康检查…"
    compose up -d --no-build postgres redis
    wait_for_service_health postgres
    wait_for_service_health redis

    verify_service_image_platform postgres
    verify_service_image_platform redis
    assert_mount_type postgres /var/lib/postgresql/data volume
    assert_mount_type redis /data volume
    info "PostgreSQL 与 Redis 镜像、平台和持久化卷检查通过。"
}

check_container_configuration() {
    info "检查新后端镜像中的应用配置和媒体挂载…"
    compose run --rm --no-deps --entrypoint python api -c '
import os
from pathlib import Path

from app.core.config import settings
from app.providers.storage import get_storage

assert settings.app_env == "test", f"APP_ENV 必须为 test，当前为 {settings.app_env!r}"
assert settings.storage_driver == "local", f"测试环境必须使用 local 存储，当前为 {settings.storage_driver!r}"
assert settings.storage_root == "/data/storage", f"媒体容器路径异常：{settings.storage_root!r}"
storage_root = Path(settings.storage_root)
assert storage_root.is_dir(), "媒体目录没有正确挂载到容器"
assert os.access(storage_root, os.R_OK | os.W_OK | os.X_OK), "媒体目录在容器内不可读写"
get_storage()
print("后端镜像配置与媒体挂载检查通过")
'
}

check_internal_dependencies() {
    info "检查容器内到 PostgreSQL 和 Redis 的连通性…"
    compose run --rm --no-deps --entrypoint python api -c '
import asyncio

import asyncpg
from redis.asyncio import from_url

from app.core.config import settings


async def main() -> None:
    database_url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
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


asyncio.run(main())
print("PostgreSQL 与 Redis 连通性检查通过")
'
}

stop_application_services() {
    info "停止旧版 API、Celery Worker 和 Beat，避免迁移期间出现并发写入…"
    compose stop api celery-worker celery-beat
    assert_service_not_running api
    assert_service_not_running celery-worker
    assert_service_not_running celery-beat
}

verify_alembic_head() {
    local expected_heads current_heads

    expected_heads="$(
        compose run --rm --no-deps --entrypoint alembic migrate heads \
            | awk '/\(head\)/ { print $1 }' \
            | sort
    )"
    current_heads="$(
        compose run --rm --no-deps --entrypoint alembic migrate current \
            | awk '/\(head\)/ { print $1 }' \
            | sort
    )"

    [[ -n "$expected_heads" ]] || fail "未能从镜像读取 Alembic head revision。"
    [[ "$current_heads" == "$expected_heads" ]] || {
        echo "期望 Alembic head：$expected_heads" >&2
        echo "数据库当前版本：${current_heads:-未处于 head}" >&2
        fail "数据库迁移版本校验失败。"
    }
    info "Alembic 迁移已到达镜像中的 head revision：$current_heads"
}

require_migration_authorization() {
    [[ "${MTV_ALLOW_DATABASE_MIGRATION:-false}" == "true" ]] || {
        fail "数据库迁移是写操作。确认已完成数据库备份后，设置 MTV_ALLOW_DATABASE_MIGRATION=true 再执行 deploy、start、restart 或 migrate。"
    }
    [[ -n "${MTV_MIGRATION_BACKUP_REFERENCE:-}" ]] || {
        warn "未设置 MTV_MIGRATION_BACKUP_REFERENCE；请确认已有可恢复的数据库备份或卷快照。"
    }
    info "数据库迁移已获确认；备份标识：${MTV_MIGRATION_BACKUP_REFERENCE:-未提供}"
}

run_database_migrations() {
    require_migration_authorization
    info "执行数据库迁移：alembic upgrade head"
    compose run --rm --no-deps --entrypoint alembic migrate upgrade head
    verify_alembic_head
}

assert_service_running() {
    local service="$1"

    compose ps --status running --services | grep -Fx -- "$service" >/dev/null || {
        compose logs --tail 100 "$service" >&2 || true
        fail "服务未处于运行状态：$service"
    }
}

assert_service_not_running() {
    local service="$1"

    if compose ps --status running --services | grep -Fx -- "$service" >/dev/null; then
        compose ps "$service" >&2 || true
        fail "服务仍在运行，不能在其继续写入数据库时执行迁移：$service"
    fi
}

assert_single_beat() {
    local count

    count="$(
        docker ps -q \
            --filter "label=com.docker.compose.project=$TEST_PROJECT_NAME" \
            --filter 'label=com.docker.compose.service=celery-beat' \
            | wc -l | tr -d '[:space:]'
    )"
    [[ "$count" == "1" ]] || fail "Celery Beat 必须且只能运行一个实例，当前数量：$count"
}

wait_for_api() {
    local started_at

    started_at="$SECONDS"
    while (( SECONDS - started_at < DEPLOY_TIMEOUT_SECONDS )); do
        if compose exec -T api python -c '
import json
from urllib.request import urlopen

payload = json.load(urlopen("http://127.0.0.1:8000/v3api/health", timeout=3))
assert payload == {"status": "ok", "env": "test"}, payload
' >/dev/null 2>&1; then
            info "API 健康检查通过（容器内 /v3api/health）。"
            return 0
        fi
        sleep 2
    done

    compose logs --tail 100 api celery-worker celery-beat >&2 || true
    fail "API 未在 ${DEPLOY_TIMEOUT_SECONDS} 秒内通过健康检查。"
}

start_application_services() {
    info "启动新版 API、Celery Worker 和唯一的 Celery Beat…"
    compose up -d --no-build --force-recreate api celery-worker celery-beat
    wait_for_api

    assert_service_running postgres
    assert_service_running redis
    assert_service_running api
    assert_service_running celery-worker
    assert_service_running celery-beat
    assert_single_beat

    verify_service_image_platform api
    verify_service_image_platform celery-worker
    verify_service_image_platform celery-beat
    assert_mount_type api /data/storage bind
    assert_mount_type celery-worker /data/storage bind
    assert_mount_type celery-beat /var/lib/mtv volume
}

rollout_current_image() {
    ensure_storage_directory
    verify_backend_image
    start_and_verify_dependencies

    # 在旧版应用仍在线时验证新镜像；配置错误时不会中断已有服务。
    check_container_configuration
    check_internal_dependencies

    # 迁移失败时保持应用服务停止，避免旧代码继续写入新旧不一致的数据库结构。
    stop_application_services
    run_database_migrations
    start_application_services

    info "部署完成。"
    compose ps postgres redis api celery-worker celery-beat
}

build_backend_image() {
    info "构建后端镜像：$BACKEND_IMAGE"
    compose build "${ACTION_ARGS[@]}" api
    verify_backend_image
}

deploy() {
    validate_static_configuration
    build_backend_image
    rollout_current_image
}

start() {
    validate_static_configuration
    rollout_current_image
}

show_status() {
    require_docker
    prepare_compose
    compose ps "${ACTION_ARGS[@]}"
}

show_logs() {
    local log_tail="${MTV_LOG_TAIL:-200}"

    require_docker
    prepare_compose
    [[ "$log_tail" =~ ^[0-9]+$ ]] || fail "MTV_LOG_TAIL 必须是非负整数，当前值：$log_tail"
    compose logs --tail "$log_tail" "${ACTION_ARGS[@]}"
}

show_images() {
    local service container_id image_id

    require_docker
    require_linux_amd64
    prepare_compose
    compose config --quiet
    resolve_backend_image
    verify_backend_image

    for service in postgres redis api celery-worker celery-beat; do
        container_id="$(get_service_container_id "$service")"
        if [[ -z "$container_id" ]]; then
            info "$service 尚未运行，跳过容器镜像检查。"
            continue
        fi
        image_id="$(docker inspect --format '{{.Image}}' "$container_id")"
        validate_image_platform "$service 服务" "$image_id"
        show_image_info "$service 服务镜像校验通过" "$image_id"
    done
}

case "$ACTION" in
    deploy)
        require_no_arguments
        deploy
        ;;
    start)
        require_no_arguments
        start
        ;;
    restart)
        require_no_arguments
        info "restart 会重新构建当前 Git 代码，再执行安全迁移和重启。"
        deploy
        ;;
    stop)
        require_no_arguments
        require_docker
        prepare_compose
        compose stop
        info "全部容器已停止；Docker 命名卷和媒体持久化目录均已保留。"
        ;;
    build|build-backend)
        validate_static_configuration
        build_backend_image
        ;;
    migrate)
        require_no_arguments
        info "migrate 会使用已有镜像执行安全迁移，并重新启动应用服务。"
        start
        ;;
    validate)
        require_no_arguments
        validate_static_configuration
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    images)
        require_no_arguments
        show_images
        ;;
    help|-h|--help)
        require_no_arguments
        usage
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
