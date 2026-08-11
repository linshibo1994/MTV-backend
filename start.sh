#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$PROJECT_ROOT/backend"
RUNTIME_DIR="$PROJECT_ROOT/.runtime"
PID_FILE="$RUNTIME_DIR/backend.pid"
LOG_FILE="$RUNTIME_DIR/backend.log"
UVICORN_BIN="$BACKEND_DIR/.venv/bin/uvicorn"
COMPOSE_FILE="$BACKEND_DIR/docker-compose.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
COMPOSE_SERVICES=(postgres redis celery-worker celery-beat)
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
POSTGRES_PORT="${POSTGRES_PORT:-55432}"
REDIS_PORT="${REDIS_PORT:-56379}"
LOCAL_DATABASE_URL="postgresql+asyncpg://flow:flow@127.0.0.1:${POSTGRES_PORT}/flowapp"
LOCAL_DATABASE_SYNC_URL="postgresql+psycopg2://flow:flow@127.0.0.1:${POSTGRES_PORT}/flowapp"
LOCAL_REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"
LOCAL_CELERY_BROKER_URL="redis://127.0.0.1:${REDIS_PORT}/1"
LOCAL_CELERY_RESULT_BACKEND="redis://127.0.0.1:${REDIS_PORT}/2"

export MTV_POSTGRES_PORT="$POSTGRES_PORT"
export MTV_REDIS_PORT="$REDIS_PORT"

usage() {
  cat <<'EOF'
用法: ./start.sh <命令>

命令:
  start    启动 PostgreSQL、Redis、Celery 和后端（默认开启热重载）
  stop     停止后端与 Compose 服务，保留数据库和 Redis 数据卷
  restart  重启整套本地服务
  status   查看后端与 Compose 服务状态
  logs     查看日志；可指定 backend 或 services，默认 backend
  migrate  显式执行数据库迁移
  help     显示本帮助

环境变量:
  HOST    监听地址，默认 0.0.0.0
  PORT    监听端口，默认 8000
  POSTGRES_PORT  PostgreSQL 宿主机端口，默认 55432
  REDIS_PORT     Redis 宿主机端口，默认 56379

示例:
  ./start.sh start
  PORT=8001 ./start.sh start
  POSTGRES_PORT=55433 REDIS_PORT=56380 ./start.sh start
  ./start.sh migrate
  ./start.sh logs
  ./start.sh logs services
EOF
}

docker_compose_available() {
  command -v docker >/dev/null 2>&1 \
    && docker compose version >/dev/null 2>&1 \
    && docker info >/dev/null 2>&1
}

require_docker_compose() {
  if ! docker_compose_available; then
    echo "Docker Compose 或 Docker 守护进程不可用，请先启动 Docker Desktop。" >&2
    exit 1
  fi
}

running_service_names() {
  "${COMPOSE[@]}" ps --status running --services 2>/dev/null || true
}

services_are_running() {
  local running_services service
  running_services="$(running_service_names)"

  for service in "${COMPOSE_SERVICES[@]}"; do
    if [[ $'\n'"$running_services"$'\n' != *$'\n'"$service"$'\n'* ]]; then
      return 1
    fi
  done
}

start_services() {
  require_docker_compose

  echo "正在启动 PostgreSQL、Redis 与 Celery 服务..."
  "${COMPOSE[@]}" up -d "${COMPOSE_SERVICES[@]}"

  for _ in {1..30}; do
    if services_are_running; then
      echo "基础服务已启动。"
      echo "PostgreSQL：127.0.0.1:${POSTGRES_PORT}；Redis：127.0.0.1:${REDIS_PORT}"
      return
    fi
    sleep 1
  done

  echo "基础服务未能在 30 秒内全部进入运行状态：" >&2
  "${COMPOSE[@]}" ps >&2
  exit 1
}

stop_services() {
  if ! docker_compose_available; then
    echo "Docker Compose 或 Docker 守护进程不可用，跳过 Compose 服务停止。" >&2
    return
  fi

  echo "正在停止 PostgreSQL、Redis 与 Celery 服务（数据卷会保留）..."
  "${COMPOSE[@]}" stop "${COMPOSE_SERVICES[@]}"
}

is_running() {
  if [[ ! -f "$PID_FILE" ]]; then
    return 1
  fi

  local pid
  pid="$(<"$PID_FILE")"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

remove_stale_pid_file() {
  if [[ -f "$PID_FILE" ]] && ! is_running; then
    rm -f "$PID_FILE"
  fi
}

ensure_port_available() {
  if ! command -v lsof >/dev/null 2>&1; then
    return
  fi

  local listener_pids
  listener_pids="$(lsof -nP -t -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "$listener_pids" ]]; then
    echo "端口 $PORT 已被进程占用（PID：${listener_pids//$'\n'/, }）。" >&2
    echo "请先停止现有服务，或使用 PORT=<其他端口> ./start.sh start。" >&2
    exit 1
  fi
}

start() {
  if [[ ! -x "$UVICORN_BIN" ]]; then
    echo "未找到后端虚拟环境：$UVICORN_BIN" >&2
    echo "请先在 backend 目录执行：uv sync --extra dev" >&2
    exit 1
  fi

  start_services

  if is_running; then
    echo "后端已在运行，PID：$(<"$PID_FILE")"
    echo "地址：http://localhost:$PORT"
    return
  fi

  remove_stale_pid_file
  ensure_port_available
  mkdir -p "$RUNTIME_DIR"

  (
    cd "$BACKEND_DIR"
    exec nohup env \
      DATABASE_URL="$LOCAL_DATABASE_URL" \
      DATABASE_SYNC_URL="$LOCAL_DATABASE_SYNC_URL" \
      REDIS_URL="$LOCAL_REDIS_URL" \
      CELERY_BROKER_URL="$LOCAL_CELERY_BROKER_URL" \
      CELERY_RESULT_BACKEND="$LOCAL_CELERY_RESULT_BACKEND" \
      "$UVICORN_BIN" app.main:app --host "$HOST" --port "$PORT" --reload
  ) >>"$LOG_FILE" 2>&1 </dev/null &
  local pid=$!
  echo "$pid" >"$PID_FILE"

  sleep 1
  if ! is_running; then
    echo "后端启动失败，日志如下：" >&2
    tail -n 40 "$LOG_FILE" >&2 || true
    rm -f "$PID_FILE"
    exit 1
  fi

  echo "后端已启动，PID：$pid"
  echo "地址：http://localhost:$PORT"
  echo "接口文档：http://localhost:$PORT/v3api/docs"
  echo "日志文件：$LOG_FILE"
  echo "首次启动或迁移变更后，请手动执行：cd backend && .venv/bin/alembic upgrade head"
}

stop_api() {
  if ! is_running; then
    remove_stale_pid_file
    echo "后端未运行。"
    return
  fi

  local pid
  pid="$(<"$PID_FILE")"
  kill "$pid"

  for _ in {1..10}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$PID_FILE"
      echo "后端已停止。"
      return
    fi
    sleep 1
  done

  echo "后端未在 10 秒内退出，PID：$pid" >&2
  echo "请检查日志或手动执行：kill $pid" >&2
  exit 1
}

stop() {
  stop_api
  stop_services
}

migrate() {
  if [[ ! -x "$BACKEND_DIR/.venv/bin/alembic" ]]; then
    echo "未找到 Alembic：$BACKEND_DIR/.venv/bin/alembic" >&2
    echo "请先在 backend 目录执行：uv sync --extra dev" >&2
    exit 1
  fi

  start_services
  (
    cd "$BACKEND_DIR"
    exec env \
      DATABASE_URL="$LOCAL_DATABASE_URL" \
      DATABASE_SYNC_URL="$LOCAL_DATABASE_SYNC_URL" \
      REDIS_URL="$LOCAL_REDIS_URL" \
      CELERY_BROKER_URL="$LOCAL_CELERY_BROKER_URL" \
      CELERY_RESULT_BACKEND="$LOCAL_CELERY_RESULT_BACKEND" \
      .venv/bin/alembic upgrade head
  )
}

status() {
  local status_code=0

  if is_running; then
    echo "后端运行中，PID：$(<"$PID_FILE")"
    echo "地址：http://localhost:$PORT"
  else
    remove_stale_pid_file
    echo "后端未运行。"
    status_code=1
  fi

  if ! docker_compose_available; then
    echo "Docker Compose 或 Docker 守护进程不可用。"
    return 1
  fi

  echo
  echo "Compose 服务状态："
  "${COMPOSE[@]}" ps

  if services_are_running; then
    echo "基础服务均在运行。"
    echo "PostgreSQL：127.0.0.1:${POSTGRES_PORT}；Redis：127.0.0.1:${REDIS_PORT}"
  else
    echo "存在未运行的基础服务。"
    status_code=1
  fi

  return "$status_code"
}

logs() {
  case "${2:-backend}" in
    backend)
      mkdir -p "$RUNTIME_DIR"
      touch "$LOG_FILE"
      tail -n 100 -f "$LOG_FILE"
      ;;
    services)
      require_docker_compose
      "${COMPOSE[@]}" logs --tail 100 -f "${COMPOSE_SERVICES[@]}"
      ;;
    *)
      echo "logs 仅支持 backend 或 services。" >&2
      exit 2
      ;;
  esac
}

case "${1:-help}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    ;;
  status)
    status
    ;;
  logs)
    logs
    ;;
  migrate)
    migrate
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "未知命令：$1" >&2
    usage >&2
    exit 2
    ;;
esac
