FROM ghcr.io/astral-sh/uv:0.8.20-python3.12-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 将依赖安装在不受开发目录挂载影响的位置，运行时直接加载本地源码。
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1
