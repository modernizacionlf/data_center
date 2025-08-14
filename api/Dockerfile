# Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

# enable bytecode compilation; copy from the cache instead of linking since it's a mounted volume
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# install dependencies using the lockgile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# add rest of project source code and install it
# installing separately from its dependencies allow optimal layer caching
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# use final image without uv
FROM python:3.12-slim-bookworm

# copy the app from the builder
COPY --from=builder --chown=app:app /app /app

# place executables in the envionment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# run FastAPI
# uses `fastapi dev` to enable hot-reloading when the `watch` sync occurs
# uses `--host 0.0.0.0` to allow access from outside
CMD ["fastapi", "dev", "--host", "0.0.0.0", "--port", "8002" "/app/main.py"]