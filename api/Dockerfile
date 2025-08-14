# Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /api

# enable bytecode compilation; copy from the cache instead of linking since it's a mounted volume
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# install dependencies using the lockgile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# add rest of project source code and install it
# installing separately from its dependencies allow optimal layer caching
COPY . /api
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# use final image without uv
FROM python:3.12-slim-bookworm

# Install cron
RUN apt-get update && \
    apt-get install -y cron && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# copy the api from the builder
COPY --from=builder --chown=api:api /api /api

# Copy cron configuration
COPY datacenter-job /etc/cron.d/datacenter-job
RUN chmod 0644 /etc/cron.d/datacenter-job && \
    crontab /etc/cron.d/datacenter-job

# set PYTHONPATH to the root of the project (/api)
ENV PYTHONPATH=/api:$PYTHONPATH

# place executables in the envionment at the front of the path
ENV PATH="/api/.venv/bin:$PATH"

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create log directory and set permissions
RUN mkdir -p /var/log && \
    touch /var/log/cronjob.log && \
    chmod 666 /var/log/cronjob.log

# Use entrypoint to start both cron and FastAPI
CMD ["/entrypoint.sh"]