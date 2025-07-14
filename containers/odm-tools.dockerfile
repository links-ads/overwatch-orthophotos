FROM python:3.12-slim

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
# set working directory
WORKDIR /app

COPY . ./
RUN uv sync --frozen --no-dev

# install the package in development mode
RUN uv pip install -e .

# set the entry point to odm-tools
ENTRYPOINT ["uv", "run", "odm-tools"]