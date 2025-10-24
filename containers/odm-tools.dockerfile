FROM python:3.12-slim

# install uv
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /uvx /usr/local/bin/

# install extra apt dependencies
RUN apt update && apt install -y libexpat1

# set working directory
WORKDIR /app
# copy material and install package
COPY src/ pyproject.toml README.md ./
RUN uv sync --no-dev

# set the entry point to odm-tools
ENTRYPOINT ["uv", "run", "odm-tools"]