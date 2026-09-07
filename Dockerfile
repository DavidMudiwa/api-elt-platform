FROM python:3.13-slim

# Install basic system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files first for Docker layer caching
COPY pyproject.toml uv.lock ./

# Create the virtual environment and install dependencies
RUN uv sync --frozen

# Copy project files
COPY . .

# Make the container use the uv-managed environment
ENV PATH="/app/.venv/bin:$PATH"

# Dagster UI
EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]