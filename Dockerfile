FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system deps needed for asyncpg (Postgres C client)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# We use requirements-render.txt here which includes asyncpg for Postgres support
COPY requirements.txt requirements-render.txt ./
RUN pip install --no-cache-dir -r requirements-render.txt

# Copy application code
COPY . .

# Expose the port uvicorn will listen on
EXPOSE 8000

# IMPORTANT: --workers 1 is non-negotiable.
# The in-process token-bucket rate limiter is only correct with a single worker.
# If you ever add workers, move the rate limiter state to Redis first.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
