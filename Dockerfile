FROM python:3.11-slim-bookworm

# Add metadata
LABEL maintainer="Data Commons Team" \
      description="Data Commons RCA Agent PoC - Root Cause Analysis and Historical Archiving dashboard"


# Install curl, ca-certificates, and gnupg
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Add Google Cloud SDK repository and install gcloud + bq CLI
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PROJECT_ROOT=/app

# Copy python setup configuration and code
COPY pyproject.toml /app/
COPY src /app/src
COPY config /app/config

# Install dependencies and the package itself
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir .

EXPOSE 8080

# Cloud Run sets PORT; default 8080 for local
CMD ["sh", "-c", "uvicorn dc_rca_agent.main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1"]
