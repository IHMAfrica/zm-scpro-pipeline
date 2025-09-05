# ===================== Base Python w/ system deps =====================
FROM python:3.9-slim AS base-python
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates gnupg apt-transport-https build-essential unixodbc-dev wget unzip; \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# ===================== Producer image (SQL Server → Kafka) =====================
FROM base-python AS producer
# Install MS ODBC Driver 18 for SQL Server (Debian 12/bookworm)
RUN set -eux; \
    apt-get update; \
    curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list; \
    apt-get update; \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18; \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY producer /app/producer
COPY scripts /app/scripts
COPY .env.example /app/.env.example

# ===================== Flink submit image =====================
FROM flink:1.17.1-scala_2.12-java11 AS flink-submit
USER root
RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends python3 python3-pip && rm -rf /var/lib/apt/lists/*; \
    python3 -m pip install --no-cache-dir apache-flink==1.17.1 psycopg2-binary==2.9.9
WORKDIR /opt/jobs
COPY flink /opt/jobs
COPY scripts /opt/scripts
COPY requirements.txt /opt/requirements.txt
# Best-effort fetch of connector jars at build (also done at runtime)
RUN /bin/bash -lc "mkdir -p /opt/flink/lib && /opt/scripts/fetch_jars.sh || true"
