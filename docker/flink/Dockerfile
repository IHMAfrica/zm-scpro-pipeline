# ---------- Stage 1: build Python deps (no apt) ----------
FROM python:3.10-slim AS pydeps
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /tmp

# Change this if your file is at repo root:
# COPY requirements.txt /tmp/requirements.txt
COPY flink/requirements.txt /tmp/requirements.txt

# Install to a relocatable prefix so we can copy it into Flink later
RUN python -m pip install --upgrade pip \
 && python -m pip install --no-cache-dir --prefix=/opt/pydeps -r /tmp/requirements.txt

# ---------- Stage 2: fetch Flink connector JARs (no apt) ----------
# ---------- Stage: fetch connector JARs ----------
FROM curlimages/curl:8.8.0 AS jars
WORKDIR /jars
SHELL ["/bin/sh", "-c"]
RUN set -e; \
  fetch() { echo ">> $1"; curl -fsSL --retry 5 --retry-connrefused -o "$(basename "$1")" "$1"; }; \
  fetch "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar"; \
  fetch "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar"; \
  fetch "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"; \
  fetch "https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/1.17.1/flink-avro-confluent-registry-1.17.1.jar"

# ---------- Stage: final Flink runtime ----------
FROM flink:1.17.1-scala_2.12-java11

USER root
WORKDIR /opt/flink/usrlib

# Install OS-native Python that matches the image's glibc, plus jq/curl for quick checks
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      python3 python3-pip python-is-python3 jq curl; \
    rm -rf /var/lib/apt/lists/*

# Install your PyFlink deps inside this image
COPY flink/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# ------------------------- ADDED: Avro for PyFlink -------------------------
# PyFlink 1.17 expects the "avro" module to be available and is compatible with
# avro-python3 >=1.8.1,<1.10.0 but not 1.9.2 exactly. 1.9.2.1 is safe.
# 1) Ensure a compatible avro is installed system-wide (for your own code)
# 2) Vendor the same package into Flink's thirdparty dir for the Beam harness
RUN set -eux; \
    pip3 uninstall -y avro || true; \
    pip3 install --no-cache-dir "avro-python3==1.9.2.1"; \
    mkdir -p /opt/flink/opt/python/pyflink/thirdparty; \
    pip3 install --no-cache-dir --target /opt/flink/opt/python/pyflink/thirdparty "avro-python3==1.9.2.1"
# --------------------------------------------------------------------------

# Make sure your job folder AND vendored libs are on PYTHONPATH
ENV PYTHONPATH=/opt/flink/usrlib/job:/opt/flink/opt/python/pyflink/thirdparty:/opt/flink/opt/python

# Add connector jars to Flink
COPY --from=jars /jars/*.jar /opt/flink/lib/
