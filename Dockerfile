# pin to bookworm so openjdk-17 is available
FROM python:3.11-slim-bookworm


RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless curl ca-certificates netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# create unprivileged user
RUN useradd -ms /bin/bash app
USER app
WORKDIR /work

# If you use a venv inside the container, create it once:
RUN python -m venv /work/.venv
ENV PATH="/work/.venv/bin:${PATH}"

# Install dependencies
RUN pip install --no-cache-dir \
    pyspark==4.0.1 \
    kafka-python \
    pymongo

# Helpful defaults for PySpark
ENV PYSPARK_PYTHON=/work/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/work/.venv/bin/python
