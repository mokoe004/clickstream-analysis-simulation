# -----------------------
# 🏗️ STAGE 1: Builder mit Tools
# -----------------------
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      netcat \
      cassandra-tools \
      gcc \
      libffi-dev \
      libssl-dev \
      build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# -----------------------
# 🚀 STAGE 2: Laufzeitcontainer (klein, aber mit Tools)
# -----------------------
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      netcat \
      cassandra-tools && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY . .

# Du kannst das CMD in docker-compose überschreiben
CMD ["python", "kafka_producer.py"]
