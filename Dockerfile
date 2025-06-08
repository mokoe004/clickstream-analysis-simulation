FROM python:3.11-slim

LABEL authors="exmob"

# Setze Arbeitsverzeichnis im Container
WORKDIR /app

# Kopiere alle Dateien ins Image (inkl. kafka_producer.py)
COPY . .

# Installiere notwendige Python-Abhängigkeit
RUN pip install --no-cache-dir kafka-python cassandra-driver dash dash-bootstrap-components pandas plotly


# Setze das Startkommando für den Container
CMD ["python", "kafka_producer.py"]
