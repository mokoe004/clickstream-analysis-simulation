FROM python:3.11-slim

LABEL authors="exmob"

WORKDIR /app

# Nur requirements kopieren, damit Pip-Install cached wird
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Setze das Startkommando für den Container
CMD ["python", "kafka_producer.py"]
