FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator.py .

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

CMD ["python", "simulator.py"]
