FROM python:3.12-slim

WORKDIR /app

COPY intel_processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY intel_processor/main.py .
COPY haversine.py .
COPY logger.py .

CMD ["python", "main.py"]
