# confluent-kafka ships prebuilt wheels (with librdkafka) for this Python version.
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

EXPOSE 8000
CMD ["python", "consumer.py"]
