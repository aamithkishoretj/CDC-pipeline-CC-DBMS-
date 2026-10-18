FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 5002
CMD ["gunicorn", "--workers", "2", "--threads", "8", "--bind", "0.0.0.0:5002", "app:app"]
