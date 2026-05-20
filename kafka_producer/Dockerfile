FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python

COPY producer.py .
COPY requirements.txt .

CMD ["python", "producer.py"]