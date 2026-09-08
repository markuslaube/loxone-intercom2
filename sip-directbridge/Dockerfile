FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sip_directbridge.py .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "sip_directbridge.py"]
CMD ["--websocket"]
