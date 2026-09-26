FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV BROKER_HOST=kafka \
    BROKER_PORT=9092 \
    SOCKET_PORT=65000 \
    API_PORT=5001 \
    DB_PATH=/app/data/evcharging.db \
    CERT_FILE=/app/certs/server_cert.pem \
    KEY_FILE=/app/certs/server_key.pem

EXPOSE 5001 65000

CMD ["python3", "EV_Central.py"]
