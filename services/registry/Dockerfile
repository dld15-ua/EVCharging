FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CENTRAL_URL=https://central:5001 \
    REGISTRY_PORT=5000 \
    CERT_FILE=/app/certs/registry_cert.pem \
    KEY_FILE=/app/certs/registry_key.pem

EXPOSE 5000

CMD ["python3", "EV_Registry.py"]
