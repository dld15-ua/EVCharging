FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CENTRAL_HOST=central \
    CENTRAL_PORT=65000 \
    ENGINE_HOST=engine \
    ENGINE_PORT=65001 \
    CP_ID=ALC1 \
    UBICACION=Default \
    REGISTRY_URL=https://registry:5000 \
    AUTO_REGISTER=true

CMD ["python3", "EV_CP_M.py"]
