FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV BROKER_HOST=kafka \
    BROKER_PORT=9092 \
    CP_ID=ALC1 \
    ENGINE_PORT=65001 \
    ENGINE_BIND=0.0.0.0 \
    ENGINE_INTERACTIVE=false

EXPOSE 65001

CMD ["python3", "EV_CP_E.py"]
