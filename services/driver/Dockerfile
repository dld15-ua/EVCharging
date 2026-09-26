FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV BROKER_HOST=kafka \
    BROKER_PORT=9092 \
    DRIVER_ID=D001

CMD ["python3", "EV_Driver.py"]
