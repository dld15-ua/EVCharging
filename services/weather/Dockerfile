FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CENTRAL_URL=https://central:5001 \
    CIUDADES=Alicante:ALC1,Madrid:ALC2 \
    LIMITE_TEMP=15

CMD ["python3", "EV_Weather.py"]
