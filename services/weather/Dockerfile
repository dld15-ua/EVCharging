FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CENTRAL_URL=https://central:5001 \
    OPENWEATHER_API_KEY=bfeda57348ea4fb6d487399245c47a30 \
    CIUDADES=Alicante:ALC1,Madrid:ALC2 \
    LIMITE_TEMP=20

CMD ["python3", "EV_Weather.py"]
