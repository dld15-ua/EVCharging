FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV FRONTEND_PORT=5002 \
    CENTRAL_API_URL=https://localhost:5001

EXPOSE 5002

CMD ["python3", "EV_Frontend.py"]
