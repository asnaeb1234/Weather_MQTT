FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY weather_receiver.py .

CMD ["python", "weather_receiver.py"]
