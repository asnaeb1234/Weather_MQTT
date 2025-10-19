from flask import Flask, request
import datetime
import os
import json
import paho.mqtt.client as mqtt
from threading import Timer
from zoneinfo import ZoneInfo

# MQTT-Konfiguration
MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "bresser/weather")
DISCOVERY_PREFIX = "homeassistant"

# Speicherort auf externer Festplatte
SAVE_PATH = "/data"

buffered_data = []
app = Flask(__name__)

mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

# Sensor-Definitionen für MQTT Discovery
SENSORS = [
    {"name": "Temperature", "key": "tempf", "unit": "°C", "template": "((value_json.tempf | float - 32) / 1.8) | round(1)"},
    {"name": "Indoor Temperature", "key": "indoortempf", "unit": "°C", "template": "((value_json.indoortempf | float - 32) / 1.8) | round(1)"},
    {"name": "Dew Point", "key": "dewptf", "unit": "°C", "template": "((value_json.dewptf | float - 32) / 1.8) | round(1)"},
    {"name": "Humidity", "key": "humidity", "unit": "%", "template": "value_json.humidity | float"},
    {"name": "Indoor Humidity", "key": "indoorhumidity", "unit": "%", "template": "value_json.indoorhumidity | float"},
    {"name": "Wind Speed", "key": "windspeedmph", "unit": "km/h", "template": "(value_json.windspeedmph | float * 0.44704 * 3.6) | round(1)"},
    {"name": "Wind Gust", "key": "windgustmph", "unit": "m/s", "template": "(value_json.windgustmph | float * 0.44704 * 3.6) | round(1)"},
    {"name": "Wind Direction", "key": "winddir", "unit": "°", "template": "value_json.winddir | float"},
    {"name": "Rain Rate", "key": "rainin", "unit": "mm/h", "template": "(value_json.rainin | float * 25.4) | round(1)"},
    {"name": "Daily Rain", "key": "dailyrainin", "unit": "mm", "template": "(value_json.dailyrainin | float * 25.4) | round(1)"},
    {"name": "Solar Radiation", "key": "solarradiation", "unit": "W/m²", "template": "value_json.solarradiation | float"},
    {"name": "UV Index", "key": "UV", "unit": "UV Index", "template": "value_json.UV | float"},
    {"name": "Soil Temperature", "key": "soiltempf", "unit": "°C", "template": "((value_json.soiltempf | float - 32) / 1.8) | round(1)"},
    {"name": "Soil Moisture", "key": "soilmoisture", "unit": "%", "template": "value_json.soilmoisture | float"},
    {"name": "Pressure", "key": "baromin", "unit": "hPa", "template": "(value_json.baromin | float * 33.8639) | round(1)"},
]

def register_sensors():
    for sensor in SENSORS:
        uid = sensor['key']
        name = "PWS " + sensor["name"]
        topic = f"{DISCOVERY_PREFIX}/sensor/bresser_{uid}/config"
        payload = {
            "name": name,
            "state_topic": MQTT_TOPIC,
            "unit_of_measurement": sensor["unit"],
            "value_template": f"{{{{ {sensor['template']} }}}}",
            "unique_id": f"bresser_{uid}",
            "device": {
                "identifiers": ["bresser_station"],
                "manufacturer": "Bresser",
                "model": "7-in-1",
                "name": "Bresser Wetterstation"
            }
        }
        mqtt_client.publish(topic, json.dumps(payload), retain=True)
        print(f"[MQTT Discovery] Published config for sensor: {name}")

@app.route('/weather', methods=['GET'])
@app.route('/weatherstation/updateweatherstation.php', methods=['GET'])
def weather():
    timestamp = datetime.datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d %H:%M:%S")
    IGNORED_KEYS = {"ID", "PASSWORD", "action", "realtime", "rtfreq", "dateutc"}
    data = {k: v for k, v in request.args.items() if k not in IGNORED_KEYS}

    # RAM-Zwischenspeicherung
    buffered_data.append((timestamp, data))

    return "OK", 200

# MQTT-Publikation alle 10 Minuten
def publish_weather_data():
    if buffered_data:
        timestamp, data = buffered_data[-1]  # Nur die neuesten Daten senden
        mqtt_payload = {"timestamp": timestamp, **data}
        mqtt_json = json.dumps(mqtt_payload)
        mqtt_client.publish(MQTT_TOPIC, mqtt_json, qos=0, retain=True)
        print(f"[MQTT] Published: {mqtt_json}")

    # Timer für nächste Veröffentlichung nach 10 Minuten
    Timer(600, publish_weather_data).start()

def save_to_disk():
    if not buffered_data:
        return

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    filepath = os.path.join(SAVE_PATH, f"{today}.csv")

    # Header schreiben, falls Datei neu
    if not os.path.exists(filepath):
        with open(filepath, 'w') as f:
            keys = buffered_data[0][1].keys()
            f.write("timestamp," + ",".join(keys) + "\n")

    # Daten anhängen
    with open(filepath, 'a') as f:
        for timestamp, data in buffered_data:
            f.write(timestamp + "," + ",".join(data.values()) + "\n")

    print(f"[FILE] Daten gespeichert in: {filepath}")
    buffered_data.clear()

def schedule_daily_save():
    now = datetime.datetime.now()
    save_time = now.replace(hour=21, minute=0, second=0, microsecond=0)
    if now >= save_time:
        save_time += datetime.timedelta(days=1)
    delay = (save_time - now).total_seconds()
    Timer(delay, scheduled_save).start()

def scheduled_save():
    save_to_disk()
    schedule_daily_save()

if __name__ == '__main__':
    register_sensors()
    schedule_daily_save()
    publish_weather_data()
    app.run(host='0.0.0.0', port=8124)
