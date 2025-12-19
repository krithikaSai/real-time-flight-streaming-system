import json
import time
import requests
import mysql.connector
from kafka import KafkaProducer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_WEATHER_DATA,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DB,
    OPENWEATHER_API_KEY
)


def get_active_airports():
    """Fetch all airports that appear in flight_metadata and have coordinates."""
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cur = conn.cursor(dictionary=True)

    sql = """
        SELECT DISTINCT fm.departure_airport AS airport, 
               ad.latitude, ad.longitude
        FROM flight_metadata fm
        JOIN airport_data ad ON fm.departure_airport = ad.icao
        WHERE ad.latitude IS NOT NULL AND ad.longitude IS NOT NULL;
    """

    cur.execute(sql)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


def fetch_weather(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
    try:
        r = requests.get(url, timeout=10)
        return r.json()
    except Exception as e:
        print("Weather fetch error:", e)
        return None


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Dynamic Weather Producer started...")

    while True:
        airports = get_active_airports()
        print(f"Found {len(airports)} airports needing weather updates.")

        for ap in airports:
            weather = fetch_weather(ap["latitude"], ap["longitude"])
            if weather:
                event = {
                    "airport": ap["airport"],
                    "weather": weather
                }
                producer.send(TOPIC_WEATHER_DATA, event)
                print("Sent weather:", ap["airport"])

        producer.flush()
        time.sleep(60)  # update every 1 minute


if __name__ == "__main__":
    main()
