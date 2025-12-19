import json
import time
import requests
from kafka import KafkaProducer
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    AIRLABS_API_KEY,
)

TOPIC_AIRPORT = getattr(__import__("config.settings", fromlist=["TOPIC_AIRPORT"]),
                        "TOPIC_AIRPORT", "airport-data")

AIRLABS_URL = "https://airlabs.co/api/v9/airports"
FETCH_INTERVAL = 3600 * 6   # every 6 hours (airports rarely change)


def fetch_airports():
    params = {
        "api_key": AIRLABS_API_KEY,
        "limit": 3000
    }
    try:
        r = requests.get(AIRLABS_URL, params=params, timeout=20)
        if r.status_code != 200:
            print("AirLabs error:", r.status_code, r.text[:100])
            return []
        data = r.json()
        return data.get("response") or []
    except Exception as e:
        print("Airport fetch error:", e)
        return []


def normalize(a):
    return {
        "airport_code": a.get("icao") or a.get("iata"),
        "iata": a.get("iata"),
        "icao": a.get("icao"),
        "name": a.get("name"),
        "city": a.get("city"),
        "country": a.get("country"),
        "latitude": a.get("lat"),
        "longitude": a.get("lng"),
        "timezone": a.get("timezone")
    }


def main():
    prod = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Airport Producer started (AirLabs → Kafka)")

    while True:
        airports = fetch_airports()
        if not airports:
            print("No airport metadata fetched. Sleeping...")
            time.sleep(FETCH_INTERVAL)
            continue

        count = 0
        for ap in airports:
            event = normalize(ap)
            if not event["airport_code"]:
                continue
            prod.send(TOPIC_AIRPORT, event)
            count += 1

        prod.flush()
        print(f"Sent {count} airport records.")

        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main()
