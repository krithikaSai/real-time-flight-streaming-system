import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    AVIATIONSTACK_API_KEY,
)

TOPIC_METADATA = getattr(__import__("config.settings", fromlist=["TOPIC_METADATA"]),
                         "TOPIC_METADATA", "metadata-data")

API_URL = "http://api.aviationstack.com/v1/flights"

POLL_INTERVAL = 45

STATUSES = ["scheduled", "active", "landed"]


def iso(dt):
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt.replace("Z", "+00:00")) \
            .astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None


def fetch(status):
    params = {
        "access_key": AVIATIONSTACK_API_KEY,
        "flight_status": status,
        "limit": 100,
    }
    try:
        r = requests.get(API_URL, params=params, timeout=15)
        if r.status_code != 200:
            print(f"[{status}] error:", r.status_code, r.text[:100])
            return []
        return r.json().get("data") or []
    except Exception as e:
        print(f"[{status}] fetch error:", e)
        return []


def good(record):
    dep = record.get("departure") or {}
    arr = record.get("arrival") or {}

    return (
        dep.get("icao")
        and arr.get("icao")
        and (dep.get("scheduled") or dep.get("actual"))
        and (arr.get("scheduled") or arr.get("actual"))
    )


def normalize(rec):
    flight = rec.get("flight") or {}
    airline = rec.get("airline") or {}
    aircraft = rec.get("aircraft") or {}

    dep = rec.get("departure") or {}
    arr = rec.get("arrival") or {}

    return {
        "flight_number": flight.get("iata") or flight.get("icao"),
        "airline_name": airline.get("name"),
        "airline_iata": airline.get("iata"),

        "aircraft_icao24": aircraft.get("icao24"),

        "departure_airport": dep.get("icao"),
        "departure_iata": dep.get("iata"),
        "departure_scheduled": iso(dep.get("scheduled")),
        "departure_actual": iso(dep.get("actual")),

        "arrival_airport": arr.get("icao"),
        "arrival_iata": arr.get("iata"),
        "arrival_scheduled": iso(arr.get("scheduled")),
        "arrival_actual": iso(arr.get("actual")),

        "status": rec.get("flight_status"),
        "delay_minutes": dep.get("delay"),

        "data_source": "aviationstack",
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Hybrid Metadata Producer running. Brace yourself.")

    while True:
        total = 0

        for status in STATUSES:
            records = fetch(status)

            usable = [r for r in records if good(r)]

            for rec in usable:
                producer.send(TOPIC_METADATA, normalize(rec))
                total += 1

        producer.flush()
        print(f"Sent {total} usable metadata rows.")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
