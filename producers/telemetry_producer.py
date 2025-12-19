import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from config.settings import KAFKA_BOOTSTRAP_SERVERS

TOPIC_TELEMETRY = getattr(
    __import__("config.settings", fromlist=["TOPIC_TELEMETRY"]),
    "TOPIC_TELEMETRY",
    "telemetry-data"
)

OPENSKY_URL = "https://opensky-network.org/api/states/all"

# Tunables
POLL_INTERVAL_SECONDS = 5
MAX_EVENTS_PER_CYCLE = 200  # cap to avoid overloading Kafka


def now_utc_iso():
    return datetime.utcnow().replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fetch_opensky():
    try:
        resp = requests.get(OPENSKY_URL, timeout=15)
        if resp.status_code != 200:
            print("OpenSky non-200:", resp.status_code)
            return []

        data = resp.json() if resp.text else {}
        return data.get("states") or []

    except Exception as e:
        print("OpenSky fetch error:", e)
        return []


def state_to_event(state):
    """
    OpenSky state vector:
    [icao24, callsign, origin_country, time_position, last_contact,
     longitude, latitude, baro_altitude, on_ground, velocity,
     true_track, vertical_rate, sensors, geo_altitude,
     squawk, spi, position_source]
    """
    try:
        icao24 = state[0]
    except Exception:
        return None

    callsign = state[1].strip() if len(state) > 1 and state[1] else None
    origin_country = state[2] if len(state) > 2 else None
    last_contact = state[4] if len(state) > 4 else None
    longitude = state[5] if len(state) > 5 else None
    latitude = state[6] if len(state) > 6 else None
    baro_altitude = state[7] if len(state) > 7 else None
    velocity = state[9] if len(state) > 9 else None
    true_track = state[10] if len(state) > 10 else None
    vertical_rate = state[11] if len(state) > 11 else None
    geo_altitude = state[13] if len(state) > 13 else None
    squawk = state[14] if len(state) > 14 else None

    altitude = geo_altitude if geo_altitude is not None else baro_altitude

    return {
        "icao24": icao24,
        "callsign": callsign,
        "origin_country": origin_country,
        "latitude": latitude,
        "longitude": longitude,
        "altitude": altitude,
        "velocity": velocity,
        "heading": true_track,
        "vertical_rate": vertical_rate,
        "squawk": squawk,
        "last_contact": last_contact,
        "event_time": now_utc_iso(),
        "source": "opensky"
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=200
    )

    print("Telemetry producer starting: OpenSky → Kafka topic:", TOPIC_TELEMETRY)

    while True:
        states = fetch_opensky()

        if not states:
            print("No telemetry from OpenSky. Sleeping", POLL_INTERVAL_SECONDS, "s")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        count = 0
        for state in states:
            if count >= MAX_EVENTS_PER_CYCLE:
                break

            ev = state_to_event(state)
            if not ev:
                continue

            producer.send(TOPIC_TELEMETRY, ev)
            count += 1

            if count <= 8:
                print(
                    "Sent:",
                    {k: ev[k] for k in ("icao24", "callsign", "latitude", "longitude", "velocity")}
                )

        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
