import json
import time
import mysql.connector
from kafka import KafkaConsumer
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DB
)

TOPIC_AIRPORT = getattr(__import__("config.settings", fromlist=["TOPIC_AIRPORT"]),
                        "TOPIC_AIRPORT", "airport-data")

SQL_INSERT = """
INSERT INTO airport_data
(airport_code, iata, icao, name, city, country, latitude, longitude, timezone)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    iata=VALUES(iata),
    icao=VALUES(icao),
    name=VALUES(name),
    city=VALUES(city),
    country=VALUES(country),
    latitude=VALUES(latitude),
    longitude=VALUES(longitude),
    timezone=VALUES(timezone),
    updated_at=CURRENT_TIMESTAMP
"""


def connect():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        autocommit=False
    )


def insert(cursor, e):
    cursor.execute(SQL_INSERT, (
        e.get("airport_code"),
        e.get("iata"),
        e.get("icao"),
        e.get("name"),
        e.get("city"),
        e.get("country"),
        e.get("latitude"),
        e.get("longitude"),
        e.get("timezone")
    ))


def main():
    consumer = KafkaConsumer(
        TOPIC_AIRPORT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000
    )

    conn = None
    cursor = None
    print("Airport Consumer listening on:", TOPIC_AIRPORT)

    while True:
        try:
            for msg in consumer:
                e = msg.value
                print("Airport:", e.get("airport_code"), e.get("city"))

                if conn is None or not conn.is_connected():
                    try:
                        conn = connect()
                        cursor = conn.cursor()
                    except Exception as err:
                        print("DB connection failed:", err)
                        time.sleep(3)
                        continue

                try:
                    insert(cursor, e)
                    conn.commit()
                except Exception as err:
                    print("Insert error:", err)
                    try:
                        conn.rollback()
                    except:
                        pass

        except Exception as err:
            print("Consumer loop error:", err)
            time.sleep(3)

        time.sleep(1)


if __name__ == "__main__":
    main()
