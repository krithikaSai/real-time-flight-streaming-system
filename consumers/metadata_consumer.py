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

TOPIC_METADATA = getattr(__import__("config.settings", fromlist=["TOPIC_METADATA"]), "TOPIC_METADATA", "metadata-data")

SQL_INSERT = """
INSERT INTO flight_metadata
(flight_number, airline_name, airline_iata, aircraft_icao24,
 departure_airport, departure_iata, departure_scheduled, departure_actual,
 arrival_airport, arrival_iata, arrival_scheduled, arrival_actual,
 status, delay_minutes, data_source)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
        e.get("flight_number"),
        e.get("airline_name"),
        e.get("airline_iata"),
        e.get("aircraft_icao24"),

        e.get("departure_airport"),
        e.get("departure_iata"),
        e.get("departure_scheduled"),
        e.get("departure_actual"),

        e.get("arrival_airport"),
        e.get("arrival_iata"),
        e.get("arrival_scheduled"),
        e.get("arrival_actual"),

        e.get("status"),
        e.get("delay_minutes"),
        e.get("data_source")
    ))


def main():
    consumer = KafkaConsumer(
        TOPIC_METADATA,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000
    )

    conn = None
    cursor = None

    print("Metadata consumer listening on:", TOPIC_METADATA)

    while True:
        try:
            for msg in consumer:
                ev = msg.value
                print("Received metadata:", ev.get("flight_number"), ev.get("status"))

                if conn is None or not conn.is_connected():
                    try:
                        conn = connect()
                        cursor = conn.cursor()
                    except Exception as e:
                        print("MySQL connection error:", e)
                        conn = None
                        cursor = None
                        time.sleep(3)
                        continue

                try:
                    insert(cursor, ev)
                    conn.commit()
                except Exception as e:
                    print("Insert error:", e)
                    try:
                        conn.rollback()
                    except:
                        pass

        except Exception as e:
            print("Consumer loop error:", e)
            time.sleep(3)

        time.sleep(1)


if __name__ == "__main__":
    main()
