import json
import time
import mysql.connector
from kafka import KafkaConsumer
from config.settings import KAFKA_BOOTSTRAP_SERVERS


TOPIC_TELEMETRY = getattr(__import__("config.settings", fromlist=["TOPIC_TELEMETRY"]), "TOPIC_TELEMETRY", "telemetry-data")
MYSQL_HOST = getattr(__import__("config.settings", fromlist=["MYSQL_HOST"]), "MYSQL_HOST", "localhost")
MYSQL_PORT = getattr(__import__("config.settings", fromlist=["MYSQL_PORT"]), "MYSQL_PORT", 3306)
MYSQL_USER = getattr(__import__("config.settings", fromlist=["MYSQL_USER"]), "MYSQL_USER", "root")
MYSQL_PASSWORD = getattr(__import__("config.settings", fromlist=["MYSQL_PASSWORD"]), "MYSQL_PASSWORD", "")
MYSQL_DB = getattr(__import__("config.settings", fromlist=["MYSQL_DB"]), "MYSQL_DB", "flights")

RECONNECT_WAIT = 5

INSERT_SQL = """
INSERT INTO telemetry_events
(icao24, callsign, origin_country, latitude, longitude, altitude, velocity, heading, vertical_rate, event_time, last_contact, source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def get_mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        autocommit=False
    )

def insert_event(cursor, event):
    # prepare values, normalizing types and missing keys
    vals = (
        event.get("icao24"),
        event.get("callsign"),
        event.get("origin_country"),
        event.get("latitude"),
        event.get("longitude"),
        event.get("altitude"),
        event.get("velocity"),
        event.get("heading"),
        event.get("vertical_rate"),
        event.get("event_time"),   # expect "YYYY-MM-DD HH:MM:SS" UTC
        event.get("last_contact"),
        event.get("source")
    )
    cursor.execute(INSERT_SQL, vals)

def main():
    consumer = KafkaConsumer(
        TOPIC_TELEMETRY,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000
    )

    print("Telemetry consumer listening on topic:", TOPIC_TELEMETRY)
    conn = None
    cursor = None

    while True:
        try:
            for message in consumer:
                event = message.value
                print("Received telemetry:", event.get("icao24"), "loc:", event.get("latitude"), event.get("longitude"))
                # ensure DB connection
                if conn is None or not conn.is_connected():
                    try:
                        conn = get_mysql_conn()
                        cursor = conn.cursor()
                    except Exception as e:
                        print("MySQL connect error:", e)
                        conn = None
                        cursor = None
                        time.sleep(RECONNECT_WAIT)
                        continue

                try:
                    insert_event(cursor, event)
                    conn.commit()
                except Exception as e:
                    print("DB insert error:", e)
                    try:
                        conn.rollback()
                    except:
                        pass
        except Exception as e:
            print("Consumer loop error:", e)
            # best-effort reconnect / sleep then continue
            time.sleep(RECONNECT_WAIT)
        # short pause to avoid tight loop if consumer timed out
        time.sleep(1)

if __name__ == "__main__":
    main()
