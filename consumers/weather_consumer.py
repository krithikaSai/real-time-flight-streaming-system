import json
import mysql.connector
from kafka import KafkaConsumer
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_WEATHER_DATA,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DB
)

def insert_weather(event):
    weather = event.get("weather", {})
    main = weather.get("main", {})
    wind = weather.get("wind", {})

    temperature = main.get("temp")
    humidity = main.get("humidity")
    wind_speed = wind.get("speed")
    condition = weather.get("weather", [{}])[0].get("main")

    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )

    cursor = connection.cursor()

    sql = """
        INSERT INTO weather_events (
            airport,
            event_time,
            temperature,
            humidity,
            wind_speed,
            condition_main
        )
        VALUES (%s, NOW(), %s, %s, %s, %s)
    """

    cursor.execute(sql, (
        event.get("airport"),
        temperature,
        humidity,
        wind_speed,
        condition
    ))

    connection.commit()
    cursor.close()
    connection.close()


def main():
    consumer = KafkaConsumer(
        TOPIC_WEATHER_DATA,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print("Consuming and storing weather events...")

    for message in consumer:
        event = message.value
        print("Received:", event.get("airport"))
        insert_weather(event)
        print("Stored in MySQL")

if __name__ == "__main__":
    main()
