import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

def run_consumer():
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "trips",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trips-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips (
                trip_id VARCHAR(50) PRIMARY KEY,
                driver_id VARCHAR(50),
                rider_id VARCHAR(50),
                vehicle_type VARCHAR(50),
                city VARCHAR(100),
                status VARCHAR(20),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds INTEGER,
                distance_km NUMERIC(6,2),
                price NUMERIC(10,2),
                payment_method VARCHAR(50),
                rating NUMERIC(2,1),
                timestamp TIMESTAMP
            );
            """
        )
        print("[Consumer] ✓ Table 'trips' ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        message_count = 0
        for message in consumer:
            try:
                trip = message.value
                def parse_ts(ts):
                    if ts is None:
                        return None
                    try:
                        return datetime.fromisoformat(ts)
                    except Exception:
                        return None

                cur.execute(
                    """
                    INSERT INTO trips (
                        trip_id, driver_id, rider_id, vehicle_type, city, status,
                        start_time, end_time, duration_seconds, distance_km, price,
                        payment_method, rating, timestamp
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (trip_id) DO NOTHING;
                    """,
                    (
                        trip.get("trip_id"),
                        trip.get("driver_id"),
                        trip.get("rider_id"),
                        trip.get("vehicle_type"),
                        trip.get("city"),
                        trip.get("status"),
                        parse_ts(trip.get("start_time")),
                        parse_ts(trip.get("end_time")),
                        trip.get("duration_seconds"),
                        trip.get("distance_km"),
                        trip.get("price"),
                        trip.get("payment_method"),
                        trip.get("rating"),
                        parse_ts(trip.get("timestamp"))
                    ),
                )
                message_count += 1
                print(f"[Consumer] ✓ #{message_count} Inserted trip {trip.get('trip_id')} | {trip.get('city')} | ${trip.get('price')}")
            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue
    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_consumer()
