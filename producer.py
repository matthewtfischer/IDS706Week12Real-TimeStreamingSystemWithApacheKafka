import time
import json
import uuid
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

CITIES = [
    "New York", "San Francisco", "Los Angeles", "Chicago", "Seattle",
    "Austin", "Boston", "Miami", "Denver", "Atlanta"
]
STATUSES = ["completed", "cancelled", "no_show"]
PAYMENT_METHODS = ["Credit Card", "Cash", "Mobile Pay", "Wallet"]
VEHICLE_TYPES = ["Sedan", "SUV", "Bike", "Luxury"]

def generate_trip():
    """Generate a synthetic ride-sharing trip event."""
    start_time = datetime.now() - timedelta(seconds=random.randint(0, 120))
    duration_seconds = random.randint(60, 3600)
    end_time = start_time + timedelta(seconds=duration_seconds)
    distance_km = round(random.uniform(0.5, 35.0), 2)
    base_fare = 2.50
    per_km = 1.25
    surge = random.choice([1.0, 1.0, 1.2, 1.5])
    price = round((base_fare + distance_km * per_km) * surge, 2)

    return {
        "trip_id": str(uuid.uuid4())[:8],
        "driver_id": f"drv_{random.randint(1000, 9999)}",
        "rider_id": f"rdr_{random.randint(10000, 99999)}",
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "city": random.choice(CITIES),
        "status": random.choices(STATUSES, weights=(85,10,5), k=1)[0],
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat() if random.random() > 0.02 else None,
        "duration_seconds": duration_seconds,
        "distance_km": distance_km,
        "price": price,
        "payment_method": random.choice(PAYMENT_METHODS),
        "rating": round(random.uniform(3.0, 5.0), 1) if random.random() > 0.05 else None,
        "timestamp": datetime.now().isoformat()
    }

def run_producer():
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")
        count = 0
        while True:
            trip = generate_trip()
            print(f"[Producer] Sending trip #{count}: {trip['trip_id']} city={trip['city']} price=${trip['price']}")
            future = producer.send("trips", value=trip)
            record_metadata = future.get(timeout=10)
            print(f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
            producer.flush()
            count += 1
            sleep_time = random.uniform(0.2, 1.0)
            time.sleep(sleep_time)
    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_producer()
