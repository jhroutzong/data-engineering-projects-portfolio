from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Connect to local Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Realistic NYC boroughs
boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]

print("Starting taxi ride producer... Press Ctrl+C to stop")
try:
    while True:
        ride = {
            "ride_id": random.randint(1000000, 9999999),
            "timestamp": datetime.utcnow().isoformat(),
            "borough": random.choice(boroughs),
            "passengers": random.randint(1, 6),
            "fare_amount": round(random.uniform(5.0, 150.0), 2),
            "tip_amount": round(random.uniform(0.0, 30.0), 2)
        }
        
        producer.send('taxi-rides', value=ride)
        print(f"Sent: {ride['borough']} - ${ride['fare_amount']}")
        time.sleep(1.5)  # ~40 rides per minute
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.flush()
    producer.close()