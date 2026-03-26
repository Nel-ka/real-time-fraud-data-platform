import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = [f"user_{i}" for i in range(1, 101)]
merchants = [f"merchant_{i}" for i in range(1, 21)]
countries = ["IN", "US", "UK", "AE"]
devices = ["iphone", "android", "web"]

def generate_transaction():
    return {
        "transaction_id": f"txn_{random.randint(100000,999999)}",
        "user_id": random.choice(users),
        "merchant_id": random.choice(merchants),
        "amount": round(random.uniform(10, 5000), 2),
        "currency": "INR",
        "country": random.choice(countries),
        "device_id": random.choice(devices),
        "timestamp": datetime.utcnow().isoformat()
    }

while True:
    event = generate_transaction()
    producer.send("transactions", value=event)
    print(event)
    time.sleep(0.2)