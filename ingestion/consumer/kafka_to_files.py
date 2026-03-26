import json
from kafka import KafkaConsumer
import os
def safe_deserializer(x):
    try:
        return json.loads(x.decode('utf-8'))
    except:
        return None
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=safe_deserializer
)

output_dir = "landing/transactions"
os.makedirs(output_dir, exist_ok=True)

file_count = 0
buffer = []

print("Consumer started...")

for message in consumer:
    if message.value is None:
        continue  # skip bad messages

    buffer.append(message.value)

    if len(buffer) >= 50:
        file_name = f"{output_dir}/transactions_{file_count}.json"

        with open(file_name, 'w') as f:
            for record in buffer:
                f.write(json.dumps(record) + "\n")

        print(f"Written {len(buffer)} records to {file_name}")

        buffer = []
        file_count += 1