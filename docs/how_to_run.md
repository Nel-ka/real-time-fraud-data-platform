# How to Run

1. Start Kafka
   docker run -d --name kafka -p 9092:9092 apache/kafka:latest

2. Run producer
   python ingestion/producer/transaction_generator.py

3. Run consumer
   python ingestion/consumer/kafka_to_files.py

4. Upload generated JSON files to Databricks Volume:
   /Volumes/workspace/default/stream/

5. Run Databricks job:
   Bronze → Silver → Gold

6. Query final output:
   gold/streaming/fraud_features