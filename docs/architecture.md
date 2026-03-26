# Architecture

## Data Flow

Kafka → File Landing → Bronze → Silver → Gold

### Ingestion

Kafka is simulated locally using Docker. Events are produced and written to JSON files via a consumer.

### Bronze Layer

* Raw ingestion
* Append-only
* Adds metadata (event_time, ingestion_time)

### Silver Layer

* Data cleaning
* Deduplication using transaction_id
* Watermarking to handle late data

### Gold Layer

* Sliding window aggregations (5-minute window)
* Fraud feature generation:

  * Transaction count
  * Average amount
  * Country diversity
* Fraud rule:
  txn_count > threshold AND country_count > 1

---

## Streaming Behavior

* Micro-batch execution using `availableNow`
* Watermark controls late data
* Window output depends on watermark progression

---

## Batch vs Streaming

Batch:

* Full data processing
* Simpler logic

Streaming:

* Stateful processing
* Handles late data
* Near real-time feature generation
