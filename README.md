# Real-Time Fraud Data Platform

## Overview

This project implements an end-to-end data pipeline for detecting fraudulent transactions using streaming and batch processing.

It simulates a real-world payment system where transaction data is ingested, processed, and transformed into fraud detection features in near real-time.

---

## Architecture

Kafka (simulated) → File Landing → Bronze → Silver → Gold

* **Bronze**: Raw ingestion (append-only)
* **Silver**: Cleaned, deduplicated data with watermarking
* **Gold**: Feature engineering using sliding window aggregations

---

## Tech Stack

* Apache Spark (Structured Streaming)
* Delta Lake
* Kafka (Docker-based simulation)
* Databricks (Community Edition)
* Python

---

## Key Features

* Stateful stream processing (deduplication + watermarking)
* Sliding window aggregations for fraud detection
* Approximate distinct counting for scalability
* Hybrid execution (streaming logic with micro-batch scheduling)
* Orchestrated pipeline (Bronze → Silver → Gold)

---

## Batch vs Streaming

This project implements both:

* Batch pipelines for historical processing
* Streaming pipelines for near real-time feature generation

---

## Limitations

* File-based ingestion used instead of direct Kafka integration
* `availableNow` trigger used due to Databricks CE limitations
* Auto Loader not used (not supported in CE)

---

## Project Structure

* `ingestion/` → Kafka producer & consumer
* `pipelines/` → batch & streaming logic
* `orchestration/` → job configuration
* `docs/` → architecture and execution details
