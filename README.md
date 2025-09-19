This repo contains a Kafka trade pipeline using FastAPI, Avro, Kafka, and Postgres. Run with docker compose up and test via /trades endpoint.

# Kafka Trade Pipeline
This project implements a simple **real-time trade processing pipeline** using:

- FastAPI (REST API for trade ingestion)
- Pydantic (validation)
- Avro (serialization)
- Kafka (message broker)
- PostgreSQL (storage)
- Docker Compose (local setup)

## How it works
1. A trade is sent to the FastAPI endpoint `/trades`.
2. The API validates the request and publishes the trade to a Kafka topic (`trades`) in Avro format.
3. A Python consumer reads from Kafka, validates again, enriches the trade by computing **notional** and setting a **regulatory flag**.
4. The enriched trade is stored in PostgreSQL for reporting.

Example Output:
| trade\_id | instrument | quantity | price  | notional | regulatory\_flag |
| --------- | ---------- | -------- | ------ | -------- | ---------------- |
| T-1000    | AAPL       | 100      | 172.50 | 17250.00 | f                |


