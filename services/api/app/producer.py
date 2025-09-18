import io
import json
import os
from kafka import KafkaProducer
from fastavro import schemaless_writer, parse_schema

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades")

_schema_path = os.path.join(os.path.dirname(__file__), "avro", "trade.avsc")
with open(_schema_path, "r") as f:
    schema = parse_schema(json.load(f))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: v,
    retries=5
)

def avro_serialize(record: dict) -> bytes:
    out = io.BytesIO()
    schemaless_writer(out, schema, record)
    return out.getvalue()

def publish_trade(record: dict):
    b = avro_serialize(record)
    future = producer.send(TOPIC, value=b)
    result = future.get(timeout=10)
    return result
