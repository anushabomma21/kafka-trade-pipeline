import os
import io
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from fastavro import schemaless_reader, parse_schema
import psycopg2
from pydantic import BaseModel

KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'trades')
GROUP_ID = os.getenv('KAFKA_GROUP', 'trade_processor')

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'tradesdb')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'trader')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'traderpass')

_schema_path = os.path.join(os.path.dirname(__file__), 'avro', 'trade.avsc')
with open(_schema_path, 'r') as f:
    schema = parse_schema(json.load(f))

class TradeModel(BaseModel):
    trade_id: str
    timestamp: str
    instrument: str
    side: str
    quantity: int
    price: float
    trader_id: str = None
    venue: str = None

def init_db(conn):
    sql_file = os.path.join(os.path.dirname(__file__), '..', '..', 'sql', 'init.sql')
    with open(sql_file, 'r') as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()

def get_db_conn():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    return conn

def enrich_and_persist(record: dict, conn):
    q = record.get('quantity')
    p = record.get('price')
    notional = float(q) * float(p)
    regulatory_flag = notional > 1000000
    processed_at = datetime.utcnow().isoformat()

    with conn.cursor() as cur:
        cur.execute(
            '''INSERT INTO trades (trade_id, instrument, side, quantity, price, notional, trader_id, venue, timestamp, processed_at, regulatory_flag)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (trade_id) DO NOTHING
            ''',
            (
                record.get('trade_id'),
                record.get('instrument'),
                record.get('side'),
                record.get('quantity'),
                record.get('price'),
                notional,
                record.get('trader_id'),
                record.get('venue'),
                record.get('timestamp'),
                processed_at,
                regulatory_flag,
            )
        )
        conn.commit()

def run_consumer():
    print('Starting Kafka consumer...')
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        consumer_timeout_ms=1000,
    )

    conn = None
    for i in range(10):
        try:
            conn = get_db_conn()
            init_db(conn)
            break
        except Exception as ex:
            print('Waiting for Postgres...', ex)
            time.sleep(3)
    if conn is None:
        raise SystemExit('Could not connect to Postgres')

    try:
        while True:
            for msg in consumer:
                try:
                    bio = io.BytesIO(msg.value)
                    record = schemaless_reader(bio, schema)
                    trade = TradeModel(**record)
                    enrich_and_persist(trade.dict(), conn)
                    print('Processed', trade.trade_id)
                except Exception as e:
                    print('Failed to process message', e)
            time.sleep(1)
    finally:
        if conn:
            conn.close()
        consumer.close()

if __name__ == '__main__':
    run_consumer()
