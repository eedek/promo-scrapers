from kafka import KafkaConsumer
from datetime import datetime
import json
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import declarative_base, sessionmaker
from models import Promotion, Base
connection = "postgresql://admin:admin@postgres:5432/moja_baza"
engine = create_engine(connection)

Session = sessionmaker(bind=engine)
# Base = declarative_base()

Base.metadata.create_all(engine)
consumer = KafkaConsumer(
    'test-topic-3',
    bootstrap_servers=['kafka-service:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
buffer = []
BATCH_SIZE = 100


def flush_buffer(buf):
    if not buf:
        return
    with Session() as session:
        stmt = insert(Promotion).on_conflict_do_nothing(constraint="unique_row")
        session.execute(stmt, buf)
        session.commit()
    print(f"[DB] Zapisano paczkę {len(buf)} rekordów do bazy.")


while True:
    records = consumer.poll(timeout_ms=2000)

    if not records:
        # Jeśli przez 2 sekundy nie przyszły nowe wiadomości, zapisujemy końcówkę bufora (< 100 rekordów)
        if buffer:
            flush_buffer(buffer)
            buffer = []
        continue

    for topic_partition, messages in records.items():
        for message in messages:
            data = message.value['message']

            # Pomijamy puste/uszkodzone rekordy (np. stare wiadomości z Kafki z "-" i 0.0)
            if not data[2] or data[2] == "-" or data[7] in (0, 0.0, "0", "0.0", "-"):
                continue

            buffer.append({
                "date": datetime.now().date(),
                "shop": data[1],
                "product_name": data[2],
                "producer": data[3],
                "category": data[4],
                "category_second": data[5],
                "product_url": data[6],
                "price_promo": data[7],
                "price_old": data[8],
                "price_omnibus": data[9],
                "promo_code": data[10]
            })

            if len(buffer) >= BATCH_SIZE:
                flush_buffer(buffer)
                buffer = []

