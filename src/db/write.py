from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from datetime import datetime
import json
import re
import time
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import declarative_base, sessionmaker
from models import Promotion, Base

connection = "postgresql://admin:admin@postgres:5432/moja_baza"
engine = create_engine(connection)

Session = sessionmaker(bind=engine)

Base.metadata.create_all(engine)

# Czekamy w pętli aż Kafka w pełni wstanie (szczególnie ważne na Raspberry Pi)
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            'test-topic-3',
            bootstrap_servers=['kafka-service:9092'],
            auto_offset_reset='earliest',
            group_id='db-writer-group',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("[DB Writer] Połączono z Kafką!")
    except NoBrokersAvailable:
        print("[DB Writer] Kafka jeszcze nie jest gotowa, ponawiam próbę za 3 sekundy...")
        time.sleep(3)


def to_float(val):
    if val is None or val == "-":
        return None
    if isinstance(val, (int, float)):
        return float(val)
    tekst = str(val).strip().replace(",", ".").replace(" ", "").replace("\xa0", "")
    m = re.search(r"(\d+(?:\.\d+)?)", tekst)
    return float(m.group(1)) if m else None


buffer = []
BATCH_SIZE = 100


def flush_buffer(buf):
    if not buf:
        return
    try:
        with Session() as session:
            stmt = insert(Promotion).on_conflict_do_nothing(constraint="unique_row")
            session.execute(stmt, buf)
            session.commit()
        print(f"[DB] Zapisano paczkę {len(buf)} rekordów do bazy.")
    except Exception as e:
        print(f"[DB BŁĄD] Nie udało się zapisać paczki do bazy: {e}")


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

            price_promo = to_float(data[7])
            # Pomijamy puste/uszkodzone rekordy
            if not data[2] or data[2] == "-" or not price_promo or price_promo == 0.0:
                continue

            price_old = to_float(data[8]) or price_promo
            price_omnibus = to_float(data[9]) or price_old

            buffer.append({
                "date": datetime.now().date(),
                "shop": str(data[1]),
                "product_name": str(data[2]),
                "producer": str(data[3]) if data[3] is not None else "-",
                "category": str(data[4]) if data[4] is not None else "-",
                "category_second": str(data[5]) if data[5] is not None else "-",
                "product_url": str(data[6]) if data[6] is not None else "-",
                "price_promo": price_promo,
                "price_old": price_old,
                "price_omnibus": price_omnibus,
                "promo_code": str(data[10]) if data[10] is not None else "-"
            })

            if len(buffer) >= BATCH_SIZE:
                flush_buffer(buffer)
                buffer = []


