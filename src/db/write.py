from kafka import KafkaConsumer
from datetime import datetime
import json
from sqlalchemy import create_engine
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
message_count = 0
for message in consumer:
    data = message.value['message']
    message_count += 1
    
    # try:
    #     float(data[7])
    #     float(data[8])
    #     float(data[9])
    # except:
    #     print("value error")
    #     continue

    new_insert = Promotion(
        date=datetime.now().date(),
        shop=data[1],
        product_name=data[2],
        producer=data[3],
        category=data[4],
        category_second=data[5],
        product_url=data[6],
        price_promo=data[7],
        price_old=data[8],
        price_omnibus=data[9],
        promo_code=data[10]
    )

    buffer.append(new_insert)

    if message_count == 100:
        with Session() as session:
            session.add_all(buffer)
            session.commit()
            buffer.clear()
            message_count = 0

