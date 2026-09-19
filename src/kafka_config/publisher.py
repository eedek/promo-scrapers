from kafka import KafkaProducer
import json

def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

# Send a message


producer = create_kafka_producer()

producer.send('test-topic', {'message': 'Hello, Kafka!'})
producer.flush()
print("Message sent successfully!")