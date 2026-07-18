import json

from kafka import KafkaProducer

from config import KAFKA_SERVER


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
)


def send_vote(topic: str, data: dict):
    producer.send(topic, data)
    producer.flush()