from kafka import KafkaProducer
import json
import os

from config import KAFKA_SERVER

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_vote(topic: str, data: dict):
    producer.send(topic, value=data)
    producer.flush()