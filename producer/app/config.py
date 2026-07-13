import os


KAFKA_TOPIC = "votes"


KAFKA_SERVER = os.getenv(
    "KAFKA_SERVER",
    "localhost:9092"
)