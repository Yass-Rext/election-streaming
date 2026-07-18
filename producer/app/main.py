import time

from config import KAFKA_TOPIC
from generator import generate_vote
from kafka_client import send_vote


print("=" * 60)
print("Kafka Producer démarré")
print("=" * 60)

while True:

    vote = generate_vote()

    send_vote(KAFKA_TOPIC, vote)

    localisation = (
        vote["region"]
        if vote["type"] == "SENEGAL"
        else vote["pays"]
    )

    print(
        f"[{vote['type']}] "
        f"{localisation} -> "
        f"{vote['candidat']}"
    )

    time.sleep(2)