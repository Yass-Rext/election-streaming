import time
from generator import generate_vote
from kafka_client import send_vote
from config import KAFKA_TOPIC

print("Producer Kafka démarré...")

while True:
    vote = generate_vote()

    send_vote(KAFKA_TOPIC, vote)

    print(f"Vote envoyé: {vote['type']} | {vote['region'] if vote['type']=='SENEGAL' else vote['zone']}")

    time.sleep(2)  # 1 vote par seconde (simulation)