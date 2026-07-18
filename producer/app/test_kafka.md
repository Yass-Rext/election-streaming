Les messages arrivent-ils dans Kafka ?

Exécute :

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic votes \
--from-beginning