# 🚨 Quelques ameliorations importantes

## Le code ci-dessus est une bonne base, mais je te recommande de le faire evoluer avant la soutenance :

## Ecriture dans MinIO
- Aujourd'hui, les votes sont affiches dans la console.
- Il faudra remplacer ce flux par un foreachBatch(write_batch()) de sinks/minio.py pour sauvegarder les evenements en Parquet.
- Un seul pipeline avec plusieurs sorties
- Actuellement, chaque agregation cree son propre writeStream.
- C'est simple pour demarrer, mais en production on prefere lire Kafka une seule fois et distribuer les traitements plus efficacement.
- Gestion des dependances
- Il faudra ajouter les packages Spark necessaires :
    - Spark Kafka
    - PostgreSQL JDBC
    - Hadoop AWS (pour MinIO)