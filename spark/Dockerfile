FROM apache/spark:3.5.1

USER root

# Dépendances Python pour les sinks (JDBC helpers / diagnostics)
RUN pip3 install --no-cache-dir \
    pandas \
    psycopg2-binary \
    sqlalchemy \
    boto3 \
    minio

WORKDIR /app

COPY app/ /app/

# Script d'attente des dépendances
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
