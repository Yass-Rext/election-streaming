FROM apache/spark:3.5.1

USER root

# Python
RUN pip3 install --no-cache-dir \
    pandas \
    psycopg2-binary \
    sqlalchemy \
    boto3 \
    minio

# Dossier de l'application
WORKDIR /app

# Copie du code Spark
COPY app/ /app/

# Dossier Ivy pour télécharger les packages Spark
RUN mkdir -p /tmp/.ivy2 \
    && chmod -R 777 /tmp/.ivy2

ENV HOME=/tmp
ENV SPARK_LOCAL_DIRS=/tmp/spark-local

CMD [ "/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
    "/app/app.py" \
]
