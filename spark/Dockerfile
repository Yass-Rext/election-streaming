FROM apache/spark:3.5.1

USER root

RUN pip3 install --no-cache-dir \
    pandas \
    psycopg2-binary \
    sqlalchemy \
    boto3 \
    minio

WORKDIR /app

COPY app/ .

