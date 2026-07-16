from pyspark.sql import DataFrame

from config import (
    POSTGRES_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


# ===========================================
# Fonction générique
# ===========================================

def write_postgres(df: DataFrame, table_name: str):

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .mode("overwrite")
        .save()
    )


# ===========================================
# foreachBatch
# ===========================================

def write_batch(table_name: str):

    def _write(batch_df: DataFrame, batch_id: int):

        print(f"Batch {batch_id} -> {table_name}")

        write_postgres(batch_df, table_name)

    return _write