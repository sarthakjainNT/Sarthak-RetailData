"""
Function for writing DataFrames to MySQL and logging ingestion stats, thos
Ensures efficient and error-free data writes to the database.
"""

import logging


def write_to_bronze(df, table, jdbc_config, mode="append"):
    try:
        df = (
            df.repartition(10)
            .write.format("jdbc")
            .option("batchsize", "1000")
            .options(**jdbc_config, dbtable=table)
            .mode(mode)
            .save()
        )
    except Exception as e:
        logging.error(f"Failed to write to {table}: {e}")
        raise


# this write the ingestion stats into the ingestion log table
def write_logs(
    spark, log_table, jdbc_config, fname, rows_read, rows_written, errors, message
):
    log_df = spark.createDataFrame(
        [(fname, rows_read, rows_written, errors, message)],
        schema="source_file STRING, rows_read INT, rows_written INT, errors INT, message STRING",
    )
    log_df.write.format("jdbc").options(**jdbc_config, dbtable=log_table).mode(
        "append"
    ).save()
