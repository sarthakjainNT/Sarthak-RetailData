"""
Ingests CSV files to the bronze layer, validates schemas and filename,clean and cast the columns , and writes to MySQL.
Handles incremental loading, error quarantine, and logging for robust ETL processing.
"""

import os, sys
from dotenv import load_dotenv

# Set up src path and environment
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
SRC_PATH = os.path.join(ROOT_DIR, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# Load environment variables
load_dotenv(os.path.join(ROOT_DIR, ".env"))
os.environ["PYTHONPATH"] = SRC_PATH

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from config import get_config
from utils import (
    lower_columns,
    validate_filename,
    cast_and_clean,
    add_metadata,
    log_missing_columns,
)
from write import write_to_bronze, write_logs
from common.logger import get_logger
from common.SparkSession import get_spark

logger = get_logger("Ingestion")
""" this is our schema that i defined for the consistency of schema """
schema = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("country", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("income", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("date", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("total_purchases", IntegerType(), True),
        StructField("amount", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_brand", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("feedback", StringType(), True),
        StructField("shipping_method", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("ratings", IntegerType(), True),
        StructField("products", StringType(), True),
    ]
)

""" this is a validate schema func that validate the schema of dataframe against the predefined schema and if there are mismatches
it logs the issue, quarantine the bad data and adds the missing cols as null and return the dataframe with the error count.
"""


def validate_schema(df, schema, spark, fname, log_table, jdbc_config, quarantine_dir):
    expected_cols = [f.name for f in schema.fields]
    missing_cols = [c for c in expected_cols if c not in df.columns]
    extra_cols = [c for c in df.columns if c not in expected_cols]

    error_count = 0
    msg = ""

    if missing_cols or extra_cols:
        error_count += 1
        msg = f"Schema check failed. Missing: {missing_cols}, Extra: {extra_cols}"
        logger.warning(msg)

        # Move bad schema to quarantine
        try:
            quarantined_df = df.select(*expected_cols)
            df.write.mode("append").parquet(
                f"{quarantine_dir}/bad_schema/"
            )  # Quarantine the original DF
            logger.info(f"Quarantined bad schema for {fname} with extra columns.")
        except Exception as e:
            logger.error(f"Failed to quarantine bad schema for {fname}: {e}")
        try:
            write_logs(
                spark,
                log_table,
                jdbc_config,
                fname,
                df.count(),
                df.count(),
                error_count,
                msg,
            )
        except Exception as e:
            logger.error(f"logging failed (schema issue): {e}")

    # Add missing columns as Null
    cleaned_df = df.select(*expected_cols)
    return df, error_count


def main():
    config = get_config()
    spark = get_spark(config["JAR_PATH"], "RetailBronzeIngestion")
    spark.sparkContext.setLogLevel("OFF")
    # JDBC configs dictionary for read & write operation in the database
    jdbc_config = {
        "url": config["JDBC_URL"],
        "user": config["JDBC_USER"],
        "password": config["JDBC_PASS"],
        "driver": config["JDBC_DRIVER"],
    }

    # Get last ingestion timestamp from the metadata tbl
    meta_table = config["METADATA_TABLE"]
    try:
        last_time = (
            spark.read.jdbc(jdbc_config["url"], meta_table, properties=jdbc_config)
            .select("last_ingestion_time")
            .first()["last_ingestion_time"]
        )
    except Exception:
        last_time = datetime.min
        logger.warning("No previous metadata found, ingesting all files.")

    # Find new files based on modification timestamp
    new_files = [
        f
        for f in os.listdir(config["INPUT"])
        if f.endswith(".csv")
        and datetime.fromtimestamp(os.path.getmtime(os.path.join(config["INPUT"], f)))
        > last_time
    ]

    if not new_files:
        logger.info("No new files to process.")
        spark.stop()
        return
    # checks for the filename and start ingestion
    for fname in new_files:
        try:
            if not validate_filename(fname):
                logger.warning(f"Invalid filename: {fname}, moving to quarantine.")
                spark.read.csv(
                    os.path.join(config["INPUT"], fname), header=True
                ).write.mode("append").parquet(f"{config['QUARANTINE_DIR']}/invalid/")
                write_logs(
                    spark,
                    config["INGEST_LOG_TABLE"],
                    jdbc_config,
                    fname,
                    0,
                    0,
                    1,
                    "Invalid filename",
                )
                continue

            # Read & process
            df = spark.read.csv(
                os.path.join(config["INPUT"], fname), header=True, inferSchema=True
            )
            df = df.transform(lower_columns)
            df, error_count = validate_schema(
                df,
                schema,
                spark,
                fname,
                config["INGEST_LOG_TABLE"],
                jdbc_config,
                config["QUARANTINE_DIR"],
            )
            if error_count > 0:
                logger.info(f"Skipped invalid schema for {fname} after quarantining.")

            df = cast_and_clean(df)

            if df.isEmpty():
                logger.warning(f"No valid data in {fname}")
                continue

            # Add metadata
            df = add_metadata(df, fname)

            # Write to Bronze
            write_to_bronze(
                df, config["BRONZE_TABLE"], jdbc_config, mode=config["mode"]
            )
            write_logs(
                spark,
                config["INGEST_LOG_TABLE"],
                jdbc_config,
                fname,
                df.count(),
                df.count(),
                error_count,
                "OK",
            )
            logger.info(f"{fname} ingested successfully!")

        except Exception as e:
            logger.error(f"Error processing {fname}: {e}")

    # Update last ingestion timestamp
    spark.createDataFrame(
        [(datetime.now(), fname)], ["last_ingestion_time", "file_name"]
    ).write.jdbc(
        jdbc_config["url"], meta_table, mode="overwrite", properties=jdbc_config
    )

    logger.info("Incremental Bronze ingestion complete.")
    spark.stop()


if __name__ == "__main__":
    main()
