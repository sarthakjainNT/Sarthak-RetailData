"""
This module transforms bronze data to silver by applying cleaning rules and business logic,
Quarantines invalid records and ensures data readiness for the warehouse to build star schema tables for analytics.
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

from common.logger import get_logger
from common.SparkSession import get_spark
from utils import read_bronze, clean_transform
from transform_and_report import (
    Dropduplicate,
    reject_nulls,
    apply_business_rules,
    transform_to_silver,
    save_dq_metrics,
)
from dimensions import build_dimensions

# creates logger for this script
logger = get_logger("CleanAndTransform")
# Load environment variables
load_dotenv()


# central config dictionary with .env and fallback for airflow paths
def get_config():
    """
    Loads configuration from environment variables with fallbacks.

    Returns:
        dict: A dictionary of configurations.

    Raises:
        KeyError: If required environment variables are missing.
        ValueError: If configuration values are invalid.
    """
    try:
        return {
            "JDBC_URL": os.getenv(
                "JDBC_URL", "jdbc:mysql://host.docker.internal:3306/retail_analysis_db"
            ),
            "JDBC_USER": os.getenv("JDBC_USER", "root"),
            "JDBC_PASS": os.getenv("JDBC_PASS", "3605"),
            "BRONZE_TABLE": os.getenv("BRONZE_TABLE", "bronze_retail"),
            "SILVER_TABLE": os.getenv("SILVER_TABLE", "silver_retail"),
            "DQ_DIR": os.getenv("DQ_DIR", "/opt/airflow/logs/dq_report"),
            "QUARANTINE_DIR": os.getenv(
                "QUARANTINE_DIR", "/opt/airflow/logs/quarantine"
            ),
            "JAR_PATH": os.getenv(
                "JAR_PATH", "/opt/airflow/jars/mysql-connector-j-9.4.0.jar"
            ),
            "JDBC_DRIVER": "com.mysql.cj.jdbc.Driver",
        }
    except ValueError as e:
        logger.error(f"Invalid configuration value: {e}")
        raise


# Here the main driver starts... This is the main orchestrator of the cleaning pipeline
def main():
    config = get_config()

    # will not raise error if already exists
    os.makedirs(config["DQ_DIR"], exist_ok=True)
    os.makedirs(config["QUARANTINE_DIR"], exist_ok=True)

    # get the spark here it starts with JDBC driver
    spark = get_spark(config["JAR_PATH"], "RetailSilverCleaning")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.sparkContext.setLogLevel("OFF")
    # read bronze table from mysql
    try:
        bronze = read_bronze(
            spark,
            jdbc_url=config["JDBC_URL"],
            jdbc_user=config["JDBC_USER"],
            jdbc_pass=config["JDBC_PASS"],
            bronze_table=config["BRONZE_TABLE"],
            jdbc_driver=config["JDBC_DRIVER"],
        )
        # after reading all the rows we log here
        logger.info(f"Bronze rows read")

        # clean the raw data, remove duplicate transaction id and remove nulls in the manditory fields
        cleaned = clean_transform(bronze)
        deduped = Dropduplicate(
            cleaned,
            config["JDBC_URL"],
            config["JDBC_USER"],
            config["JDBC_PASS"],
            config["JDBC_DRIVER"],
            config["QUARANTINE_DIR"],
        )
        no_nulls = reject_nulls(
            deduped,
            config["JDBC_URL"],
            config["JDBC_USER"],
            config["JDBC_PASS"],
            config["JDBC_DRIVER"],
            config["QUARANTINE_DIR"],
        )
        # applies the business rules for valid age,payment methods, phone, zipcode etc..... and drops duplicates for safety
        business_cleaned, business_errors = apply_business_rules(
            no_nulls, config["QUARANTINE_DIR"]
        )
        business_cleaned = business_cleaned.dropDuplicates(["transaction_id"])

        # Full silver dataset (includes customer attributes needed for dims)
        final_full = transform_to_silver(business_cleaned)
        # Build dimensions and write the final fact table silver_retail
        build_dimensions(
            final_full,
            spark,
            config["JDBC_URL"],
            config["JDBC_USER"],
            config["JDBC_PASS"],
            config["JDBC_DRIVER"],
            config["QUARANTINE_DIR"],
            config["SILVER_TABLE"],
        )
        # Save DQ metrics like null checks counts and all
        save_dq_metrics(
            spark,
            deduped,
            final_full,
            config["DQ_DIR"],
            config["JDBC_URL"],
            config["JDBC_USER"],
            config["JDBC_PASS"],
            config["JDBC_DRIVER"],
        )

        logger.info("Clean to Silver pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
