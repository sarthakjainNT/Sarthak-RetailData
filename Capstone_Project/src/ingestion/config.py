"""
This module is central hub for configurations of files anddd creds from environment variables ensure security
It supports both local and airflow env and validate the required keys

"""

import os
import sys
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

""" fucntion that parses JDBC credentials and file paths from .env and defaults for airflow """


def get_config():
    try:
        config = {
            "JDBC_URL": os.getenv(
                "JDBC_URL", "jdbc:mysql://host.docker.internal:3306/retail_analysis_db"
            ),
            "JDBC_USER": os.getenv("JDBC_USER", "root"),
            "JDBC_PASS": os.getenv("JDBC_PASS", "3605"),
            "BRONZE_TABLE": os.getenv("BRONZE_TABLE", "bronze_retail"),
            "SILVER_TABLE": os.getenv("SILVER_TABLE", "silver_retail"),
            "DQ_DIR": os.getenv("DQ_DIR", "/opt/airflow/logs/dq_report"),
            "QUARANTINE_DIR": os.getenv("QUARANTINE_DIR", "store/quarantine"),
            "JAR_PATH": os.getenv(
                "JAR_PATH", "/opt/airflow/jars/mysql-connector-j-9.4.0.jar"
            ),
            "JDBC_DRIVER": "com.mysql.cj.jdbc.Driver",
            "METADATA_TABLE": "metadata_table",
            "mode": os.getenv("DEFAULT_MODE", "append"),
            "INPUT": os.getenv("DEFAULT_INPUT", "/opt/airflow/data/raw"),
            "INGEST_LOG_TABLE": os.getenv("INGEST_LOG_TABLE", "ingestion_log"),
        }
        """ Basic validation ensures required configs are there otherwise it will exit with proepr log """
        required_keys = ["INPUT", "JDBC_URL", "JDBC_USER", "JDBC_PASS"]
        for key in required_keys:
            if not config[key]:
                raise KeyError(f"Missing required config: {key}")
    except KeyError as e:
        logging.error(f"Configuration KeyError: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)
    return config
