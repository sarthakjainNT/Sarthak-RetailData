"""
Manages Spark session creation with configurations for ETL tasks and
Handles dynamic resource allocation and error logging for smooth Spark operations.
"""
from pyspark.sql import SparkSession
def get_spark(jar_path=None,name = ""):

    try:
        return (
            SparkSession.builder.appName(name)
            .config("spark.jars", jar_path)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "8g")
            .config("spark.master", "local[*]")
            .config("spark.dynamicAllocation.enabled","true")
            # .config("spark.sql.legacy.allowDeletingFilesInTempDir", "true")
            .getOrCreate()
        )
    except Exception as e:
        raise Exception(f"Unexpected error: {e}")
        sys.exit(1)