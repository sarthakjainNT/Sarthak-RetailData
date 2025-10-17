"""
This is a module in which we use utility functions where we read bronze table, clean and tranform each columns to maintain the 
effeciency and make cast_to_jdbc_safe to avoid the type error and make the df appropriate for the jdbc.
"""
from pyspark.sql.functions import (
    col,
    when,
    lit,
    coalesce,
    regexp_replace,
    to_timestamp,
    date_format,
    trim,
    lpad,
)
from pyspark.sql.types import StringType, IntegerType
from common.logger import get_logger

# this create logger for this file with this passed name
logger = get_logger("CleanAndTransform")


# this reads the table from mysql and logs the row count
def read_bronze(spark, jdbc_url, jdbc_user, jdbc_pass, bronze_table, jdbc_driver):
    logger.info(f"Loading Bronze table `{bronze_table}` from JDBC")
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", bronze_table)
            .option("user", jdbc_user)
            .option("password", jdbc_pass)
            .option("driver", jdbc_driver)
            .option("numPartitions", 8)
            .option("batchsize", "10000")
            .load()
        )
        logger.info(
            f"Loaded bronze table `{bronze_table}` (schema cols: {len(df.columns)})"
        )
        return df
    except Exception as e:
        logger.error(f"Failed reading bronze table `{bronze_table}`: {e}")
        raise


"""cast null typed values to string to avoid schema mismatch or null exeception, 
   I used this only for airflow as spark not allows null values and gives error in the logs"""


def cast_to_jdbc_safe(df):
    for c, t in df.dtypes:
        if t == "null":
            if c.lower() == "age":
                df = df.withColumn(c, lit(0).cast(IntegerType()))
            else:
                df = df.withColumn(c, col(c).cast(StringType()))
    return df


# core cleaning part
def clean_transform(df):
    try:
        df = df.toDF(*[c.strip().lower() for c in df.columns])

        # for Amount this remove , and negative nulls
        if "amount" in df.columns:
            df = df.withColumn(
                "amount", regexp_replace(col("amount"), ",", "").cast("double")
            ).withColumn(
                "amount", when(col("amount") < 0, None).otherwise(col("amount"))
            )

        # cleans phone and zip digit only we use lpad for left pad
        if "phone" in df.columns:
            df = df.withColumn(
                "phone", regexp_replace(col("phone"), "[^0-9]", "")
            ).withColumn("phone", lpad(col("phone"), 10, "0"))

        if "zipcode" in df.columns:
            df = df.withColumn(
                "zipcode", regexp_replace(col("zipcode"), "[^0-9]", "")
            ).withColumn("zipcode", lpad(col("zipcode"), 6, "0"))

        # Product info
        if "products" in df.columns:
            df = df.withColumn(
                "products",
                when(
                    col("products").isNull() | (trim(col("products")) == ""),
                    lit("Unknown_Product"),
                ).otherwise(col("products")),
            )

        if "product_category" in df.columns:
            df = df.withColumn(
                "product_category",
                when(
                    col("product_category").isNull() | (col("product_category") == ""),
                    lit("Misc"),
                ).otherwise(col("product_category")),
            )

        # Customer info
        df = df.withColumn(
            "customer_id",
            when(
                col("customer_id").isNull()
                | (trim(col("customer_id").cast("string")) == ""),
                lit("Unknown_Customer"),
            ).otherwise(col("customer_id").cast("string")),
        )

        # Ratings, age, gender, income
        if "ratings" in df.columns:
            df = df.withColumn(
                "ratings",
                coalesce(
                    when((col("ratings") < 1) | (col("ratings") > 5), 0).otherwise(
                        col("ratings")
                    ),
                    lit(0),  # Replace null values with 0
                ),
            )
        if "age" in df.columns:
            df = df.withColumn(
                "age",
                when((col("age") < 10) | (col("age") > 100), None).otherwise(
                    col("age")
                ),
            )
        if "gender" in df.columns:
            df = df.withColumn(
                "gender",
                when(
                    col("gender").isNull() | (col("gender") == "U"), lit("Unknown")
                ).otherwise(col("gender")),
            )
        if "income" in df.columns:
            df = df.withColumn(
                "income",
                when(col("income") == "Med", lit("Medium")).otherwise(col("income")),
            )

        # Payment method
        if "payment_method" in df.columns:
            df = df.withColumn(
                "payment_method",
                when(col("payment_method") == "GPay", lit("Other")).otherwise(
                    col("payment_method")
                ),
            )

        # Date parsing
        if "date" in df.columns:
            df = df.withColumn("date_raw", trim(col("date").cast("string")))
            df = df.withColumn(
                "transaction_ts",
                coalesce(
                    to_timestamp(col("date_raw"), "yyyy-MM-dd HH:mm:ss"),
                    to_timestamp(col("date_raw"), "yyyy-MM-dd"),
                    to_timestamp(col("date_raw"), "dd/MM/yyyy"),
                    to_timestamp(col("date_raw"), "MM/dd/yyyy"),
                    to_timestamp(col("date_raw"), "MMM-dd-yyyy"),
                    to_timestamp(col("date_raw"), "dd-MMM-yyyy"),
                    to_timestamp(col("date_raw"), "yyyy/MM/dd"),
                ),
            )
            df = df.withColumn(
                "date_id",
                when(
                    col("transaction_ts").isNotNull(),
                    date_format(col("transaction_ts"), "yyyyMMdd").cast("int"),
                ),
            )
        else:
            df = df.withColumn("transaction_ts", lit(None))
            df = df.withColumn("date_id", lit(None).cast("int"))

        # Fill defaults for missing values
        defaults = {
            k: v
            for k, v in {
                "feedback": "No Feedback",
                "product_brand": "Unknown Brand",
                "customer_segment": "Unknown",
            }.items()
            if k in df.columns
        }
        if defaults:
            df = df.fillna(defaults)

        # Cast to JDBC safe and return
        df = cast_to_jdbc_safe(df)
        logger.info("Clean_transform complete")
        return df
    except Exception as e:
        logger.error(f"clean_transform failed: {e}")
        raise
