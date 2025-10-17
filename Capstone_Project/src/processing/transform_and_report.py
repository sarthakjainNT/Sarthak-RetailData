"""
Transforms data to silver schema, imputes values, and generates data quality reports.
Optimizes for performance and data quality in the ETL pipeline, also handles the duplicates and nulls in the data.
"""

import os
from datetime import datetime
from functools import reduce

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    row_number,
    to_json,
    when,
    coalesce,
    struct,
    avg,
    round,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from common.logger import get_logger

# creates logger for this file
logger = get_logger("CleanAndTransform")


# it keeps latest records and send duplicates to the quearntine and error_duplicates in mysql
def Dropduplicate(df, jdbc_url, user, password, jdbc_driver, quarantine_dir):
    """
    Keep latest per transaction_id (by transaction_ts). Send duplicates to both
    quarantine parquet and an error table (error_duplicates).
    """
    try:
        logger.info("Checking for duplicate transactions by transaction_id")
        w = Window.partitionBy("transaction_id").orderBy(
            col("transaction_ts").desc_nulls_last()
        )
        df_rn = df.withColumn("rn", row_number().over(w))

        kept = df_rn.filter(col("rn") == 1).drop("rn")
        duplicates = df_rn.filter(col("rn") > 1).drop("rn")
        # this gives a proper view with error reason in table
        if duplicates.limit(1).count() > 0:
            logger.info(
                "Found duplicate transaction_id exporting to error_duplicates table and quarantine (parquet)"
            )
            duplicates_out = duplicates.select(
                col("transaction_id"),
                to_json(
                    struct(
                        *[col(c) for c in duplicates.columns if c != "transaction_id"]
                    )
                ).alias("row_json"),
            ).withColumn("error_reason", lit("duplicate_transaction"))
            try:
                duplicates_out.write.format("jdbc").options(
                    url=jdbc_url,
                    dbtable="error_duplicates",
                    user=user,
                    password=password,
                    driver=jdbc_driver,
                ).mode("append").save()
            except Exception as e:
                logger.error(f"Failed writing duplicates to JDBC: {e}")

            try:
                os.makedirs(quarantine_dir, exist_ok=True)
                duplicates.write.mode("append").parquet(
                    os.path.join(quarantine_dir, "duplicates")
                )
                logger.info(
                    f"Duplicates written to quarantine: {os.path.join(quarantine_dir, 'duplicates')}"
                )
            except OSError as e:
                logger.error(f"Failed writing duplicates to quarantine: {e}")
                raise
        return kept
    except Exception as e:
        logger.error(f"duplicate check failed: {e}")
        raise


# this checks for mandatory columns and move bad rows to silver_errors and keep valid rows
def reject_nulls(
    df, jdbc_url, user, password, jdbc_driver, quarantine_dir, source_identifier=None
):
    """
    Reject rows where mandatory fields are null. Write rejected to silver_errors and quarantine.
    Return only accepted rows (selected columns matching silver).
    """
    try:
        logger.info("Checking mandatory fields")
        mandatory = [c for c in ["transaction_id", "customer_id"] if c in df.columns]
        if not mandatory:
            return df

        null_cond = reduce(lambda a, b: a | b, [col(c).isNull() for c in mandatory])
        rejected = df.filter(null_cond)
        cleaned = df.filter(~null_cond)

        if rejected.limit(1).count() > 0:
            rejected_out = rejected.select(
                col("transaction_id"),
                to_json(
                    struct(
                        *[
                            col(c)
                            for c in rejected.columns
                            if c not in ("transaction_id", "error_reason")
                        ]
                    )
                ).alias("row_json"),
            ).withColumn("error_reason", lit("mandatory_null"))

            try:
                rejected_out.write.format("jdbc").options(
                    url=jdbc_url,
                    dbtable="silver_errors",
                    user=user,
                    password=password,
                    driver=jdbc_driver,
                ).mode("append").save()
            except Exception as e:
                logger.error(f"Failed writing mandatory-null rows to JDBC: {e}")

            try:
                os.makedirs(quarantine_dir, exist_ok=True)
                rejected.write.mode("append").parquet(
                    os.path.join(quarantine_dir, "mandatory_nulls")
                )
            except Exception as e:
                logger.error(f"Failed writing mandatory-null rows to quarantine: {e}")

        cleaned = df.filter(~null_cond)
        desired = [
            "transaction_id",
            "transaction_ts",
            "date_id",
            "customer_id",
            "name",
            "email",
            "address",
            "phone",
            "city",
            "state",
            "zipcode",
            "country",
            "age",
            "gender",
            "income",
            "customer_segment",
            "products",
            "product_category",
            "product_brand",
            "product_type",
            "amount",
            "total_amount",
            "total_purchases",
            "order_status",
            "payment_method",
            "shipping_method",
            "ratings",
            "feedback",
        ]
        # it keeps only valid rows selects column matching tge silver schema
        return cleaned.select(*[c for c in desired if c in cleaned.columns])
    except Exception as e:
        logger.error(f"reject_nulls failed: {e}")
        raise


# valid age ratings amount email gender income payment etc...
def apply_business_rules(df, quarantine_dir=None):
    """
    Apply domain rules and move violations to parquet quarantine and return filtered df and count of errors.
    """
    try:
        logger.info("Applying business rules")
        business_errors = 0

        rules = {
            "invalid_age": (col("age").isNotNull())
            & ((col("age") < 10) | (col("age") > 100)),
            "negative_amounts": col("amount").isNotNull() & (col("amount") < 0),
            "invalid_ratings": col("ratings").isNotNull()
            & ((col("ratings") < 1) | (col("ratings") > 5)),
            "invalid_email": col("email").isNotNull()
            & (
                ~col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
            ),
            "invalid_gender": col("gender").isNotNull()
            & (~col("gender").isin("Male", "Female", "Unknown")),
            "invalid_income": col("income").isNotNull()
            & (~col("income").isin("Low", "Medium", "High")),
            "invalid_payment": col("payment_method").isNotNull()
            & (
                ~col("payment_method").isin(
                    "Credit Card", "Debit Card", "Cash", "PayPal", "Other"
                )
            ),
        }

        for name, condition in rules.items():
            invalid_rows = df.filter(condition)
            if invalid_rows.limit(1).count() > 0 and quarantine_dir:
                os.makedirs(quarantine_dir, exist_ok=True)
                invalid_rows.write.mode("append").parquet(
                    os.path.join(quarantine_dir, name)
                )
                cnt = invalid_rows.count()
                business_errors += cnt
                # after error count we will log here for the tracebility
                logger.info(f"Quarantined {cnt} rows: {name}")
                # here only the clean data is returned and remove the invalid
                df = df.join(
                    invalid_rows.select("transaction_id"),
                    on="transaction_id",
                    how="left_anti",
                )

        return df, business_errors
    except Exception as e:
        logger.error(f"apply_business_rules failed: {e}")
        raise


# adds the total amount, imputes the null amount and adds the timestamp
def transform_to_silver(df):
    """
    Calculate total_amount by computing , calculate the product category avg and global avg as default in case of amount is missing
    and after that it will return all the final cols
    """
    try:
        logger.info("Transforming to silver schema and imputing amounts")
        if "total_amount" in df.columns:
            df = df.withColumn(
                "total_amount", round(col("total_amount").cast(DoubleType()), 2)
            )
        else:
            df = df.withColumn("total_amount", col("total_purchases") * col("amount"))

        # calculate category and global average for imputing nulls and global avf is fallback
        avg_amount = df.groupBy("product_category").agg(
            avg("amount").alias("avg_amount")
        )
        global_avg = df.select(avg("amount").alias("global_avg")).first()["global_avg"]
        df = (
            df.join(avg_amount, on="product_category", how="left")
            .withColumn(
                "amount",
                when(
                    col("amount").isNull(), coalesce(col("avg_amount"), lit(global_avg))
                ).otherwise(col("amount")),
            )
            .drop("avg_amount")
        )
        # add metadata columns
        final = df.withColumnRenamed("transaction_ts", "transaction_date").withColumn(
            "processing_timestamp", current_timestamp()
        )
        # here is all the final columns to populate
        all_cols = [
            "transaction_id",
            "transaction_date",
            "date_id",
            # customer
            "customer_id",
            "name",
            "email",
            "address",
            "phone",
            "city",
            "state",
            "zipcode",
            "country",
            "age",
            "gender",
            "income",
            "customer_segment",
            # product
            "products",
            "product_category",
            "product_brand",
            "product_type",
            # transaction
            "total_purchases",
            "amount",
            "total_amount",
            "order_status",
            "payment_method",
            "shipping_method",
            "ratings",
            "feedback",
            "processing_timestamp",
        ]

        final_cols = [c for c in all_cols if c in final.columns]
        logger.info("Transformation to Silver complete")
        return final.select(*final_cols).dropDuplicates(["transaction_id"])
    except Exception as e:
        logger.error(f"transform_to_silver failed: {e}")
        raise


# compute and savess the data quality report in CSV and MYSQL
def save_dq_metrics(
    spark, deduped, final, dq_dir, jdbc_url, user, password, jdbc_driver
):
    """
    Compute light-weight DQ metrics and save to JDBC and CSV.
    """
    try:
        logger.info("Saving DQ metrics")
        dq = [
            ("rows_total", str(deduped.count())),
            ("rows_final", str(final.count())),
            (
                "null_customer_count",
                str(final.filter(col("customer_id") == "Unknown_Customer").count()),
            ),
            ("null_amount_count", str(final.filter(col("amount").isNull()).count())),
            (
                "invalid_ratings_count",
                str(
                    deduped.filter((col("ratings") < 1) | (col("ratings") > 5)).count()
                ),
            ),
            (
                "invalid_age_count",
                str(deduped.filter((col("age") < 10) | (col("age") > 100)).count()),
            ),
            ("negative_amount_count", str(deduped.filter(col("amount") < 0).count())),
            (
                "mapped_gender_count",
                str(deduped.filter(col("gender") == "Unknown").count()),
            ),
            (
                "mapped_income_count",
                str(deduped.filter(col("income") == "Medium").count()),
            ),
            (
                "mapped_payment_count",
                str(deduped.filter(col("payment_method") == "Other").count()),
            ),
        ]

        dq_df = spark.createDataFrame(dq, ["metric_name", "metric_value"]).repartition(
            1
        )
        dq_df.write.format("jdbc").options(
            url=jdbc_url,
            dbtable="dq_report",
            user=user,
            password=password,
            driver=jdbc_driver,
        ).option("batchsize", "10000").mode("append").save()
        # save in CSV and makes dir if not there
        os.makedirs(dq_dir, exist_ok=True)
        dq_df.coalesce(1).write.mode("overwrite").csv(
            f"{dq_dir}/dq_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv",
            header=True,
        )
        logger.info("DQ metrics saved")
    except Exception as e:
        logger.error(f"Failed saving DQ metrics: {e}")
        raise
