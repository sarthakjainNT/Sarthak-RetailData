"""
This module contains functions to perform ELT operations for building and updating dimension tables,
and the fact table in a data warehouse. here are things like what this reallyyyy doing

-Upserting Dimension Tables: This involves adding new records to dimension tables while avoiding unnecessary overwrites. It ensures dimension tables have surrogate keys for referencing.
-Generating Date Dimension Table: Creates a date dimension table that having a specified range of transaction dates.
-Building and Updating Dimensions: Creates and updates various dimension tables (Customer, Product, Payment, Shipping, Date) by joining them with the fact table and generating surrogate keys.
-Fact Table Updates: Joins the dimension tables to the fact table ensuring only new records are inserted into the fact table.

Functions:
- jdbc_table_exists(spark, jdbc_url, user, password, table_name): Checks if a table exists in the MySQL database.
- align_to_columns(df, target_columns): Aligns a DataFrame to the target column schema to avoid mismatches.
- safe_fill(df, columns, default_val="Unknown"): Fills missing (null) values with a default value to prevent errors.
- upsert_dimension(spark, dim_df, jdbc_opts, table_name, key_col, join_cols): Inserts or updates records in the dimension table.
- generate_date_dim(spark, final_df, lookback_years=5): Creates a date dimension table for a given range of dates.
- build_dimensions(final_df, spark, jdbc_url, user, password, quarantine_dir, fact_table_name): Builds or updates multiple dimension tables and joins them with the fact table.
"""

from datetime import datetime, timedelta
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    coalesce,
    max as spark_max,
    sequence,
    to_date,
    explode,
    year,
    month,
    dayofmonth,
    dayofweek,
    expr,
    when,
)
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, IntegerType, StringType
from common.logger import get_logger
from utils import cast_to_jdbc_safe

# creates logger for this file with the name passed
logger = get_logger("CleanAndTransform")


# This tries to read a table from MySQL and If it fails means table doesn’t exist
def jdbc_table_exists(spark, jdbc_url, user, password, table_name, jdbc_driver):
    """
    Checks if a table exists in the MySQL database, returns true if exists, false otherwise
    """
    try:
        spark.read.format("jdbc").option("url", jdbc_url).option(
            "dbtable", table_name
        ).option("user", user).option("password", password).option(
            "driver", jdbc_driver
        ).load().limit(
            1
        ).first()
        return True
    except Exception:
        return False


""" It aligns the columns with the target cols if the df has a column which is not in target cols then it will make it none
and after it return the new df with all the rows without any data loss
"""


def align_to_columns(df, target_columns):
    exprs = []
    for c in target_columns:
        if c in df.columns:
            exprs.append(col(c))
        else:
            # if not then fill it with none
            exprs.append(lit(None).cast(StringType()).alias(c))
    return df.select(*exprs)


# it is used to fill null values with unknown so no null exception or error faced during airflow dag run
def safe_fill(df, columns, default_val="Unknown"):
    try:
        for c in columns:
            if c in df.columns:
                if c == "age":
                    df = df.withColumn(
                        c, when(col(c).isNull(), lit(0)).otherwise(col(c))
                    )
                else:
                    df = df.withColumn(
                        c, when(col(c).isNull(), lit(default_val)).otherwise(col(c))
                    )
        return df
    except TypeError as e:
        logger.error(f"Type error: {e}")
        raise
    except ValueError as e:
        logger.error(f"Value error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


def upsert_dimension(spark, dim_df, jdbc_opts, table_name, key_col, join_cols):
    """
    Append new rows to dimension table (avoid re-writing entire table).
    This implementation reads minimal columns from existing table where possible.
    """
    try:
        # Ensure join columns are not null and type safe
        dim_df = cast_to_jdbc_safe(dim_df)
        dim_df = safe_fill(dim_df, join_cols or [], default_val="unknown")

        if jdbc_table_exists(
            spark,
            jdbc_opts["url"],
            jdbc_opts["user"],
            jdbc_opts["password"],
            table_name,
            jdbc_opts["driver"],
        ):
            existing = (
                spark.read.format("jdbc")
                .options(**jdbc_opts, dbtable=table_name)
                .load()
            )
            existing = cast_to_jdbc_safe(existing)

            # Align schemas
            full_cols = list(dict.fromkeys(existing.columns + dim_df.columns))
            existing = align_to_columns(existing, full_cols)
            dim_df = align_to_columns(dim_df, full_cols)

            # now leftanti join so it filter only new rows not already in dimension and append them in existings
            if join_cols:
                missing = [c for c in join_cols if c not in dim_df.columns]
                if missing:
                    logger.warning(
                        f"{table_name}: missing join cols in incoming df: {missing}. Skipping append."
                    )
                    return existing
                new_rows = dim_df.join(
                    existing.select(*join_cols), on=join_cols, how="left_anti"
                )
            else:
                new_rows = dim_df

            # Ensure uniqueness on join_cols before key generation (prevents dup keys)
            if join_cols:
                new_rows = new_rows.dropDuplicates(join_cols)

            if new_rows.limit(1).count() > 0:
                # Safe max key
                max_key = existing.agg(
                    coalesce(spark_max(key_col), lit(0)).alias("max_key")
                ).collect()[0]["max_key"]

                # Add surrogate key if missing or null
                if key_col not in new_rows.columns or new_rows.filter(
                    col(key_col).isNull()
                ).take(1):
                    # Window with partitionBy(lit(0)) to avoid warning
                    window_spec = Window.partitionBy(lit(0)).orderBy(*join_cols)

                    new_rows = new_rows.withColumn(
                        key_col,
                        (row_number().over(window_spec) + lit(max_key)).cast(
                            LongType()
                        ),
                    )

                new_rows = align_to_columns(new_rows, full_cols)
                new_rows = cast_to_jdbc_safe(new_rows)

                # Repartition to 1 for small dim tables (faster JDBC write)
                new_rows = new_rows.repartition(1)

                new_rows.write.format("jdbc").options(
                    **jdbc_opts, dbtable=table_name
                ).mode("append").save()

            final_tbl = (
                spark.read.format("jdbc")
                .options(**jdbc_opts, dbtable=table_name)
                .load()
            )
        else:
            # First-time table creation: generate surrogate key for all rows
            # Dedup on join_cols for uniqueness (ensures sequential even for low cardinality)
            if join_cols:
                dim_df = dim_df.dropDuplicates(join_cols)

            window_spec = Window.partitionBy(lit(0)).orderBy(*join_cols)
            dim_df = dim_df.withColumn(
                key_col, row_number().over(window_spec).cast(LongType())
            )

            dim_df = cast_to_jdbc_safe(dim_df)

            # Repartition to 1 for small tables
            dim_df = dim_df.repartition(1)

            dim_df.write.format("jdbc").options(**jdbc_opts, dbtable=table_name).mode(
                "overwrite"
            ).save()
            final_tbl = (
                spark.read.format("jdbc")
                .options(**jdbc_opts, dbtable=table_name)
                .load()
            )

        logger.info(f"{table_name} ready!!!")
        return final_tbl
    except Exception as e:
        logger.error(f"upsert_dimension failed for {table_name}: {e}")
        raise


# builds a date dimention table between min and max transaction dates
def generate_date_dim(spark, final_df, lookback_years: int = 5):
    row = (
        final_df.selectExpr(
            "min(transaction_date) as min_d", "max(transaction_date) as max_d"
        )
        .limit(1)
        .collect()
    )
    min_d = row[0]["min_d"]
    max_d = row[0]["max_d"]

    today = datetime.utcnow().date()
    if min_d is None or max_d is None:
        start_date = today - timedelta(days=lookback_years * 365)
        end_date = today
    else:
        try:
            start_date = (
                min_d.date()
                if hasattr(min_d, "date")
                else datetime.strptime(str(min_d), "%Y-%m-%d").date()
            )
        except Exception:
            start_date = today - timedelta(days=lookback_years * 365)
        try:
            end_date = (
                max_d.date()
                if hasattr(max_d, "date")
                else datetime.strptime(str(max_d), "%Y-%m-%d").date()
            )
        except Exception:
            end_date = today

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    logger.info(f"Generating date dim from {start_str} to {end_str}")

    start = to_date(lit(start_str))
    end = to_date(lit(end_str))
    dates = spark.range(1).select(
        explode(sequence(start, end, expr("interval 1 day"))).alias("date")
    )
    window_spec = Window.orderBy("date")
    date_dim = (
        dates.withColumn("date_key", row_number().over(window_spec).cast(IntegerType()))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
    )

    return date_dim.select("date_key", "date", "year", "month", "day", "day_of_week")


# builds all the dimensions with fact table and join dims with fact
def build_dimensions(
    final_df,
    spark,
    jdbc_url,
    user,
    password,
    jdbc_driver,
    quarantine_dir,
    fact_table_name,
):
    """
    Build customer/product/payment/shipping/date dimensions and write fact (silver) table.
    final_df: cleaned silver dataset
    """
    logger.info("Building/updating dimension tables with surrogate keys...")
    final_df = final_df.cache()
    jdbc_opts = {
        "url": jdbc_url,
        "user": user,
        "password": password,
        "driver": jdbc_driver,
    }

    try:
        # for customer dimension
        cust_cols = [
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
        ]
        customer_df = (
            final_df.select(*[c for c in cust_cols if c in final_df.columns])
            .where(col("customer_id").isNotNull())
            .dropDuplicates(["customer_id"])
            .cache()
        )

        # Replace nulls with 'unknown' for customer dimension
        for c in customer_df.columns:
            if c == "age":
                customer_df = customer_df.withColumn(
                    c, when(col(c).isNull(), lit(0)).otherwise(col(c))
                )
            else:
                customer_df = customer_df.withColumn(
                    c, when(col(c).isNull(), lit("unknown")).otherwise(col(c))
                )

        customer_tbl = upsert_dimension(
            spark,
            customer_df,
            jdbc_opts,
            table_name="silver_customer",
            key_col="customer_key",
            join_cols=["customer_id"],
        )
        customer_df.unpersist()  # Clean up cache

        # for products dimension (No pre-key; upsert handles sequential keys)
        prod_cols = ["products", "product_category", "product_brand", "product_type"]
        product_df = (
            final_df.select(*[c for c in prod_cols if c in final_df.columns])
            .dropDuplicates(prod_cols)
            .distinct()
        )  # Explicit dedup for consistency

        product_tbl = upsert_dimension(
            spark,
            product_df,
            jdbc_opts,
            table_name="silver_product",
            key_col="product_key",
            join_cols=prod_cols,
        )

        # for payment dimension
        if {"payment_method", "order_status"}.issubset(final_df.columns):
            pay_df = (
                final_df.select("payment_method", "order_status")
                .dropDuplicates()
                .distinct()  # Explicit dedup for composite keys
                .where(col("payment_method").isNotNull())
                .cache()  # Cache small dim for fast reuse/joins
            )
            pay_tbl = upsert_dimension(
                spark,
                pay_df,
                jdbc_opts,
                table_name="silver_payment",
                key_col="payment_id",
                join_cols=["payment_method", "order_status"],
            )
            pay_df.unpersist()  # Clean up cache
        else:
            pay_tbl = None

        # for shipping dimension (Explicit dedup + cache for low cardinality)
        if "shipping_method" in final_df.columns:
            ship_df = (
                final_df.select("shipping_method")
                .dropDuplicates()
                .distinct()  # Explicit dedup (ensures exactly N rows for N uniques)
                .where(col("shipping_method").isNotNull())
                .cache()  # Cache small dim
            )
            ship_tbl = upsert_dimension(
                spark,
                ship_df,
                jdbc_opts,
                table_name="silver_shipping",
                key_col="shipping_id",
                join_cols=["shipping_method"],
            )
            ship_df.unpersist()  # Clean up
        else:
            ship_tbl = None

        # for date dimension
        date_df = generate_date_dim(spark, final_df, lookback_years=5)
        date_tbl = upsert_dimension(
            spark,
            date_df,
            jdbc_opts,
            table_name="silver_date",
            key_col="date_key",
            join_cols=["date_key"],
        )

        # here fact table starts
        fact_df = final_df

        # Join customer
        if "customer_key" in customer_tbl.columns:
            fact_df = fact_df.join(
                customer_tbl.select("customer_id", "customer_key"),
                on="customer_id",
                how="left",
            )
        else:
            fact_df = fact_df.withColumn("customer_key", lit(None).cast(LongType()))

        # Join product
        if "product_key" in product_tbl.columns:
            broadcast_prod = product_tbl.select(
                "product_key",
                "products",
                "product_category",
                "product_brand",
                "product_type",
            ).hint(
                "broadcast"
            )  # optimizes using Broadcast if <1M rows
            fact_df = fact_df.join(
                broadcast_prod,
                on=["products", "product_category", "product_brand", "product_type"],
                how="left",
            )
        else:
            fact_df = fact_df.withColumn("product_key", lit(None).cast(LongType()))

        # Join payment
        if pay_tbl is not None and "payment_id" in pay_tbl.columns:
            broadcast_pay = pay_tbl.select(
                "payment_method", "order_status", "payment_id"
            ).hint("broadcast")
            fact_df = fact_df.join(
                broadcast_pay,
                on=["payment_method", "order_status"],
                how="left",
            )
        else:
            fact_df = fact_df.withColumn("payment_id", lit(None).cast(LongType()))

        # Join shipping
        if ship_tbl is not None and "shipping_id" in ship_tbl.columns:
            broadcast_ship = ship_tbl.select("shipping_method", "shipping_id").hint(
                "broadcast"
            )
            fact_df = fact_df.join(
                broadcast_ship,
                on="shipping_method",
                how="left",
            )
        else:
            fact_df = fact_df.withColumn("shipping_id", lit(None).cast(LongType()))

        # Join date dimension
        if "date_id" in fact_df.columns:
            fact_df = fact_df.withColumn(
                "transaction_date",
                to_date(col("date_id").cast(StringType()), "yyyyMMdd"),
            )
            # Join on transaction_date - date in date_dim
            fact_df = fact_df.join(
                date_tbl.select("date_key", "date"),
                fact_df.transaction_date == date_tbl.date,
                how="left",
            )
            fact_df = fact_df.drop("date_id").withColumnRenamed("date_key", "date_id")
        else:
            fact_df = fact_df.withColumn("date_id", lit(None).cast(IntegerType()))

        fact_cols = [
            "transaction_id",
            "transaction_date",
            "date_id",
            "customer_key",
            "product_key",
            "payment_id",
            "shipping_id",
            "amount",
            "total_amount",
            "total_purchases",
            "order_status",
            "ratings",
            "feedback",
            "processing_timestamp",
        ]
        fact_ready = fact_df.select(
            *[c for c in fact_cols if c in fact_df.columns]
        ).dropDuplicates(["transaction_id"])

        # Write fact table in mysql db
        if jdbc_table_exists(
            spark, jdbc_url, user, password, fact_table_name, jdbc_driver
        ):
            existing_fact = (
                spark.read.format("jdbc")
                .options(**jdbc_opts, dbtable=fact_table_name)
                .load()
                .select("transaction_id")
                .distinct()
            )
            fact_ready = fact_ready.join(
                existing_fact, on="transaction_id", how="left_anti"
            )
            write_mode = "append"
        else:
            write_mode = "overwrite"

        if fact_ready.limit(1).count() > 0:
            fact_ready.write.format("jdbc").options(
                **jdbc_opts, dbtable=fact_table_name
            ).option("batchsize", "10000").mode(write_mode).save()
            logger.info(f"Wrote fact table {fact_table_name} (mode={write_mode})")
        else:
            logger.info("No new fact rows to write.")

        return fact_ready
    except Exception as e:
        logger.error(f"Error building dimensions: {e}")
        raise
    finally:
        if "final_df" in locals():
            # i used this as it is useful for freeing up resources when the cached data is no longer needed
            final_df.unpersist()
