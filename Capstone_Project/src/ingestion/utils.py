"""
Utility function or helper function for standarize columns,validate filename, schema alignment,
and metadata addition in the ETL pipeline.
Includes helpers like log_missing_columns to keep records of files and lower_columns for consistent data handling.
"""

import re
from pyspark.sql.functions import col, lit, current_timestamp,regexp_replace
from pyspark.sql.types import StringType


# standardize cols to lowercase and trims so that it will be more easy to handle
def lower_columns(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])


def validate_filename(fname):
    # strict pattern to ensure naming convention
    return bool(re.match(r"^retail_\d{8}_\d{6}\.csv$", fname))


# clean and cast the values
def cast_and_clean(df):
    numeric_columns = [
        "transaction_id",
        "customer_id",
        "age",
        "year",
        "month",
        "total_purchases",
        "ratings",
    ]
    columns_to_clean = [
        "name",
    ]
    for col_name in numeric_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast("int"))
    for col_name in columns_to_clean:
        if col_name in df.columns:
        # Use regexp_replace to remove commas and semicolons from the data in the column
            df = df.withColumn(col_name, regexp_replace(col(col_name), '[,;]', ''))
    return df


# adds info or metadata
def add_metadata(df, fname):
    return df.withColumn("source_file", lit(fname)).withColumn(
        "ingestion_time", current_timestamp()
    )


# add missing columns as None
def log_missing_columns(df, schema, missing_cols):
    for col_name in missing_cols:
        dtype = next(
            (f.dataType for f in schema.fields if f.name == col_name), StringType()
        )
        df = df.withColumn(col_name, lit(None).cast(dtype))
    return df
