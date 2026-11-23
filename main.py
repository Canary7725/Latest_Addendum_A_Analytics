import json
import os
import shutil

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as spark_types

from download_latest_file import download_file_from_url_and_extract


def load_config(config_path="config.json"):
    with open(config_path, encoding="utf-8") as config_file:
        return json.load(config_file)


def build_schema(columns_mapper):
    conversion_dict = {
        item["source_col_name"]: item["new_name"]
        for item in columns_mapper
    }

    def build_spark_type(column_config, type_key):
        type_name = column_config[type_key]
        spark_type = getattr(spark_types, type_name)

        if type_name == "DecimalType":
            return spark_type(
                column_config["precision"], column_config["scale"]
            )

        return spark_type()

    original_schema = spark_types.StructType([
        spark_types.StructField(
            item["source_col_name"],
            build_spark_type(item, "input_spark_type"),
            True,
        )
        for item in columns_mapper
    ])

    final_schema = spark_types.StructType([
        spark_types.StructField(
            item["new_name"],
            build_spark_type(item, "spark_type"),
            True,
        )
        for item in columns_mapper
    ])

    pandas_schema = {
        item["source_col_name"]: item["pandas_dtype"]
        for item in columns_mapper
    }
    numeric_columns = [
        item["source_col_name"]
        for item in columns_mapper
        if item.get("clean_as_numeric", False)
    ]

    return (
        conversion_dict,
        original_schema,
        final_schema,
        pandas_schema,
        numeric_columns,
    )


def read_pandas_df(
    file_path,
    pandas_schema,
    numeric_columns,
    conversion_dict,
    use_xlsx,
    skip_rows,
):
    print(f"Reading file: {file_path}")
    usecols = list(pandas_schema.keys())

    if use_xlsx:
        df = pd.read_excel(
            file_path,
            usecols=usecols,
            dtype=pandas_schema,
            skiprows=skip_rows,
        )
    else:
        df = pd.read_csv(
            file_path,
            usecols=usecols,
            encoding="latin1",
            skiprows=skip_rows,
            dtype=pandas_schema,
        )

    df = clean_pandas_df(df, numeric_columns)
    df.rename(columns=conversion_dict, inplace=True)
    return df


def clean_pandas_df(df, numeric_columns):
    df = df.fillna("")

    for column in df.columns:
        df[column] = (
            df[column]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )

    df["APC"] = df["APC"].apply(lambda value: value.zfill(4))

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def create_spark_dataframe(spark, original_schema, pandas_df):
    return spark.createDataFrame(pandas_df, schema=original_schema)


def save_original_parquet(df, output_path):
    df.write.mode("overwrite").parquet(output_path)
    print(f"Initial dataframe saved to {output_path}")


def load_and_rename(spark, input_path, conversion_dict, final_schema):
    df = spark.read.parquet(input_path)

    for old_column, new_column in conversion_dict.items():
        if old_column in df.columns:
            df = df.withColumnRenamed(old_column, new_column)

    for field in final_schema.fields:
        if field.name in df.columns:
            df = df.withColumn(
                field.name,
                F.col(field.name).cast(field.dataType),
            )

    return df


def save_final_parquet(df, output_path):
    df.write.mode("overwrite").parquet(output_path)
    print(f"Final dataframe saved to {output_path}")


def read_final_parquet(spark, path):
    return spark.read.parquet(path)


def test_dataframe(df):
    checks = {
        "APC codes": ("apc", r"^\d{4}$"),
        "Payment Rate": ("payment_rate", r"^\d+(\.\d{3})?$"),
        "Minimum Unadjusted Copayment": (
            "min_unadjusted_copay",
            r"^\d+(\.\d{2})?$",
        ),
        "Adjusted Beneficiary Copayment": (
            "adj_benefi_copay",
            r"^\d+(\.\d{2})?$",
        ),
    }

    for description, (column_name, pattern) in checks.items():
        invalid_count = df.filter(
            F.col(column_name).isNull()
            | ~F.col(column_name).cast(spark_types.StringType()).rlike(pattern)
        ).count()

        if invalid_count > 0:
            raise ValueError(f"{description} validation failed: {invalid_count} rows")

    print("All tests passed!")


def main():
    config = load_config()
    source_config = config["source"]
    paths = config["paths"]
    use_xlsx = source_config["file_type"].lower() == "xlsx"
    expected_extension = ".xlsx" if use_xlsx else ".csv"

    try:
        input_file = download_file_from_url_and_extract(
            source_config["url"],
            output_dir=paths["download_dir"],
            expected_extension=expected_extension,
        )

        schema_config = build_schema(config["columns_mapper"])
        (
            conversion_dict,
            original_schema,
            final_schema,
            pandas_schema,
            numeric_columns,
        ) = schema_config

        pandas_df = read_pandas_df(
            input_file,
            pandas_schema,
            numeric_columns,
            conversion_dict,
            use_xlsx,
            source_config["skip_rows"],
        )

        with SparkSession.builder.appName("Addendum A Analytics").getOrCreate() as spark:
            spark_df = create_spark_dataframe(
                spark,
                original_schema,
                pandas_df,
            )
            save_original_parquet(spark_df, paths["initial_output"])

            renamed_df = load_and_rename(
                spark,
                paths["initial_output"],
                conversion_dict,
                final_schema,
            )
            save_final_parquet(renamed_df, paths["final_output"])

            final_df = read_final_parquet(spark, paths["final_output"])
            test_dataframe(final_df)
    finally:
        if os.path.exists(paths["download_dir"]):
            shutil.rmtree(paths["download_dir"])


if __name__ == "__main__":
    main()
