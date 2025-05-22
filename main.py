import decimal
import json
import os
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from download_latest_file import download_file_from_url_and_extract




def load_config():
    with open('./config.json') as config:
        return json.load(config)
    

def build_schema(columns_mapper):
    conversion_dict = {
        item.get("source_col_name") : item["new_name"]
        for item in columns_mapper
    }
      
    spark_schema_original=StructType([
          StructField(col,StringType(),True)
          for col in conversion_dict.keys()
    ])
    #REVIEW: no need to use original schema

    spark_schema_final = StructType([
        StructField("apc", StringType(), True),
        StructField("group_title", StringType(), True),
        StructField("si", StringType(), True),
        StructField("payment_rate", DecimalType(10, 3), True),
        StructField("min_unadjusted_copay", DecimalType(10, 2), True),
        StructField("adj_benefi_copay", DecimalType(10, 2), True),
    ])
      
    pandas_schema = {
        "APC": "string",
        "Group Title": "string",
        "SI": "string",
        "Payment Rate": "float",
        "Minimum Unadjusted Copayment": "float",
        "Adjusted Beneficiary Copayment": "float"
    }
    # REVIEW: you can use a key named 'data_type' in the config json and use that to for schema in pandas, no need to define separate schema here
    # code line will be optimized that way

    return conversion_dict, spark_schema_original, spark_schema_final, pandas_schema

def read_pandas_df(pandas_schema,conversion_dict,use_xlsx):
    if use_xlsx:
        file_name = next((f for f in os.listdir('.') if f.endswith('.xlsx')), None)
    else:
        file_name = next((f for f in os.listdir('.') if f.endswith('.csv')), None)

    if file_name is None:
        raise FileNotFoundError("No matching .xlsx or .csv file found.")

    print("reading file name",file_name)
    usecols = list(pandas_schema.keys()) #Only use cols specified in the pandas_schema

    if use_xlsx:
        df = pd.read_excel(file_name, usecols=usecols, dtype=str,skiprows=2)
    else:
        df = pd.read_csv(file_name, usecols=usecols, encoding='latin1', skiprows=2, dtype=str)

    df = clean_pandas_df(df)
    df.rename(columns=conversion_dict,inplace=True)
    return df


def clean_pandas_df(df):
    df = df.fillna('')

    for col in df.columns:
        df[col] = df[col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip() #Removed the '$', ',' and strip  for easier type conversion

    df['APC'] = df['APC'].apply(lambda x: x.zfill(4)) #append 3 digits apc code to 4 digits

    numeric_cols = ["Payment Rate","Minimum Unadjusted Copayment", "Adjusted Beneficiary Copayment"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def create_spark_dataframe(spark,spark_schema_original,pandas_df):
    return spark.createDataFrame(pandas_df, schema=spark_schema_original)


def save_original_parquet(df):
    df.write.mode('overwrite').parquet('./initial_parquet')
    print("Initial dataframe saved")


def load_and_rename(spark, input_path, conversion_dict, final_schema):
    df = spark.read.parquet(input_path)    
    # Rename columns using conversion_dict values (new column names)
    for old_col, new_col in conversion_dict.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
    
    # Convert data types as per final schema
    for field in final_schema.fields:
        col_name = field.name
        data_type = field.dataType
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(data_type))
    
    return df

def save_final_parquet(df):
    df.write.mode("overwrite").parquet('./final_parquet')
    print("Final DataFrame saved")


def read_final_parquet(spark, path):
    return spark.read.parquet(path)

def test_dataframe(df):
    apc_check = df.filter(~(col("apc").rlike(r"^\d{4}$")))
    assert apc_check.count() == 0, "APC codes are not 4-digit numeric"

    payment_check = df.filter(
        ~col("payment_rate").cast(StringType()).rlike(r"^\d+(\.\d{3})?$")
    )
    assert payment_check.count() == 0, "Payment Rate does not have 3 decimal places"

    min_copay_check = df.filter(
        ~col("min_unadjusted_copay").cast(StringType()).rlike(r"^\d+(\.\d{2})?$")
    )
    assert min_copay_check.count() == 0, "Minimum Unadjusted Copayment does not have 2 decimal places"

    adj_copay_check = df.filter(
        ~col("adj_benefi_copay").cast(StringType()).rlike(r"^\d+(\.\d{2})?$")
    )
    assert adj_copay_check.count() == 0, "Adjusted Beneficiary Copayment does not have 2 decimal places"

    print("All tests passed!")

def main():
    use_xlsx=True
    #REVIEW: usage of file as xls or csv should be passed as param to main while calling the py file
    try:
        download_file_from_url_and_extract('https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates')
        # REVIEW:use the url in the config 
        columns_mapper = load_config()
        conversion_dict, spark_schema_original, spark_schema_final, pandas_schema = build_schema(columns_mapper['columns_mapper'])

        pandas_df=read_pandas_df(pandas_schema,conversion_dict,use_xlsx)

        with SparkSession.builder.appName('Addendum A Analytics').getOrCreate() as spark:
            spark_df=create_spark_dataframe(spark,spark_schema_original,pandas_df)
            save_original_parquet(spark_df)
            df_renamed=load_and_rename(spark,'./initial_parquet',conversion_dict,spark_schema_final)
            save_final_parquet(df_renamed)
            final_df=read_final_parquet(spark,'./final_parquet')
            test_dataframe(final_df)
    except Exception as e:
        print(e)  
    finally:      
        file_name=[f for f in os.listdir('.') if f.endswith('.csv') or f.endswith('.xlsx')]
        for file in file_name:
            os.remove(file)

if __name__ == "__main__":
    main()