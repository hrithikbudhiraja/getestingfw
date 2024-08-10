# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC  %pip install great-expectations azure-storage-blob

# COMMAND ----------

import great_expectations as ge
import pandas as pd
import json
from datetime import datetime 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from azure.storage.blob import BlobServiceClient
import io

# COMMAND ----------

# MAGIC %run /Workspace/Users/hrithikbudhiraja@gmail.com/getestingfw/code/configDetails

# COMMAND ----------

# MAGIC %run /Workspace/Users/hrithikbudhiraja@gmail.com/getestingfw/code/utilities

# COMMAND ----------

def main(tables_required,config):

    # Common config details
    connection_string = config['connection_string']
    source_container_name = config['source_container_name']
    target_container_name = config['target_container_name']
    test_results_container = config['test_results_container']

    for table in tables_required:

        # Table specific config details
        source_blob_name = config[table]['source_blob_name']
        target_blob_name = config[table]['target_blob_name']
        table_name = config[table]['table_name']
        non_nullable_columns = config[table]['non_nullable_columns']
        non_duplicate_columns = config[table]['non_duplicate_columns']
        selective_matching_validation_columns = config[table]['selective_matching_validation_columns']
        mean_value_validation_columns = config[table]['mean_value_validation_columns']
        max_value_validation_columns = config[table]['max_value_validation_columns']
        min_value_validation_columns = config[table]['min_value_validation_columns']
        median_value_validation_columns = config[table]['median_value_validation_columns']
        email_validation_column = config[table]['email_validation_columns']
        now = datetime.now()
        test_results_file_name = f"{table}_{now}_results.csv"
        
        # Read source dataframe 
        try:
            source_df = read_csv_from_azure(connection_string, source_container_name, source_blob_name)
            source_ge_df = ge.from_pandas(source_df)
        except:
            raise Exception(f"unable to read source file:{source_blob_name}")
        
        # Read target dataframe
        try:
            target_df = read_csv_from_azure(connection_string, target_container_name, target_blob_name)
            target_ge_df = ge.from_pandas(source_df)
        except:
            raise Exception(f"unable to read source file:{target_blob_name}")


        # Null validation
        null_validation_result = validate_nulls(target_ge_df,non_nullable_columns)
        print("Null validation is completed")

        # Duplicate validation
        duplicate_validation_result = validate_duplicates(target_ge_df,non_duplicate_columns)
        print("Duplicate validation is completed")

        # Count validation
        count_validation_result = validate_count(target_ge_df,source_ge_df)
        print("Count validation is completed")

        # Selective matching validation
        selective_column_matching_validation_result = validate_selective_column_matching(target_ge_df,selective_matching_validation_columns)
        print("Selective column matching validation is completed")

        # Mean value validation
        column_mean_value_validation_result = validate_column_mean_value(target_ge_df, mean_value_validation_columns)
        print(" Column mean value validation is completed")

        # Max value validation
        column_max_value_validation_result = validate_column_max_value(target_ge_df, max_value_validation_columns)
        print(" Column max value validation is completed")

        # Min value validation
        column_min_value_validation_result = validate_column_min_value(target_ge_df, min_value_validation_columns)
        print(" Column min value validation is completed")

        # Median value validation
        column_median_value_validation_result = validate_column_median_value(target_ge_df, median_value_validation_columns)
        print("column median value validation is completed")

        # Email value validation
        email_validation_result = validate_email(target_ge_df,email_validation_column)
        print ("Email value validation is completed")

        validation_results = combine_results(null_validation_result,duplicate_validation_result,count_validation_result,     selective_column_matching_validation_result,column_mean_value_validation_result,column_max_value_validation_result,column_min_value_validation_result,column_median_value_validation_result,email_validation_result)

        validation_results_json = json.dumps(validation_results,indent=4)

        results_df = get_results_in_table_format(target_blob_name, source_blob_name, validation_results_json)

        results_pandas_df = results_df.toPandas()

        csv_data = results_pandas_df.to_csv(index=False).encode('utf-8')

        try:
            write_results_to_azure(csv_data,connection_string,test_results_container,test_results_file_name)
        except:
            raise Exception("unable to write results")        

        return results_df

# COMMAND ----------

results_df = main(["marks_report"],config)
display(results_df)
