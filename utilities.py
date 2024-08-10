# Databricks notebook source
# MAGIC %pip install great-expectations azure-storage-blob

# COMMAND ----------

import great_expectations as ge
import pandas as pd
import json
from datetime import datetime 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from azure.storage.blob import BlobServiceClient
import io

#---------------------------------------Data reading---------------------------------------
# Read data from azure blob storage
def read_csv_from_azure(connection_string, container_name, blob_name):
    blob_service_client=BlobServiceClient.from_connection_string(connection_string)
    blob_client=blob_service_client.get_blob_client(container_name,blob_name)
    blob_content=blob_client.download_blob().content_as_text()

    df = pd.read_csv(io.StringIO(blob_content))
    return df

#------------------------------------------------------------------------------------------

#---------------------------------------Validations---------------------------------------
# Validate nulls
def validate_nulls(df,column_list):
    null_validation_result=[]
    for column in column_list:
        null_validation_result.append(df.expect_column_values_to_not_be_null(column))
    return null_validation_result

# Validate duplicates
def validate_duplicates(df,column_list):
    duplicate_validation_result = df.expect_compound_columns_to_be_unique(column_list)
    return duplicate_validation_result

# Validate count
def validate_count(target_df,source_df):
    source_count = source_df.shape[0]
    count_validation_result = target_df.expect_table_row_count_to_equal(source_count)
    return count_validation_result

# Validate selective column matching
def validate_selective_column_matching(df,column_list):
    selective_column_matching_validation_result = df.expect_table_columns_to_match_set(column_list,exact_match = False)
    return selective_column_matching_validation_result

# Validate column mean value
def validate_column_mean_value(df, column_list):
    column_mean_value_validation_result = []
    for column in column_list:
        column_mean_value_validation_result.append(df.expect_column_mean_to_be_between(column["column_name"],column["min_value"],column["max_value"]))
    return column_mean_value_validation_result

# Validate column min value
def validate_column_min_value(df, column_list):
    column_min_value_validation_result = []
    for column in column_list:
        column_min_value_validation_result.append(df.expect_column_min_to_be_between(column["column_name"],column["min_value"],column["max_value"]))
    return column_min_value_validation_result

# Validate column max value
def validate_column_max_value(df, column_list):
    column_max_value_validation_result = []
    for column in column_list:
        column_max_value_validation_result.append(df.expect_column_max_to_be_between(column["column_name"],column["min_value"],column["max_value"]))
    return column_max_value_validation_result


# Validate column median value
def validate_column_median_value(df, column_list):
    column_median_value_validation_result = []
    for column in column_list:
        column_median_value_validation_result.append(df.expect_column_median_to_be_between(column["column_name"],column["min_value"],column["max_value"]))
    return column_median_value_validation_result

# Validate email
def validate_email(df,column_list):
    email_validation_result=[]
    pattern = "r^.+@.+\..{2,}$"
    for column in column_list:
        email_validation_result.append(df.expect_column_values_to_match_regex(column,pattern))
    return email_validation_result
    
#------------------------------------------------------------------------------------------

#------------------------------------Formatting results------------------------------------

# Combining results in dictionary format
def combine_results(null_validation_result,duplicate_validation_result,count_validation_result,             selective_matching_validation_result,mean_value_validation_result,max_value_validation_result,min_value_validation_result,median_value_validation_result,email_validation_result):
    return {
        "null_validation_result": [result.to_json_dict() for result in null_validation_result],
        "duplicate_validation_result": duplicate_validation_result.to_json_dict(),
        "count_validation_result": count_validation_result.to_json_dict(),
        "selective_matching_validation_result": selective_matching_validation_result.to_json_dict(),
        "mean_value_validation_result": [result.to_json_dict() for result in mean_value_validation_result],
        "max_value_validation_result": [result.to_json_dict() for result in max_value_validation_result],        "min_value_validation_result": [result.to_json_dict() for result in min_value_validation_result],        "median_value_validation_result": [result.to_json_dict() for result in median_value_validation_result],
        "email_validation_result":[result.to_json_dict() for result in email_validation_result],
    }

# Loading results in a dataframe format
def get_results_in_table_format(target_table_name, source_table_name,validation_results_json):
    result_list = []
    data = json.loads(validation_results_json)
    
    null_validation_result = data["null_validation_result"]
    duplicate_validation_result = data["duplicate_validation_result"]
    count_validation_result = data["count_validation_result"]
    selective_matching_validation_result = data["selective_matching_validation_result"]
    mean_value_validation_result = data["mean_value_validation_result"]
    max_value_validation_result = data["max_value_validation_result"]
    min_value_validation_result = data["min_value_validation_result"]
    median_value_validation_result = data["median_value_validation_result"]
    email_validation_result = data["email_validation_result"]

    validation_scenario = "null_validation_result"
    for i in range(len(null_validation_result)):
        expectation = null_validation_result[i]["expectation_config"]["expectation_type"]
        column = null_validation_result[i]["expectation_config"]["kwargs"]["column"]
        success = null_validation_result[i]["success"]
        expected_min = None
        expected_max = None
        target_count = null_validation_result[i]["result"]["element_count"]
        source_count = None
        unexpected_count = null_validation_result[i]["result"]["unexpected_count"]
        result_list.append([target_table_name,source_table_name,validation_scenario,expectation,column,success,expected_min,expected_max,target_count,source_count,unexpected_count])

    validation_scenario = "duplicate_validation_result"
    expectation = duplicate_validation_result["expectation_config"]["expectation_type"]
    success = duplicate_validation_result["success"]
    expected_min = None
    expected_max = None
    target_count = duplicate_validation_result["result"]["element_count"]
    source_count = None
    unexpected_count = duplicate_validation_result["result"]["unexpected_count"]
    column_list = duplicate_validation_result["expectation_config"]["kwargs"]["column_list"]
    for i in range(len(column_list)):
        column = column_list[i]
        result_list.append([target_table_name,source_table_name,validation_scenario,expectation,column,success,expected_min,expected_max,target_count,source_count,unexpected_count])
        
    validation_scenario = "count_validation_result"
    expectation = count_validation_result["expectation_config"]["expectation_type"]
    sucess = count_validation_result["success"]
    expected_min = None
    expected_max = None
    source_count = count_validation_result["expectation_config"]["kwargs"]["value"]
    target_count = count_validation_result["result"]["observed_value"]
    unexpected_count = target_count - source_count
    column = None
    result_list.append([target_table_name,source_table_name,validation_scenario,expectation,column,success,expected_min,expected_max,target_count,source_count,unexpected_count])

    validation_scenario = "mean_value_validation_result"
    for i in range(len(mean_value_validation_result)):
        expectation = mean_value_validation_result[i]["expectation_config"]["expectation_type"]
        column = mean_value_validation_result[i]["expectation_config"]["kwargs"]["column"]
        success = mean_value_validation_result[i]["success"]
        expected_min = mean_value_validation_result[i]["expectation_config"]["kwargs"]["min_value"]
        expected_max = mean_value_validation_result[i]["expectation_config"]["kwargs"]["max_value"]
        source_count = None
        target_count = None
        unexpected_count = None
        result_list.append([target_table_name,source_table_name,validation_scenario,expectation,column,success,expected_min,expected_max,target_count,source_count,unexpected_count])

    validation_scenario = "max_value_validation_result"
    for i in range(len(max_value_validation_result)):
        expectation = max_value_validation_result[i]["expectation_config"]["expectation_type"]
        column = max_value_validation_result[i]["expectation_config"]["kwargs"]["column"]
        success = max_value_validation_result[i]["success"]
        expected_min = max_value_validation_result[i]["expectation_config"]["kwargs"]["min_value"]
        expected_max = max_value_validation_result[i]["expectation_config"]["kwargs"]["max_value"]
        source_count = None
        target_count = None
        unexpected_count = None
        result_list.append([target_table_name,source_table_name,validation_scenario,expectation,column,success,expected_min,expected_max,target_count,source_count,unexpected_count])

    validation_scenario = "min_value_validation_result"
    for i in range(len(min_value_validation_result)):
        expectation = min_value_validation_result[i]["expectation_config"]["expectation_type"]
        column = min_value_validation_result[i]["expectation_config"]["kwargs"]["column"]
        success = min_value_validation_result[i]["success"]
        expected_min = min_value_validation_result[i]["expectation_config"]["kwargs"]["min_value"]
        expected_max = min_value_validation_result[i]["expectation_config"]["kwargs"]["max_value"]
        source_count = None
        target_count = None
        unexpected_count = None
        result_list.append([target_table_name,source_table_name,validation_scenario,expectation,column,success,expected_min,expected_max,target_count,source_count,unexpected_count])


    validation_scenario = "median_value_validation_result"
    for i in range(len(median_value_validation_result)):
        expectation = median_value_validation_result[i]["expectation_config"]["expectation_type"]
        column = median_value_validation_result[i]["expectation_config"]["kwargs"]["column"]
        success = median_value_validation_result[i]["success"]
        expected_min = median_value_validation_result[i]["expectation_config"]["kwargs"]["min_value"]
        expected_max = median_value_validation_result[i]["expectation_config"]["kwargs"]["max_value"]
        source_count = None
        target_count = None
        unexpected_count = None
        result_list.append([target_table_name,source_table_name,validation_scenario,expectation,column,success,expected_min,expected_max,target_count,source_count,unexpected_count]) 

        validation_scenario = "email_validation_reuslt"
        for i in range(len(email_validation_result)):
            expectation = email_validation_result[i]["expectation_config"]["expectation_type"]
            column = email_validation_result[i]["expectation_config"]["kwargs"]["column"]
            success = email_validation_result[i]["success"]
            expected_min = None
            expected_max = None
            source_count = None
            target_count = email_validation_result[i]["result"]["element_count"]
            unexpected_count = email_validation_result[i]["result"]["unexpected_count"]
            result_list.append([target_table_name,source_table_name,validation_scenario,expectation,column,success,expected_min,expected_max,target_count,source_count,unexpected_count])

    validation_scenario = "selective_matching_validation_result"
    expectation = selective_matching_validation_result["expectation_config"]["expectation_type"]
    success = selective_matching_validation_result["success"]
    expected_min = None
    expected_max = None
    source_count = None
    target_count = None
    unexpected_count = None
    column_list = selective_matching_validation_result["expectation_config"]["kwargs"]["column_set"]
    for i in range(len(column_list)):
        column = column_list[i]
        if column in selective_matching_validation_result["result"]["observed_value"]:
            success = True
        else:
            success = False
        result_list.append([target_table_name,source_table_name,validation_scenario,expectation,column,success,expected_min,expected_max,target_count,source_count,unexpected_count])

    results_df = spark.createDataFrame(data = result_list, schema = [
    'target_table_name','source_table_name','validation_scenario', 'expectation', 'column', 'success', 'expected_min', 'expected_max', 'target_count','source_count', 'unexpected_count'
    ])

    return results_df
        
#------------------------------------------------------------------------------------------

#---------------------------------------Writing test results-------------------------------

# Writing tests results to Azure storage
def write_results_to_azure(data,connection_string,output_container,file_name):
    
    connection_string=connection_string
    container_name=output_container
    blob_name=file_name

    blob_service_client=BlobServiceClient.from_connection_string(connection_string)

    container_client=blob_service_client.get_container_client(container_name)
    blob_client=container_client.get_blob_client(blob_name)

    blob_client.upload_blob(data, overwrite=False)


# def write_results_to_azure(df,connection_string,output_container,file_name):
#     # Azure Blob Storage path (replace with your actual container name)
#     container_name = output_container
#     output_path = f"abfss://{container_name}@{connection_string}.dfs.core.windows.net/{output_container}/{file_name}"

#     # Write DataFrame to Azure Blob Storage as CSV
#     df.write.mode("overwrite").format("csv").option("header", "true").save(output_path)


#------------------------------------------------------------------------------------------


