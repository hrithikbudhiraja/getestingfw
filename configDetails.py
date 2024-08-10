# Databricks notebook source
config = {
    'connection_string':'***********',
    'source_container_name':'raw',
    'target_container_name' :'silver',
    'test_results_container':'test-output',
    "marks_report":
        {   
            'source_blob_name':'marks_report.csv',
            'target_blob_name':'marks_report.csv',
            "table_name": "marks_report",
            "non_nullable_columns": ['dept_id','student_id','gender'],
            "non_duplicate_columns": ['dept_id','student_id','student_name'],
            "selective_matching_validation_columns": ['dept_id','student_id','student_name'],
            "mean_value_validation_columns": [{"column_name":"marks","min_value":80.0,"max_value":90.0}],
            "max_value_validation_columns": [{"column_name":"marks","min_value":80.0,"max_value":90.0}],
            "min_value_validation_columns": [{"column_name":"marks","min_value":80.0,"max_value":90.0}],
            "median_value_validation_columns": [{"column_name":"marks","min_value":70.0,"max_value":95.0}],
            "email_validation_columns": ["marks"]
         }
}
