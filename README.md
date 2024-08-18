# GE-TESTING-FRAMEWORK

### What is it?
GE-TESTING-FRAMWORK is a data quality management asset. It is a custom built framework of reusable automation python/pyspark scripts integrated with Great Expectation library. The framework is integrated wih Azure Data Factory and datbricks to validate data transformation on ETL pipelines

### Why?
The framework can be utilized for automating quality checks ensuring data completeness, detecting duplicates, count validations and enforcing unique key constraints, reducing manual errors and developmental delays by ensuring only validated and clean data is ingested

### What is Great Expectations testing tool?
Great Expectations is an open-source Python library for validating and documenting data. It helps data teams ensure data quality by defining expectations for data and automatically checking if those expectations are met.

Example:
expect_column_values_to_not_be_null
<img width="655" alt="image" src="https://github.com/user-attachments/assets/055c77b4-047b-4b81-b2b4-410fd40e3099">
Here, we call a the Great Expectations functionality expect_column_values_to_not_be_null in the pythin function validate nulls, and pass the columns for which we want the test to be checked, it generates the result in a json format, which is finally converted to a table for clear represntation

### Great Expectations Implemented
- expect_column_values_to_not_be_null
- expect_columns_to_be_unique
- expect_table_row_count_to_equal
- expect_column_mean_to_be_between
- expect_column_median_to_be_between
- expect_column_min_to_be_between
- expect_column_max_to_be_between
- expect_column_to_exist
- expect_column_values_to_be_of_type

### ADF pipeline Integration with the framework
<img width="821" alt="image" src="https://github.com/user-attachments/assets/5af8e4fc-ebf6-4ae5-8926-b5f70d95d459">
Here, after the copy data activity, a databricks activity is executed which contains the testing framework

### Result Format
<img width="1029" alt="image" src="https://github.com/user-attachments/assets/6357b7e4-5b0f-4847-a487-29d3631ccb85">







