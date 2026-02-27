# s3_sirflow_sf
This project is to build a ETL pipeline (S3--->Airflow--->Snowflake)

File type is a .csv file stored in S3 bucket, file contains 7000+ rows of structured, but unclean data, with nulls existing and unformatted colums.

The DAG code's functionality primary has 2 tasks, first task being to load the file by establishing connection from S3 to snowflake using IAM roles and policies attached, subsequently creating file format, a table and stage to hold the connection to S3 bucket, finally loading the data to Snowflake using copy into command.

Second task is to perform light transformations on the raw table and loading the transformed table back to Snowflake separately. Transformation made: Standardised and formatted data, replaced null values.

The DAG code imports python connector and snowflake connector.

Dependencies: load_task>>transform_task