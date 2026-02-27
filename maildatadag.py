from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector

def load_data():
    conn= snowflake.connector.connect(
        user='xxxx',
        password='xxxx',
        account='xxxx',
        warehouse='COMPUTE_WH',
        database='MYDATA',
        schema='RAWDATA'
    )

    cs= conn.cursor()

    try:
        cs.execute("""

        CREATE OR REPLACE TABLE HISTORICAL_MAILS_TABLE(
        
            ACCOUNT_NAME STRING,
            FIRST_NAME STRING,
            LAST_NAME STRING,
            DESIGNATION STRING,
            EMAIL_ID STRING,
            PRACTICE STRING,
            INITIAL STRING,
            INITIAL_STATUS STRING,
            FUP1 STRING,
            FUP1_STATUS STRING,
            FUP2 STRING,
            FUP2_STATUS STRING,
            COMMENTS STRING,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UPDATED_AT TIMESTAMP);""")
        
        cs.execute("""
        
        CREATE OR REPLACE FILE FORMAT CSV_FORMAT
        TYPE=CSV
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1;""")

        cs.execute("""
        
        CREATE OR REPLACE STAGE HISTORICAL_MAILING_S3
        URL='s3://mails-data-bucket/'
        CREDENTIALS=(aws_key_id = 'xxxx'
                aws_secret_key = 'xxxx')
        FILE_FORMAT='CSV_FORMAT';""")

        cs.execute("""
        COPY INTO HISTORICAL_MAILS_TABLE(
        
            ACCOUNT_NAME,
            FIRST_NAME,
            LAST_NAME,
            DESIGNATION,
            EMAIL_ID,
            PRACTICE,
            INITIAL,
            INITIAL_STATUS,
            FUP1,
            FUP1_STATUS,
            FUP2,
            FUP2_STATUS,
            COMMENTS)
        FROM @HISTORICAL_MAILING_S3/historical_mailingdata.csv
        FILE_FORMAT=(FORMAT_NAME=CSV_FORMAT)
        ON_ERROR='CONTINUE';""")

    finally:
        cs.close()
        conn.close()

def transform_data():
    conn=snowflake.connector.connect(
        user='xxxx',
        password='xxxx',
        account='xxxx',
        warehouse='COMPUTE_WH',
        database='MYDATA',
        schema='RAWDATA'
    )

    cs=conn.cursor()

    try:
        cs.execute("""
        CREATE OR REPLACE TABLE HISTORICAL_TRANSFORMED AS
        SELECT
            ACCOUNT_NAME,
            FIRST_NAME,
            LAST_NAME,
            COALESCE(DESIGNATION,'NA') AS TITLE,
            EMAIL_ID,
            PRACTICE,
            TRY_TO_DATE(INITIAL, 'DD-MM-YYYY') AS INITIAL,
            COALESCE(INITIAL_STATUS, 'NO RESPONSE') AS INITIAL_STATUS,
            TRY_TO_DATE(FUP1, 'DD-MM-YYYY') AS FUP1,
            COALESCE(FUP1_STATUS, 'NO RESPONSE') AS FUP1_STATUS,
            TRY_TO_DATE(FUP2, 'DD-MM-YYYY') AS FUP2,
            COALESCE(FUP2_STATUS, 'NO RESPONSE') AS FUP2_STATUS,
            COALESCE(COMMENTS, 'DELIVERED') AS COMMENTS
            FROM MYDATA.RAWDATA.HISTORICAL_MAILS_TABLE;""")
    finally:
        cs.close()
        conn.close()

with DAG(
    dag_id='ETL_S3_AF_SF',
    start_date=datetime(2026,2,25),
    schedule=None,
    catchup=False,
) as dag:

    load_task=PythonOperator(
        task_id='load_raw_mailing_data',
        python_callable=load_data
        )

    transform_task=PythonOperator(
        task_id='transform_loaded_data',
        python_callable=transform_data
    )

    load_task>>transform_task