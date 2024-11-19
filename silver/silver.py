import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

# Authentication and email configuration for failures
def alert_email_on_failure(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    email = "jaquerando@gmail.com"

    subject = f"Failure Alert - DAG: {dag_id}, Task: {task_id}"
    body = f"""
    <h3>DAG Failure Alert</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Failed Task:</strong> {task_id}</p>
    <p><strong>Execution Date:</strong> {execution_date}</p>
    <p><strong>Log URL:</strong> <a href="{log_url}">{log_url}</a></p>
    <p>Check the log for failure details.</p>
    """
    send_email(to=email, subject=subject, html_content=body)

# Sensor to check if the file was updated in Bronze
def check_file_updated(**kwargs):
    updated = kwargs['ti'].xcom_pull(
        dag_id='bronze_dag',  # Name of the Bronze DAG
        task_ids='fetch_data_and_compare',  # Task that defines the XCom
        key='file_updated'
    )
    
    # Checks if the returned value is valid
    if updated is None:
        logging.warning("No value found in XCom. Aborting execution.")
        return False
    
    if updated:
        logging.info("Signal received in XCom to run the Silver DAG: Running.")
        return True
    else:
        logging.info("Signal received in XCom to NOT run the Silver DAG: Aborting.")
        return False

# Function to download data from GCS
def download_data(**kwargs):
    log_messages = ["Starting download of data from the Bronze layer"]
    try:
        bucket_name = "bucket-case-abinbev"
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob("data/bronze/breweries_raw.ndjson")
        raw_data = blob.download_as_text()
        kwargs['ti'].xcom_push(key="raw_data", value=raw_data)
        log_messages.append("Download completed successfully.")
    except Exception as e:
        log_messages.append(f"Error downloading data: {e}")
        logging.error(f"Error: {e}")
        raise
    save_log(log_messages)

# Function to transform the data
def transform_data(**kwargs):
    log_messages = ["Starting data transformation"]
    try:
        raw_data = kwargs['ti'].xcom_pull(key="raw_data", task_ids="download_data")
        raw_df = pd.read_json(raw_data, lines=True)
        raw_df["id"] = raw_df["id"].astype(str).str.strip()
        raw_df["name"] = raw_df["name"].astype(str).str.title()
        raw_df["brewery_type"] = raw_df["brewery_type"].fillna("unknown").astype(str)
        raw_df["state_partition"] = raw_df["state"].apply(lambda x: hash(x) % 50)
        kwargs['ti'].xcom_push(key="transformed_data", value=raw_df.to_json(orient='records', lines=True))
        log_messages.append("Transformation completed successfully.")
    except Exception as e:
        log_messages.append(f"Error transforming data: {e}")
        logging.error(f"Error: {e}")
        raise
    save_log(log_messages)

# Function to load data into BigQuery
def load_data_to_bigquery(**kwargs):
    log_messages = ["Starting data loading to BigQuery"]
    try:
        transformed_data = kwargs['ti'].xcom_pull(key="transformed_data", task_ids="transform_data")
        transformed_df = pd.read_json(transformed_data, lines=True)
        project_id = "case-abinbev"
        dataset_id = "Medallion"
        table_id = "silver"
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_dataframe(transformed_df,   
 table_ref, job_config=job_config)
        job.result()
        log_messages.append(f"Data successfully loaded into table {dataset_id}.{table_id} in BigQuery.")
    except Exception as e:
        log_messages.append(f"Error loading data into BigQuery: {e}")
        logging.error(f"Error: {e}")
        raise
    save_log(log_messages)

# Function to save logs
def save_log(messages):
    client = storage.Client()
    log_bucket = client.get_bucket("us-central1-composer-case-e66c77cc-bucket")
    log_blob = log_bucket.blob(f'logs/silver_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    log_blob.upload_from_string("\n".join(messages), content_type="text/plain")
    logging.info("Log saved to log bucket.")

# Silver DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert_email_on_failure
}

with DAG(
    'silver_dag',
    default_args=default_args,
    description='DAG to transform data from the Bronze layer and load it into the Silver layer in BigQuery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Sensor to check if the Bronze DAG detected an update
    wait_for_file_update = PythonSensor(
        task_id='wait_for_file_update',
        python_callable=check_file_updated,
        poke_interval=30,
        timeout=600,  # Maximum waiting time
        mode='poke',
    )
    
    # Download task
    download_data_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )
    
    # Transformation task
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    # Loading task
    load_data_task = PythonOperator(
        task_id='load_data_to_bigquery',
        python_callable=load_data_to_bigquery,
    )

    # Defining the execution sequence
    wait_for_file_update >> download_data_task >> transform_data_task >> load_data_task
    

# Server setup
app = FastAPI()

class StatusResponse(BaseModel):
    status: str

@app.get("/")
def read_root():
    return {"message": "Bronze DAG server is running."}

@app.get("/status", response_model=StatusResponse)
def status():
    return {"status": "ok"}    