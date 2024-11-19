import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

# Email configuration for failures
def alert_email_on_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
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

# Function to save logs
def save_log(messages):
    try:
        client = storage.Client()
        log_bucket_name = "us-central1-composer-case-e66c77cc-bucket"
        log_bucket = client.get_bucket(log_bucket_name)
        
        log_blob_name = f'logs/gold_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log'
        log_blob = log_bucket.blob(log_blob_name)
        
        log_content = "\n".join(messages)
        log_blob.upload_from_string(log_content, content_type="text/plain; charset=utf-8")
        
        logging.info(f"Log successfully saved in bucket {log_bucket_name} with name {log_blob_name}")
    except Exception as e:
        logging.error(f"Error saving log to log bucket: {e}")
        raise

# Callback to save logs on failure
def save_log_on_failure(context):
    log_messages = [
        f"Error in DAG: {context['dag'].dag_id}",
        f"Task that failed: {context['task_instance'].task_id}",
        f"Execution date: {context['execution_date']}",
        f"Error message: {context['exception']}",
    ]
    save_log(log_messages)

# Sensor to check Silver success
def check_silver_success(**kwargs):
    signal = kwargs['ti'].xcom_pull(
        dag_id='silver_dag',
        task_ids='load_data_to_bigquery',
        key='success_signal'
    )
    if signal:
        logging.info("Success signal received from Silver DAG.")
        return True
    else:
        logging.info("No success signal received from Silver DAG.")
        return False

# Data transformation for the Gold layer
def transform_to_gold(**kwargs):
    log_messages = ["Starting data transformation for the Gold layer"]
    try:
        bucket_name = "bucket-case-abinbev"
        silver_file_path = "data/silver/breweries_transformed/breweries_transformed.parquet"
        gold_file_path = "data/gold/breweries_aggregated.parquet"
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        
        silver_blob = bucket.blob(silver_file_path)
        silver_data = silver_blob.download_as_bytes()
        
        silver_df = pd.read_parquet(silver_data)
        gold_df = silver_df.groupby(["country", "state", "brewery_type"]).size().reset_index(name="total_breweries")
        
        gold_df.to_parquet(gold_file_path, index=False)
        gold_blob = bucket.blob(gold_file_path)
        gold_blob.upload_from_filename(gold_file_path)
        
        log_messages.append("Transformation and saving to GCS completed successfully.")
    except Exception as e:
        log_messages.append(f"Error during transformation for the Gold layer: {e}")
        logging.error(f"Error: {e}")
        raise
    save_log(log_messages)

# Load data into BigQuery
def load_gold_to_bigquery_from_parquet(**kwargs):
    log_messages = ["Starting loading Gold layer data into BigQuery"]
    try:
        bucket_name = "bucket-case-abinbev"
        gold_file_path = "data/gold/breweries_aggregated.parquet"
        
        client = bigquery.Client()
        table_id = "case-abinbev.Medallion.gold"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        uri = f"gs://{bucket_name}/{gold_file_path}"
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        
        log_messages.append(f"Data successfully loaded to table {table_id}.")
    except Exception as e:
        log_messages.append(f"Error loading data into BigQuery: {e}")
        logging.error(f"Error: {e}")
        raise
    save_log(log_messages)

# Gold DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': save_log_on_failure,
}

with DAG(
    'gold_dag',
    default_args=default_args,
    description='DAG to transform data from the Silver layer and load it into the Gold layer in BigQuery',
    schedule_interval=None,  # Triggered only by Silver
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Silver success validation
    wait_for_silver_success = PythonSensor(
        task_id="wait_for_silver_success",
        python_callable=check_silver_success,
        poke_interval=30,
        timeout=150,  # Limits poking to 5 attempts (150 seconds total)
        mode="poke"
    )
    
    # Transformation to Gold
    transform_to_gold_task = PythonOperator(
        task_id="transform_to_gold",
        python_callable=transform_to_gold,
    )
    
    # Loading into BigQuery
    load_gold_to_bigquery_task = PythonOperator(
        task_id="load_gold_to_bigquery_from_parquet",
        python_callable=load_gold_to_bigquery_from_parquet,
    )
    
    # DAG flow
    wait_for_silver_success >> transform_to_gold_task >> load_gold_to_bigquery_task
    
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