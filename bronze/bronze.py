import logging
import json
import requests
import hashlib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import storage
from fastapi import FastAPI
from pydantic import BaseModel

# Configuration for sending email in case of failure
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

# Configuration and log variables
url = "https://api.openbrewerydb.org/breweries"
bucket_name = 'bucket-case-abinbev'
data_blob_name = 'data/bronze/breweries_raw.ndjson'
control_blob_name = 'data/bronze/last_update.txt'
log_bucket_name = 'us-central1-composer-case-e66c77cc-bucket'
log_messages = []

def log_message(message):
    log_messages.append(f"\n{message}\n{'-'*40}")

def get_last_update_from_control_file():
    """Reads the control file to get the hash or timestamp of the last update."""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    control_blob = bucket.blob(control_blob_name)
    
    if not control_blob.exists():
        log_message("Control file not found. Considering as first run.")
        return None

    last_update = control_blob.download_as_text().strip()
    log_message(f"Last hash/timestamp found in control file: {last_update}")
    return last_update

def save_to_control_file(content):
    """Updates the control file with the new hash/timestamp."""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    control_blob = bucket.blob(control_blob_name)
    control_blob.upload_from_string(content)
    log_message(f"Control file updated with: {content}")

def fetch_data_and_compare():
    """Downloads the data and compares it with the last stored hash/timestamp."""
    log_message("Downloading data for verification.")
    response = requests.get(url)
    response.raise_for_status()
    breweries = response.json()
    
    # Sorts the data and converts to string before calculating the hash
    breweries_sorted_str = json.dumps(breweries, sort_keys=True)
    current_hash = hashlib.md5(breweries_sorted_str.encode('utf-8')).hexdigest()
    log_message(f"Hash of current downloaded data: {current_hash}")

    # Gets the last hash/timestamp from the control file
    last_update = get_last_update_from_control_file()

    if last_update == current_hash:
        log_message("No updates detected when comparing with the control file.")
        save_log()
        return False  # Data unchanged

    # If there is a difference, saves the new data to GCS and updates the control
    save_data_to_gcs(breweries_sorted_str, current_hash)
    save_to_control_file(current_hash)
    return True  # Data updated

def save_data_to_gcs(data, current_hash):
    """Uploads the data to GCS."""
    log_message("Update detected. Uploading data to GCS.")
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(data_blob_name)

    # Formats data to NDJSON
    breweries_list = json.loads(data)
    formatted_data = "\n".join([json.dumps(record) for record in breweries_list])
    blob.upload_from_string(formatted_data, content_type='application/x-ndjson')

    log_message(f"Destination bucket: {bucket_name}, File path: {data_blob_name}")
    log_message(f"New hash stored in control: {current_hash}")
    save_log()

def save_log():
    """Saves log messages to log bucket with utf-8 charset."""
    client = storage.Client()
    log_bucket = client.get_bucket(log_bucket_name)
    log_blob = log_bucket.blob(f'logs/bronze_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    
    # Sets content with UTF-8 charset
    log_blob.upload_from_string("\n".join(log_messages), content_type="text/plain; charset=utf-8")
    
    logging.info("Log saved to log bucket with UTF-8 charset.")


# Defining default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert_email_on_failure
}

# Defining the Bronze DAG
with DAG(
    'bronze_dag',
    default_args=default_args,
    description='DAG to check for updates and consume data from the Open Brewery DB API',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_data_and_compare',
        python_callable=fetch_data_and_compare
    )

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
