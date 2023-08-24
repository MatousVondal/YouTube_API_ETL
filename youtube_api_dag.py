from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from youtube_statistics import YTStatsAnalyzer
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging
from airflow.utils.email import send_email

def extract_and_transform():
    """
    Extract and transform YouTube video statistics using the YTStatsAnalyzer.

    Returns:
        pandas.DataFrame: DataFrame containing analyzed YouTube video statistics.
    """
    # YouTube API key and number of results to fetch
    api_key = "YOUR_YOUTUBE_API_KEY"
    results = 50
    
    # Create an instance of YTStatsAnalyzer to extract and analyze data
    analyzer = YTStatsAnalyzer(api_key, results)
    
    # Call the analyze_statistics method to get DataFrame of YouTube video statistics
    df = analyzer.analyze_statistics()

    return df

def validate_data(**kwargs):
    try:
        # Fetch the DataFrame from the previous task
        data = kwargs['task_instance'].xcom_pull(task_ids='extract_and_transform_task')
        
        # Check if the data is a non-empty DataFrame
        if not isinstance(data, pd.DataFrame) or data.empty:
            raise ValueError("Data is empty or not in the expected format.")
        
        # Data Validation: Check for missing values in specific columns
        if data['video_id'].isnull().any():
            raise ValueError("Data contains missing values in the 'video_id' column.")
        
        if data['channel_id'].isnull().any():
            raise ValueError("Data contains missing values in the 'channel_id' column.")
        
        # Define expected data types for each column
        expected_data_types = {
            "video_id": str,
            "video_title": str,
            "channel_id": str,
            "channel_title": str,
            "date_of_published": pd.Timestamp,
            "most_frequent_word": str,
            "category": str,
            "view_count": int,
            "like_count": int,
            "view_count_ratio": float,
            "like_count_ratio": float,
            "comment_count": int,
            "duration_sec": int,
            "categorize_duration": str,
            "channel_view_count": int,
            "channel_sub_count": int,
            "channel_video_count": int,
            "watch_video": str,
            "picture": str,
            "language": str
        }
        
        # Validate data types of each column
        for column, expected_type in expected_data_types.items():
            if column not in data.columns or not all(isinstance(val, expected_type) for val in data[column]):
                raise ValueError(f"Data type validation failed for column: {column}")
        
        logging.info("Data type validation checks passed.")
        
    except Exception as e:
        # Error Handling and Logging
        error_message = f"Data validation error: {str(e)}"
        logging.error(error_message)
        raise

def export_to_bigquery(**kwargs):
    # Fetch the DataFrame from the previous task
    data = kwargs['task_instance'].xcom_pull(task_ids='extract_and_transform_task')
    
    # Generate a unique filename for the BigQuery table
    now = datetime.now()
    current_time = now.strftime("%d%m%Y%H%M%S")
    filename = "YouTube_stats_" + current_time
    
    # Define credentials and project details for BigQuery
    key_path = 'PATH_TO_YOUR_JSON_CREDENTIALS_FILE'
    project_id = "YOUR_PROJECT_ID"
    dataset_id = "YOUR_DATASET_ID"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=project_id)
    
    # Construct the table ID using the generated filename
    table_id = f"{project_id}.{dataset_id}.{filename}"
    
    # Configure the BigQuery job settings
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    # Load the DataFrame into the specified BigQuery table
    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
    job.result()

def send_email_on_failure(context):
    task_instance = context['task_instance']
    subject = f"Airflow Alert: Task {task_instance.task_id} Failed"
    body = (
        f"Task execution failed. DAG ID: {context['dag_run'].dag_id}\n"
        f"Task ID: {task_instance.task_id}\n"
        f"Execution Time: {context['execution_date']}\n"
        f"Log URL: {task_instance.log_url}"
    )
    send_email(['YOUR_EMAIL_ADDRESS'], subject, body)

def send_email_on_success(context):
    dag_run = context['dag_run']
    subject = f"Airflow Alert: DAG {dag_run.dag_id} Succeeded"
    body = (
        f"DAG run succeeded. DAG ID: {dag_run.dag_id}\n"
        f"Execution Time: {context['execution_date']}\n"
        f"Log URL: {dag_run.log_url}"
    )
    send_email(['YOUR_EMAIL_ADDRESS'], subject, body)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 8, 6, 0),  
    'email': ['YOUR_EMAIL_ADDRESS'],
    'email_on_failure': True,  # Enable email notification on task failure
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': send_email_on_failure  # Define the failure callback function
}

# Create a DAG instance
with DAG('youtube_api_dag',
        default_args=default_args,
        schedule_interval='0 6,12,18 * * *',  # Triggers at 6:00 AM, 12:00 PM, and 6:00 PM
        catchup=False,
        on_success_callback=send_email_on_success,
        on_failure_callback=send_email_on_failure) as dag:

    # Define tasks in the DAG
    extract_and_transform_task = PythonOperator(
        task_id='extract_and_transform_task',
        python_callable=extract_and_transform
    )

    data_validation_task = PythonOperator(
        task_id='data_validation_task',
        python_callable=validate_data,
        provide_context=True
    )

    export_to_bigquery_task = PythonOperator(
        task_id='export_to_bigquery_task',
        python_callable=export_to_bigquery,
        provide_context=True
    )

    # Define task dependencies
    extract_and_transform_task >> data_validation_task >> export_to_bigquery_task
            
