import pandas as pd
from airflow import DAG
from airflow.standard.providers.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract(csv_path):
    raw_data = pd.read_csv(csv_path)
    return raw_data

def transform(raw_data):
    raw_data = raw_data[['country', 'description', 'points', 'price', 'region_1', 'region_2']]
    raw_data.fillna({'price': round(raw_data['price'].mean(), 2), 
                     'region_1' : 'missing', 
                     'region_2' : 'missing'}, inplace = True)
    raw_data['points'].astype(int)
    raw_data.set_index('country')
    raw_data = raw_data.head(100)
    return raw_data

def load(clean_data, clean_data_file_path):
     clean_data.to_csv(clean_data_file_path, index = False)

def etl_pipeline(csv_path, clean_data_file_path):
    raw_data = extract(csv_path)
    processed_data = transform(raw_data)
    load(processed_data, clean_data_file_path)
    return {f"ETL completed, Data saved to {clean_data_file_path}"}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    dag_id="wineetl_pipeline",
    default_args=default_args,
    description="A simple ETL pipeline",
    schedule_interval="@daily", 
    catchup=False, 
)
# ===== Define Tasks =====
extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load,
    dag=dag,
)

# ===== Set Task Dependencies =====
extract_task >> transform_task >> load_task