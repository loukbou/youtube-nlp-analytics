from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from dateutil import parser
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def preprocess_data():
    """Load and clean scraped YouTube data"""

    # Get the latest scraped dataset
    files = [f for f in os.listdir('/opt/airflow/dags/data/') if f.startswith('scraped_data')]
    latest_file = sorted(files)[-1]

    df = pd.read_csv(f"/opt/airflow/dags/data/{latest_file}")

    # Convert numeric fields
    numeric_cols = ['viewCount', 'likeCount', 'commentCount']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # Convert publish date to datetime
    df['publishedAt'] = df['publishedAt'].apply(lambda x: parser.parse(x))
    df['publishDay'] = df['publishedAt'].apply(lambda x: x.strftime("%A"))

    # Save cleaned data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df.to_csv(f"/opt/airflow/dags/data/processed_data_{timestamp}.csv", index=False)

# Airflow DAG setup
default_args = {
    'owner': 'Bouchra',
    'start_date': datetime(2025, 3, 23),
}

dag = DAG(
    'preprocessing_dag',
    default_args=default_args,
    schedule_interval=None,
)

preprocessing_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag,
)

trigger_analysis = TriggerDagRunOperator(
    task_id="trigger_analysis_dag",
    trigger_dag_id="analysis_dag",  # doit matcher le dag_id du DAG suivant
    dag=dag,
)

preprocessing_task >> trigger_analysis
