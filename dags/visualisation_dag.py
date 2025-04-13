from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# This DAG doesn't run any analysis.
# It's just a reminder of the Streamlit dashboard URL and port (http://localhost:8501).

default_args = {
    'owner': 'Bouchra',
    'start_date': datetime(2025, 4, 8),
}

def log_dashboard_url():
    print("----Dashboard de visualisation disponible sur : http://localhost:8501")
    print("Assurez-vous que le conteneur Streamlit est lancé avec :")
    print("docker run -p 8501:8501 -v $(pwd)/data:/data streamlit-dashboard")

with DAG('visualisation_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         description="DAG pour visualiser les résultats des analyses") as dag:

    show_dashboard_task = PythonOperator(
        task_id='afficher_dashboard',
        python_callable=log_dashboard_url
    )
