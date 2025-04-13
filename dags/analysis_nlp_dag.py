from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from transformers import pipeline
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging

def analyze_data():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        # Find the latest processed file
        data_dir = '/opt/airflow/dags/data/'
        files = [f for f in os.listdir(data_dir) if f.startswith('processed_data')]
        if not files:
            logger.error("No processed data files found")
            raise FileNotFoundError("No processed data files available")
            
        latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(data_dir, f)))
        logger.info(f"Processing file: {latest_file}")
        
        # Load data
        df = pd.read_csv(os.path.join(data_dir, latest_file))
        
        # Initialize models
        try:
            labels = ["education", "humour", "tech", "voyage", "musique", "actualité", "divertissement"]
            classifier = pipeline(
                "zero-shot-classification", 
                model="facebook/bart-large-mnli"
            )
            sentiment_analyzer = pipeline(
                "sentiment-analysis",
                model="distilbert-base-uncased-finetuned-sst-2-english"
            )
        except Exception as e:
            logger.error(f"Model loading failed: {str(e)}")
            raise

        # Process data in batches for efficiency
        batch_size = 32
        results = []
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size].copy()
            try:
                batch["topic"] = batch["title"].apply(
                    lambda t: classifier(t, labels, multi_label=False)["labels"][0]
                )
                batch["sentiment"] = batch["title"].apply(
                    lambda t: sentiment_analyzer(t)[0]["label"]
                )
                batch["analysis_date"] = datetime.now().date()
                results.append(batch)
            except Exception as e:
                logger.warning(f"Error processing batch {i//batch_size}: {str(e)}")
                continue

        # Combine results and save
        result_df = pd.concat(results)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(data_dir, f"analysis_results_{ts}.csv")
        result_df.to_csv(output_path, index=False)
        logger.info(f"Analysis saved to {output_path}")
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

default_args = {
    'owner': 'Bouchra',
    'start_date': datetime(2025, 4, 8),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: logging.error("DAG failed", exc_info=context['exception']),
}

with DAG(
    'analysis_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    analyze_task = PythonOperator(
        task_id='analyze_video_data',
        python_callable=analyze_data,
        execution_timeout=timedelta(minutes=30)
    )
    
    trigger_visualization = TriggerDagRunOperator(
        task_id='trigger_visualization',
        trigger_dag_id='visualisation_dag',
        wait_for_completion=False
    )

    analyze_task >> trigger_visualization