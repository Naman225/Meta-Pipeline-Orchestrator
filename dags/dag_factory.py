from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Ensure the paths are reachable
sys.path.append("/opt/airflow")

from src.metadata_manager.manager import MetadataManager
meta = MetadataManager("/opt/airflow/config/metadata_config.yaml")

# Mock the function missing from manager for demonstration
def mock_get_all_active_pipelines():
    return [{
        'pipeline_name': 'orders_pipeline',
        'schedule': '@daily'
    }]

def create_dag(pipeline_config):
    """Generates an Airflow DAG from a pipeline metadata config"""
    
    dag = DAG(
        dag_id=pipeline_config['pipeline_name'],
        schedule_interval=pipeline_config['schedule'],
        start_date=datetime(2024, 1, 1),
        catchup=False
    )
    
    def run_task():
        from spark_jobs.pipeline_engine import PipelineEngine
        from pyspark.sql import SparkSession
        
        # Local spark submit setup for the task
        spark = SparkSession.builder \
            .appName(f"Airflow_Task_{pipeline_config['pipeline_name']}") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
            
        engine = PipelineEngine(spark, meta)
        engine.run_pipeline(pipeline_config['pipeline_name'])
    
    with dag:
        task = PythonOperator(
            task_id='run_pipeline',
            python_callable=run_task
        )
    
    return dag

# Auto-generate DAGs for all active pipelines
for config in mock_get_all_active_pipelines():
    dag_id = config['pipeline_name']
    globals()[dag_id] = create_dag(config)