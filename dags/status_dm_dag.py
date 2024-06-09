from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow'
    , 'depends_on_past': False
    , 'email': ['ardhanidzakyy@gmail.com']
    , 'email_on_failure': False
    , 'email_on_retry': False
    , 'retries': 1
}

dag = DAG(
    'create_status_dm',
    default_args=default_args,
    description='A DAG to create Status data mart using PySpark',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

create_status_dm_dag = SparkSubmitOperator(
    task_id='create_status_dm_dag'
    , application='/Users/ardhanidzaky/airflow/dags/status_dm_spark.py'
    , name='status_data_mart'
    , conn_id='spark_default'
    , dag=dag
)

create_status_dm_dag
