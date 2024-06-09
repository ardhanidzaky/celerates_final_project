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
    'create_category_dm',
    default_args=default_args,
    description='A DAG to create Category data mart using PySpark',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

create_category_dm_dag = SparkSubmitOperator(
    task_id='create_category_dm_dag'
    , application='/Users/ardhanidzaky/airflow/dags/category_dm_spark.py'
    , name='category_data_mart'
    , conn_id='spark_default'
    , dag=dag
)

create_category_dm_dag
