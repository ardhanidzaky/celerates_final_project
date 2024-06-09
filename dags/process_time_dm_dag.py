from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import datetime

current_date = datetime.date.today().strftime('%Y-%m-%d')
query_dm3 = f"""
    INSERT INTO process_time_pd (date, avg_processing_time)
    SELECT  DATE(date)
            , AVG(CASE WHEN status = 'CLOSED' THEN EXTRACT(EPOCH FROM (closed_date - date)) ELSE 0 END) / 60 AS avg_processing_time
    FROM transactional
    WHERE   DATE(date) = '{current_date}'
            AND DATE(pt_date) = '{current_date}'
    GROUP BY DATE(date);
"""

# Default arguments for the DAG
default_args = {
    'owner': 'airflow'
    , 'depends_on_past': False
    , 'email': ['ardhanidzakyy@gmail.com']
    , 'email_on_failure': False
    , 'email_on_retry': False
    , 'retries': 1
}

dag = DAG(
    'create_process_time_dm',
    default_args=default_args,
    description='A DAG to create Process Time data mart using PySpark',
    schedule_interval=None,
)

create_process_time_dm_dag = PostgresOperator(
    task_id='create_process_time_dm_dag',
    postgres_conn_id='supabase',
    sql=query_dm3,
    dag=dag,
)

create_process_time_dm_dag
