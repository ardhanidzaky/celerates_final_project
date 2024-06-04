from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

query_dm1 = """
    CREATE TABLE IF NOT EXISTS data_mart1 AS
    SELECT
        DATE(date)
        , category
        , COUNT(*) AS request_count
    FROM transactional
    GROUP BY DATE(date), category;
"""

query_dm2 = """
    CREATE TABLE IF NOT EXISTS data_mart2 AS
    SELECT
        DATE(date)
        , status
        , COUNT(*) AS request_count
    FROM transactional
    GROUP BY DATE(date), status;
"""

query_dm3 = """
    CREATE TABLE IF NOT EXISTS data_mart3 AS
    SELECT
        DATE(date)
        , AVG(CASE WHEN status = 'CLOSED' THEN EXTRACT(EPOCH FROM (closed_date - date)) ELSE 0 END) / 60 AS avg_processing_time
    FROM transactional
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
    'create_data_mart',
    default_args=default_args,
    description='A DAG to create data marts from the transactional table',
    schedule_interval=None,
)

# Define operators to execute SQL queries
create_data_mart1 = PostgresOperator(
    task_id='create_data_mart1',
    postgres_conn_id='supabase',
    sql=query_dm1,
    dag=dag,
)

create_data_mart2 = PostgresOperator(
    task_id='create_data_mart2',
    postgres_conn_id='supabase',
    sql=query_dm2,
    dag=dag,
)

create_data_mart3 = PostgresOperator(
    task_id='create_data_mart3',
    postgres_conn_id='supabase',
    sql=query_dm3,
    dag=dag,
)

create_data_mart1 >> create_data_mart2 >> create_data_mart3