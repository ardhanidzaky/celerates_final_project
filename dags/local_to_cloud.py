from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import io

def transfer_data():
    src_hook = PostgresHook(postgres_conn_id='ardhani_postgres')
    src_conn = src_hook.get_conn()
    src_cursor = src_conn.cursor()
    
    dest_hook = PostgresHook(postgres_conn_id='supabase')
    dest_conn = dest_hook.get_conn()
    dest_cursor = dest_conn.cursor()

    query = "SELECT * FROM final_project.day_0;"
    src_cursor.execute(query)
    data = src_cursor.fetchall()
    columns = [desc[0] for desc in src_cursor.description]

    df = pd.DataFrame(data, columns=columns)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    dest_cursor.execute("""
        DROP TABLE IF EXISTS temp_table;
    """)
    dest_conn.commit()

    dest_cursor.execute("""
        CREATE TABLE temp_table AS SELECT * FROM transactional LIMIT 0;
    """)
    dest_conn.commit()
    
    dest_cursor.copy_expert("""
        COPY temp_table FROM STDIN WITH CSV HEADER;
    """, csv_buffer)
    dest_conn.commit()

    dest_cursor.execute("""
        INSERT INTO transactional (SELECT * FROM temp_table);
    """)
    dest_conn.commit()

    # Close connections
    src_cursor.close()
    src_conn.close()
    dest_cursor.close()
    dest_conn.close()

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
    'transfer_local_to_cloud'
    , default_args=default_args
    , description='A DAG to transfer data from local PostgreSQL to cloud PostgreSQL'
    , catchup=False
)

# Define the task
transfer_task = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data,
    dag=dag,
)

# Set task dependencies
transfer_task
