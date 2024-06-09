from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, date_format
import os

def main():
    os.environ['PYSPARK_PYTHON'] = '/Users/ardhanidzaky/Documents/celerates/cele_env/bin/python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/ardhanidzaky/Documents/celerates/cele_env/bin/python'

    src_hook = PostgresHook(postgres_conn_id='supabase')
    src_conn = src_hook.get_conn()
    src_cursor = src_conn.cursor()

    current_date = datetime.date.today().strftime('%Y-%m-%d')
    query = f"""
        SELECT  *
        FROM    transactional
        WHERE   pt_date = '{current_date}'
                AND date(date) = '{current_date}';
    """
    src_cursor.execute(query)
    data = src_cursor.fetchall()
    columns = [desc[0] for desc in src_cursor.description]
    df = pd.DataFrame(data, columns=columns)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Create Status Data Mart") \
        .getOrCreate()
    transactional_df = spark.createDataFrame(df)

    # Perform transformation
    data_mart2_df = transactional_df \
        .groupBy(date_format(col("date"), "yyyy-MM-dd").alias("date"), "status") \
        .agg(count("*").alias("request_count"))
    
    # Convert Spark DataFrame to Pandas DataFrame
    result_df = data_mart2_df.toPandas()

    # Connect to PostgreSQL using PostgresHook for writing
    dest_hook = PostgresHook(postgres_conn_id='supabase')
    dest_conn = dest_hook.get_conn()
    dest_cursor = dest_conn.cursor()

    # Append data to the target table in PostgreSQL
    for _, row in result_df.iterrows():
        insert_query = """
            INSERT INTO status_pd (date, status, request_count)
            VALUES (%s, %s, %s);
        """
        dest_cursor.execute(insert_query, (row['date'], row['status'], row['request_count']))

    dest_conn.commit()
    dest_cursor.close()
    dest_conn.close()
    src_cursor.close()
    src_conn.close()

    spark.stop()

if __name__ == "__main__":
    main()
