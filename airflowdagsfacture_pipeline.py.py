from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    'facture_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:

    run_producer = PythonOperator(
        task_id='run_kafka_producer',
        python_callable=lambda: exec(open('/opt/airflow/dags/kafka_producer.py').read())
    
    run_spark = BashOperator(
        task_id='run_spark_processor',
        bash_command='spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /app/spark_processor.py'
    )

    run_producer >> run_spark