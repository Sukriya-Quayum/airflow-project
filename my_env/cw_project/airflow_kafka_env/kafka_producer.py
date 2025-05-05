#author :sukriya
# trigger kafka every 5 min
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    'kafka_producer_trigger',
    default_args=default_args,
    description='Trigger Kafka producer script every 5 minutes',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2025, 4, 8),
    catchup=False,
)
trigger_kafka_producer = BashOperator(
    task_id='trigger_kafka_producer',
    bash_command='python /home/sbq0002/airflow/dags/cw_project/airflow_kafka_env/kafka_producer_script.py',
    dag=dag,
)

trigger_kafka_producer

