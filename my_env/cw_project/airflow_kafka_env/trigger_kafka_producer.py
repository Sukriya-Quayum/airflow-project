#author :sukriya
#2nd one in airflow dag in part 2  trigger kafka every 5 min
#triggering of kafka producer script evry 5 min
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with  DAG(
   dag_id= 'kafka_producer_trigger',
    default_args=default_args,
    description='Trigger Kafka producer script every 5 minutes',
    schedule='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2024, 4, 8),
    catchup=False,
    tags=['kafka', 'production'],
) as dag:


    trigger_kafka_producer = BashOperator(
      task_id='trigger_kafka_producer',
      bash_command='python /home/sbq0002/airflow/dags/cw_project/airflow_kafka_env/trigger_kafka_producer.py',
    
)

trigger_kafka_producer

