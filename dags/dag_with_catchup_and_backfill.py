from airflow import DAG 
from datetime import datetime, timedelta

from airflow.operators.bash_operator import BashOperator


default_args = {
	'owner': 'Deepak',
	'retries': 5,
	'retry_delay': timedelta(minutes=2)
}
with DAG (
	dag_id='DAG_with_catchup_backfill_v01',
    default_args=default_args,
    description='This is the first dag',
    start_date = datetime(2023, 4, 20, 2),
    schedule_interval='@daily',
    catchup=True
) as dag:
    
	task1 = BashOperator(
		task_id='FirstTask',
		bash_command='echo Hello World!'
	)

	task1