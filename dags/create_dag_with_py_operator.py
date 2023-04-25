from datetime import datetime, timedelta 

from airflow import DAG 
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Deepak',
    'retries': 5, 
    'retry_delay':timedelta(minutes=5)
}

def greet(ti):
    """
        This function simply greets the users.
    """
    print("Hello World! This is a Python operator DAG")
    first_name=ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name=ti.xcom_pull(task_ids='get_name', key='last_name')
    age=ti.xcom_pull(task_ids='get_age', key='age')
    print(f"I am your host, my name is {first_name} {last_name} and age is {age}")


def get_age(ti):
    """
        This is xcom for age. 
    """
    ti.xcom_push(key='age', value=19)

def get_name(ti):
    """
        Setting up the xcoms here.
    """
    ti.xcom_push(key='first_name', value='Deepak')
    ti.xcom_push(key='last_name', value='Pandey')


with DAG(

    dag_id='First_Python_DAG_v05',
    description='This is first dag using python',
    start_date=datetime(2022, 4, 24, 2),
    schedule_interval='@monthly'
) as dag:
    task1 = PythonOperator(
        task_id='Greet',
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    task2 >> task1
    task3 >> task1
