from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'Deepak',
    'retries': 3,
    'retry_delay':timedelta(minutes=2
    )
}

with DAG(
    dag_id='DAG_with_postgres_v02',
    default_args=default_args,
    start_date=datetime(2023, 4, 24),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id='Create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs_test_v02 (
                dt date, 
                dag_id character varying, 
                primary key (dt, dag_id)
            )
        """
    )

    task2 = PostgresOperator(
        task_id='delete_from_table',
        postgres_conn_id='postgres_localhost',
        sql= """
            delete from dag_runs_test_v02 where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}'
        """
    )

    task3 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql= """
            insert into dag_runs_test_v02 (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )


    task1 >> task2 >> task3
