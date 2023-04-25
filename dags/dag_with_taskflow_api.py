from datetime import datetime, timedelta

from airflow.decorators import dag, task


default_args = { 
    'owner':'Deepak',
    'retries': 5, 
    'retry_delay': timedelta(minutes=1)
}

@dag(dag_id='dag_with_taskflow', 
     default_args=default_args,
     start_date=datetime(2023, 1, 20),
     schedule_interval='@monthly'
     )
def hello_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name':'Deepak',
            'last_name':'Pandey'
        }
    
    @task()
    def get_age():
        return 19
    
    @task()
    def greet(first_name, last_name, age):
        print(f"Hello, my name is {first_name} {last_name} and age is {age}")
    
    name_dict=get_name()
    age=get_age()

    greet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
          age=age)

dag=hello_world_etl()