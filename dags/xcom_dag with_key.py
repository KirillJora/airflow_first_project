from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG('xcom_dag_with_key', start_date = datetime(2022, 1,1), schedule = '@daily', catchup = False) as dag:

    @task
    def peter_task(ti = None):
        #below command performs pushing the data into xcom DB as a dictionary with key & value suggested
        ti.xcom_push(key = 'mobile_phone', value = 'iphone')
        return 'iphone'

    @task
    def lorie_task(ti = None):
        ti.xcom_push(key = 'mobile_phone', value = 'galaxy')

    @task
    def bryan_task(ti=None):
        #below command performs pulling the data from xcom DB and assigns derived value to the phone
        phone = ti.xcom_pull(task_ids=['peter_task', 'lorie_task'], key='mobile_phone')
        print(phone)

#specify order of operations

    peter_task() >> lorie_task() >> bryan_task()