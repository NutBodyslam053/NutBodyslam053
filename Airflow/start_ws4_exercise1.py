import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'datath',
}

def my_function(something: str):
    print(something)

with DAG(
    "exercise1_simple_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["exercise"]
) as dag:

    t1 = PythonOperator(
        task_id="print_hello",
        python_callable=my_function,
        op_kwargs={"something": "Hello World!"},
    )

    t2 = BashOperator(
        task_id="print_date",
        bash_command="echo $(date)",
    )

t1 >> t2
    
