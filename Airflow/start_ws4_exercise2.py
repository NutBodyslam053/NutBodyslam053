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
    "exercise2_fan_out_dag",
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

    t3 = BashOperator(
        task_id="list_file_gcs",
        bash_command="gsutil ls gs://asia-east2-nutbodyslam053-r-71cde460-bucket/dags"
    )

    
t1 >> [t2, t3]
    
