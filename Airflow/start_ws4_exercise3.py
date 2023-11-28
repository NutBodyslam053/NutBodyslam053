from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

with DAG(
    "exercise3_fan_in_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["exercise"]
) as dag:
    
    t = [DummyOperator(task_id=f"task_{i}") for i in range(7)]


[t[0], t[1], t[2]] >> t[4] >> t[6] << [t[3], t[5]]
    