import os
from pendulum import datetime, duration, now
from utils.notifications_v2 import send_teams_notification
from utils.utils import generate_gcs_base_uris, generate_bq_table_paths

from airflow.utils.helpers import chain
from airflow.decorators import dag, task_group
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# =================================================<< BEGIN >>================================================= #
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
PROJECT_ID = "envilink"
# ==============================================<< Airflow DAG >>============================================== #
LOCAL_TIMEZONE = "Asia/Bangkok"
DEFAULT_ARGS = {
    "owner": "Nut",
    "on_failure_callback": send_teams_notification,
}
# =============================================<< Cloud Storage >>============================================= #
GCS_BASE_URIS = generate_gcs_base_uris(
    environment=ENVIRONMENT,
    project_id=PROJECT_ID,
    data_zones=["public"],
)
# ===============================================<< BiqQuery >>================================================ #
BQ_TABLE_PATHS = generate_bq_table_paths(
    environment=ENVIRONMENT,
    data_category="pm25",
    table_types=["wh", "mart"],
)
# ==================================================<< END >>================================================== #


@dag(
    schedule_interval="30 * * * *",
    start_date=datetime(2024, 4, 2, tz=LOCAL_TIMEZONE),
    catchup=False,
    tags=["PM25", "API"],
    dagrun_timeout=duration(minutes=15),
    template_searchpath=["/opt/airflow/dags/utils/"],
    default_args=DEFAULT_ARGS,
)
def bq_export_csv_to_gcs() -> None:

    # Waiting for all parent dags completed successfully
    @task_group(group_id="external_task_sensor")
    def external_task_sensor(parent_dags: list[str]) -> list[ExternalTaskSensor]:
        return [
            ExternalTaskSensor(
                task_id=f"waiting_for_{parent_dag}",
                external_dag_id=parent_dag,
                allowed_states=["success", "failed"],
                mode="poke",
                timeout=600,  # 10 minutes
                poke_interval=60,  # 1 minute
                soft_fail=False,
                check_existence=True,
            )
            for parent_dag in parent_dags
        ]

    sensor = external_task_sensor(parent_dags=["pcd_air4thai", "ccdc_dustboy", "dpm_dpmalert", "nt_rguard"])

    # Transfer data table from BigQuery ["warehouse"] to ["mart"]
    @task_group(group_id="data_mart")
    def data_mart(source_tables: list[str], target_tables: list[str]) -> list[BigQueryInsertJobOperator]:
        operators = [
            BigQueryInsertJobOperator(
                task_id=f"create_table_mart__{target_table}",
                gcp_conn_id="gcp",
                project_id=PROJECT_ID,
                configuration={
                    "jobType": "QUERY",
                    "query": {
                        "query": f"{{% include 'sql/create_table_mart__{target_table}.sql' %}}",
                        "useLegacySql": False,
                    },
                    "jobTimeoutMs": (now().int_timestamp + 120) * 1000,  # 2 minutes
                },
                params={
                    "source_table": BQ_TABLE_PATHS["mart"].get(source_table),
                    "target_table": BQ_TABLE_PATHS["mart"].get(target_table),
                    "station_info_table": BQ_TABLE_PATHS["wh"].get("station_info"),
                    "station_reads_table": BQ_TABLE_PATHS["wh"].get("station_reads"),
                },
                location="asia-southeast1",
                trigger_rule="all_done",  # It should be "none_failed_min_one_success"
            )
            for source_table, target_table in zip(source_tables, target_tables)
        ]

        chain(*operators)

    mart = data_mart(source_tables=[None, "transaction_24h"], target_tables=["transaction_24h", "accumulation_24h"])

    # Export data from BigQuery ["mart"] to GCS ["public"]
    @task_group(group_id="serving_area")
    def serving_area(mart_types: list[str]) -> None:
        return [
            BigQueryToGCSOperator(
                task_id=f"load_mart_to_gcs_public__{mart_type}",
                gcp_conn_id="gcp",
                project_id=PROJECT_ID,
                source_project_dataset_table=BQ_TABLE_PATHS["mart"].get(mart_type),
                destination_cloud_storage_uris=[
                    f"gs://{GCS_BASE_URIS['public']['bucket_name']}/{GCS_BASE_URIS['public']['folder_path']}/{mart_type}.csv"
                ],
                export_format="CSV",
                field_delimiter=",",
                print_header=True,
            )
            for mart_type in mart_types
        ]

    serve = serving_area(mart_types=["transaction_24h", "accumulation_24h"])

    sensor >> mart >> serve


bq_export_csv_to_gcs()
