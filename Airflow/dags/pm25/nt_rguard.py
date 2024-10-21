import os
import json
import pandas as pd
from pendulum import datetime, duration, now
from utils.notifications_v2 import send_teams_notification
from utils.utils import generate_gcs_base_uris, generate_bq_table_paths

from airflow.utils.helpers import chain
from airflow.models.xcom_arg import XComArg
from airflow.exceptions import AirflowFailException
from airflow.decorators import dag, task, task_group
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =================================================<< BEGIN >>================================================= #
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
PROJECT_ID = "envilink"
DATA_OWNER = "nt"
DATA_SOURCE = "rguard"
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
    data_owner=DATA_OWNER,
    data_source=DATA_SOURCE,
    data_zones=["raw", "discovery", "processed"],
)
# ===============================================<< BiqQuery >>================================================ #
BQ_TABLE_PATHS = generate_bq_table_paths(
    environment=ENVIRONMENT,
    data_owner=DATA_OWNER,
    data_source=DATA_SOURCE,
    data_category="pm25",
    table_types=["temp", "wh", "master"],
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
def nt_rguard() -> None:

    # Initialize the essential constants
    @task_group(group_id="init_constants")
    def init_constants() -> None:
        @task
        def update_gcs_base_uris(data_interval_end=None) -> XComArg:
            NOW = data_interval_end.in_tz(LOCAL_TIMEZONE)
            DS_NODASH = NOW.format("YYYYMMDD")
            TS_NODASH = NOW.format("HHmm")

            GCS_BASE_URIS["raw"].update(
                {
                    "folder_path": GCS_BASE_URIS["raw"]["folder_path"] + f"/{DS_NODASH}",
                    "file_name": f"{DS_NODASH}_{TS_NODASH}_{DATA_SOURCE}_raw.json",
                }
            )

            return GCS_BASE_URIS

        update_gcs_base_uris()

    init = init_constants()

    # Fetch raw data from API and upload to GCS ["raw"]
    @task_group(group_id="raw_area")
    def raw_area() -> None:
        import asyncio
        from utils.utils import fetch_api_async, upload_to_gcs_from_string

        @task
        def fetch_api_to_gcs_raw(device_info_file_path: str, nt_rguard_api_key: str, ti=None) -> None:
            GCS_BASE_URIS = ti.xcom_pull(task_ids="init_constants.update_gcs_base_uris")
            NT_DEVICE_ID_DATAFRAME = pd.read_csv(
                f"gs://envilink_master_data/{device_info_file_path}", usecols=["DevID"]
            )

            json_data = asyncio.run(
                fetch_api_async(
                    url_template=r"https://rguard.ntdigitalsolutions.com/api/stations/{device_id}/aqi",
                    device_id_list=NT_DEVICE_ID_DATAFRAME["DevID"].to_list(),
                    headers={"apikey": nt_rguard_api_key},
                    timeout=300,  # 5 minutes
                    max_retries=3,
                    retry_delay=60,  # 1 minute
                )
            )

            upload_to_gcs_from_string(
                bucket_name=GCS_BASE_URIS["raw"].get("bucket_name"),
                folder_path=GCS_BASE_URIS["raw"].get("folder_path"),
                file_name=GCS_BASE_URIS["raw"].get("file_name"),
                data=json.dumps(json_data),
                content_type="application/json",
            )

        fetch_api_to_gcs_raw(
            device_info_file_path="{{ var.value.nt_dev_id_install_locations_file_name }}",
            nt_rguard_api_key="{{ var.value.nt_rguard_api_key }}",
        )

    raw = raw_area()

    # Transform raw data and upload to GCS ["discovery", "processed"]
    @task_group(group_id="staging_area")
    def staging_area(source_buckets: list[str], target_buckets: list[str]) -> dict[str, None]:
        from utils.pm25.nt_rguard_transform import TransformFunctions
        from utils.utils import upload_to_gcs_from_parquet

        tasks = {}

        for source_bucket, target_bucket in zip(source_buckets, target_buckets):

            @task(task_id=f"load_gcs_raw_to_gcs_{target_bucket}")
            def process_gcs_data(source_bucket: str, target_bucket: str, ti=None, data_interval_end=None) -> None:
                GCS_BASE_URIS = ti.xcom_pull(task_ids="init_constants.update_gcs_base_uris")

                transform_function = getattr(TransformFunctions, f"transform_{target_bucket}", None)

                if transform_function:
                    transformed_df = transform_function(
                        logical_date=data_interval_end.in_tz(LOCAL_TIMEZONE),
                        bucket_name=GCS_BASE_URIS[source_bucket].get("bucket_name"),
                        folder_path=GCS_BASE_URIS[source_bucket].get("folder_path"),
                        file_name=GCS_BASE_URIS[source_bucket].get("file_name"),
                    )

                    upload_to_gcs_from_parquet(
                        dataframe=transformed_df,
                        bucket_name=GCS_BASE_URIS[target_bucket].get("bucket_name"),
                        folder_path=GCS_BASE_URIS[target_bucket].get("folder_path"),
                        partition_cols=["ingest_date"],
                    )
                else:
                    raise AirflowFailException(f"Function 'transform_{target_bucket}' not found.")

            tasks[target_bucket] = process_gcs_data(source_bucket=source_bucket, target_bucket=target_bucket)

        return {target_bucket: tasks[target_bucket] for target_bucket in target_buckets}

    staging = staging_area(source_buckets=["raw", "discovery"], target_buckets=["discovery", "processed"])

    # Transfer processed data to BigQuery ["temp", "warehouse"]
    @task_group(group_id="data_warehouse")
    def data_warehouse(
        wh_types: list[str], parent_dags: list[str]
    ) -> list[None, ExternalTaskSensor, BigQueryInsertJobOperator]:

        # Upload processed data from GCS to BigQuery ["temp"]
        @task
        def load_gcs_processed_to_bq_temp(data_interval_end=None) -> None:
            import pandas_gbq
            import pandas as pd
            from utils.utils import download_from_gcs_as_dataframe, generate_bq_schema

            NOW = data_interval_end.in_tz(LOCAL_TIMEZONE)
            DS = NOW.format("YYYY-MM-DD")
            TS = NOW.format("YYYY-MM-DD HH:mm:ss")

            processed_df = download_from_gcs_as_dataframe(
                bucket_name=GCS_BASE_URIS["processed"].get("bucket_name"),
                folder_path=GCS_BASE_URIS["processed"].get("folder_path"),
                partition_dict={"ingest_date": DS},
                filters=[("ingest_datetime", "==", pd.Timestamp(TS))],
            )

            if len(processed_df):
                pandas_gbq.to_gbq(
                    dataframe=processed_df,
                    destination_table=BQ_TABLE_PATHS["temp"],
                    project_id=PROJECT_ID,
                    if_exists="replace",
                    table_schema=generate_bq_schema(processed_df),
                )
                print("Processed data uploaded to BigQuery successfully.")
            else:
                raise AirflowFailException(f"DataFrame is empty. No data to upload.")

        temp = load_gcs_processed_to_bq_temp()

        # Waiting for completion of parent DAG task upserting data to `station_info` table
        sensors = [
            ExternalTaskSensor(
                task_id=f"waiting_for_{parent_dag}",
                external_dag_id=parent_dag,
                external_task_id="data_warehouse.upsert_table_temp_to_wh__station_info",
                allowed_states=["success", "failed", "skipped", "upstream_failed"],
                mode="poke",
                timeout=120,  # 2 minutes
                poke_interval=60,  # 1 minute
                soft_fail=False,
                check_existence=True,
            )
            for parent_dag in parent_dags
        ]

        # Upsert processed data from BigQuery ["temp"] to ["warehouse"]
        operators = [
            BigQueryInsertJobOperator(
                task_id=f"upsert_table_temp_to_wh__{wh_type}",
                gcp_conn_id="gcp",
                project_id=PROJECT_ID,
                configuration={
                    "jobType": "QUERY",
                    "query": {
                        "query": f"{{% include 'sql/upsert_table_temp_to_wh__{wh_type}.sql' %}}",
                        "useLegacySql": False,
                    },
                    "jobTimeoutMs": (now().int_timestamp + 120) * 1000,  # 2 minutes
                },
                params={
                    "source_table": BQ_TABLE_PATHS["temp"],
                    "target_table": BQ_TABLE_PATHS["wh"].get(wh_type),
                    "geog_table": BQ_TABLE_PATHS["master"].get("gistda_th_geog"),
                    "station_info_table": BQ_TABLE_PATHS["wh"].get("station_info"),
                },
                location="asia-southeast1",
            )
            for wh_type in wh_types
        ]

        chain(temp, sensors, *operators)

    warehouse = data_warehouse(wh_types=["station_info", "station_reads"], parent_dags=["dpm_dpmalert"])

    init >> raw >> staging["discovery"] >> staging["processed"] >> warehouse


nt_rguard()
