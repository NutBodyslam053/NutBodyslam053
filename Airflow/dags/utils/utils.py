import re
import boto3
import aiohttp
import asyncio
import requests
import pandas as pd
import pyarrow as pa
from time import sleep
from io import BytesIO
from google.cloud import storage
from asyncio import TimeoutError
from aiohttp import ClientResponseError, ClientConnectorError, ClientTimeout
from requests.exceptions import RequestException, HTTPError, Timeout, JSONDecodeError

from airflow.exceptions import AirflowFailException


def fetch_api_sync(
    url: str,
    homepage_url: str | None = None,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 300,  # 5 minutes
    max_retries: int = 3,
    retry_delay: int = 60,  # 1 minute
) -> dict[str]:
    attempt = 0
    status_code = "N/A"

    while attempt < max_retries:
        try:
            print(f"Attempt {attempt + 1} of {max_retries} to fetch data from {url}")

            if homepage_url:
                with requests.Session() as session:
                    session.get(homepage_url, timeout=5)
                    response = session.get(url=url, headers=headers, params=params, timeout=timeout)
            else:
                response = requests.get(url=url, params=params, headers=headers, timeout=timeout)

            status_code = response.status_code

            response.raise_for_status()
            json_data = response.json()

            print("Fetched data from API successfully.")
            return json_data  # Exit loop if request is successful

        except JSONDecodeError:
            raise AirflowFailException(f"Failed to decode response to JSON format. | Status code: ({status_code})")
        except HTTPError as e:
            if status_code == 401:
                raise AirflowFailException(
                    f"Unauthorized access - Token or API key may be expired or invalid. | Status code: ({status_code})"
                )
            print(
                f"HTTPError: {e} on attempt {attempt + 1} of {max_retries}. | Status code: ({status_code}), Retrying in {retry_delay} seconds..."
            )
        except Timeout:
            print(
                f"Timeout: Request timed out on attempt {attempt + 1} of {max_retries}. | Status code: ({status_code}), Retrying in {retry_delay} seconds..."
            )
        except RequestException as e:
            print(
                f"RequestException: {e} on attempt {attempt + 1} of {max_retries}. | Status code: ({status_code}), Retrying in {retry_delay} seconds..."
            )

        attempt += 1
        sleep(retry_delay)

    raise AirflowFailException(f"Failed to fetch data from API after {max_retries} attempts!!!")


async def fetch_api_async(
    url_template: str,
    device_id_list: list[str],
    headers: dict[str, str] | None = None,
    timeout: int = 300,
    max_retries: int = 3,
    retry_delay: int = 60,
    max_concurrency: int = 20,
) -> dict[str, dict[str]]:
    semaphore = asyncio.Semaphore(max_concurrency)
    urls = [url_template.format(device_id=device_id) for device_id in device_id_list]

    async def fetch(session: aiohttp.ClientSession, url: str, device_id: str) -> dict[str, dict]:
        async with semaphore:
            attempt = 0
            status_code = "N/A"
            while attempt < max_retries:
                try:
                    if len(urls) < 20:
                        print(f"Attempt {attempt + 1} of {max_retries} to fetch data from {url}")
                    async with session.get(
                        url,
                        headers=headers,
                        raise_for_status=True,
                        timeout=ClientTimeout(total=timeout),
                    ) as response:
                        status_code = response.status
                        data = await response.json()
                        return {device_id: data}

                except ClientResponseError as e:
                    if e.status == 401:
                        raise AirflowFailException(
                            f"Unauthorized access - Token or API key may be expired or invalid. | Status code: ({e.status})"
                        )
                    if e.status == 429 and "Retry-After" in e.headers and e.headers["Retry-After"].isdigit():
                        retry_after = int(e.headers["Retry-After"])
                        print(
                            f"ClientResponseError: Rate limit reached on attempt {attempt + 1} of {max_retries}. | Status code: ({e.status}) | Device ID: {device_id}, Retrying in {retry_after} seconds..."
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    print(
                        f"ClientResponseError: {e} on attempt {attempt + 1} of {max_retries}. | Status code: ({e.status}) | Device ID: {device_id}, Retrying in {retry_delay} seconds..."
                    )
                except ClientConnectorError as e:
                    print(
                        f"ClientConnectorError: Network connection error on attempt {attempt + 1} of {max_retries}. | Status code: ({status_code}) | Device ID: {device_id}, Retrying in {retry_delay} seconds..."
                    )
                except TimeoutError:
                    print(
                        f"TimeoutError: Request timed out on attempt {attempt + 1} of {max_retries}. | Status code: ({status_code}) | Device ID: {device_id}, Retrying in {retry_delay} seconds..."
                    )
                except Exception as e:
                    print(
                        f"Exception: {e} on attempt {attempt + 1} of {max_retries}. | Status code: ({status_code}) | Device ID: {device_id}, Retrying in {retry_delay} seconds..."
                    )

                attempt += 1
                if attempt >= max_retries:
                    print(f"Failed to fetch data from API after {max_retries} attempts!!!")
                    return {
                        device_id: {
                            "error": str(e),
                            "status_code": status_code,
                            "headers": (dict(e.headers) if hasattr(e, "headers") and e.headers else {}),
                        }
                    }
                await asyncio.sleep(retry_delay)

    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(fetch(session=session, url=url, device_id=device_id))
            for url, device_id in zip(urls, device_id_list)
        ]

        results = await asyncio.gather(*tasks)

    json_data = {k: v for d in results for k, v in d.items()}

    print("Fetched data from API successfully.")

    return json_data


def generate_gcs_base_uris(
    environment: str,
    project_id: str,
    data_zones: list[str],
    data_owner: str | None = None,
    data_source: str | None = None,
) -> dict[str, str]:
    gcs_base_uris = {
        data_zone.lower(): {
            "bucket_name": (f"{project_id}_{data_zone}" if environment == "prd" else f"{project_id}_dev").lower(),
            "folder_path": (
                "mart_pm25"
                if environment == "prd" and data_zone == "public"
                else (
                    f"{project_id}_{data_zone}/mart_pm25"
                    if environment == "dev" and data_zone == "public"
                    else (
                        f"{data_owner}/{data_source}"
                        if environment == "prd"
                        else f"{project_id}_{data_zone}/{data_owner}/{data_source}"
                    )
                )
            ).lower(),
        }
        for data_zone in data_zones
    }

    return gcs_base_uris


def generate_bq_table_paths(
    environment: str,
    data_category: str,
    table_types: list[str],
    data_owner: str | None = None,
    data_source: str | None = None,
) -> dict[str, str]:
    mappings = {
        "prd": {
            "temp": f"temp.{data_owner}_{data_source}",
            "wh": {
                "station_info": f"wh_{data_category}.station_info",
                "station_reads": f"wh_{data_category}.station_reads",
            },
            "mart": {
                "transaction_24h": f"mart_{data_category}.transaction_24h",
                "accumulation_24h": f"mart_{data_category}.accumulation_24h",
            },
            "master": {
                "gistda_th_geog": f"master.gistda_th_geography",
            },
        },
        "dev": {
            "temp": f"dev.temp-{data_owner}_{data_source}",
            "wh": {
                "station_info": f"dev.wh_{data_category}-station_info",
                "station_reads": f"dev.wh_{data_category}-station_reads",
            },
            "mart": {
                "transaction_24h": f"dev.mart_{data_category}-transaction_24h",
                "accumulation_24h": f"dev.mart_{data_category}-accumulation_24h",
            },
            "master": {
                "gistda_th_geog": f"master.gistda_th_geography",
            },
        },
    }

    table_mapping = mappings[environment]

    bq_table_paths = {table_type: table_mapping[table_type] for table_type in table_types}

    return bq_table_paths


def convert_pm25_value_to_pm25_color_id(pm25_value: int | float) -> int:
    try:
        pm25_value = float(pm25_value)

        if 0 <= pm25_value <= 15:
            return 1
        elif 15 < pm25_value <= 25:
            return 2
        elif 25 < pm25_value <= 37.5:
            return 3
        elif 37.5 < pm25_value <= 75:
            return 4
        elif pm25_value > 75:
            return 5

    except (ValueError, TypeError):
        return 0

    return 0


def pm25_value_to_color(row, pm25_col_name):
    pm25_value = row[pm25_col_name]
    if pm25_value >= 0 and pm25_value <= 15:
        pm25_color = "blue"
    elif pm25_value > 15 and pm25_value <= 25:
        pm25_color = "green"
    elif pm25_value > 25 and pm25_value <= 37.5:
        pm25_color = "yellow"
    elif pm25_value > 37.5 and pm25_value <= 75:
        pm25_color = "orange"
    elif pm25_value > 75:
        pm25_color = "red"
    else:
        pm25_color = None

    return pm25_color


def list_minio_objects(
    bucket_name: str,
    minio_endpoint: str,
    access_key: str,
    secret_key: str,
    extensions: str | tuple[str] | None = None,
) -> list[str]:

    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    response = s3_client.list_objects_v2(Bucket=bucket_name)

    if extensions:
        extensions = tuple(ext.lower() for ext in extensions)
        files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].lower().endswith(extensions)]
    else:
        files = sorted([obj["Key"] for obj in response.get("Contents", [])])

    return files


def read_excel_from_minio(
    bucket_name: str,
    file_name: str,
    minio_endpoint: str,
    access_key: str,
    secret_key: str,
) -> pd.DataFrame:

    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response["Body"].read()

    dataframe = pd.read_excel(BytesIO(file_content))

    return dataframe


def delete_minio_objects(
    bucket_name: str,
    minio_endpoint: str,
    access_key: str,
    secret_key: str,
    extensions: str | tuple[str, ...] | None = None,
    selected_files: list[str] | None = None,
) -> None:

    files_to_delete = list_minio_objects(
        bucket_name=bucket_name,
        minio_endpoint=minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        extensions=extensions,
    )

    if selected_files:
        selected_files = [file.lower() for file in selected_files]
        files_to_delete = [file for file in files_to_delete if file.lower() in selected_files]

    if not files_to_delete:
        print(f"No files found to delete.")
        return

    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    delete_objects = [{"Key": key} for key in files_to_delete]

    delete_response = s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_objects})

    deleted_files = delete_response.get("Deleted", [])

    if deleted_files:
        print("Deleted files:")
        for file in deleted_files:
            print(f"    File '{file['Key']}' deleted successfully.")
    else:
        print("No files were deleted.")


def drop_non_priority_columns(dataframe: pd.DataFrame, priority_columns: list[str]) -> pd.DataFrame:
    selected_columns = [col for col in priority_columns if col in dataframe.columns]

    if selected_columns:
        selected_column = selected_columns[0]
        dataframe = dataframe.drop(
            columns=[col for col in priority_columns if col != selected_column],
            errors="ignore",
        )

    return dataframe


def drop_pii_columns(dataframe: pd.DataFrame, columns_to_exclude: list[str]) -> pd.DataFrame:
    exclusion_string = r"|".join(map(re.escape, columns_to_exclude))
    pattern_string = rf"(?i)^(?!.*({exclusion_string})).*$"

    dataframe = dataframe.filter(regex=pattern_string, axis=1).copy()

    return dataframe


def rename_gcs_files(
    bucket_name: str,
    prefix: str,
    replacement: str,
    pattern: str = r"(?<=/)[^/]+(?=\.\w+)",
) -> None:

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if not blob.name.endswith("/"):
            new_blobname = re.sub(pattern=pattern, repl=replacement, string=blob.name)
            new_blob = bucket.rename_blob(blob=blob, new_name=new_blobname)
            print(f"Blob '{blob.name}' renamed to '{new_blob.name}'")

    return


def list_gcs_files(
    bucket_name: str,
    prefix: str = None,
    extensions: str | tuple[str, ...] | None = None,
) -> list[str]:

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    file_list = []

    if extensions:
        for blob in blobs:
            if not blob.name.endswith("/") and blob.name.endswith(extensions):
                file_list.append(blob.name)

    else:
        for blob in blobs:
            if not blob.name.endswith("/"):
                file_list.append(blob.name)

    return file_list


def read_excel_from_gcs(bucket_name: str, file_path: str) -> pd.DataFrame:

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    content = blob.download_as_bytes()
    dataframe = pd.read_excel(BytesIO(content), index_col=False, dtype=str)

    return dataframe


# ========== **WARNING:** This function permanently deletes GCS files. Use with caution! ==========#
# def delete_gcs_files(
#     bucket_name: str,
#     selected_files: List[str] = None,
#     prefix: str = None,
#     extensions: str | tuple[str, ...] = None,
# ) -> None:
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)

#     if selected_files:
#         for file_name in selected_files:
#             blob = bucket.blob(file_name)
#             blob.delete()
#             print(f"Blob '{file_name}' deleted.")

#     else:
#         blobs = bucket.list_blobs(prefix=prefix)

#         if extensions:
#             for blob in blobs:
#                 if not blob.name.endswith("/") and blob.name.endswith(extensions):
#                     blob.delete()
#                     print(f"Blob '{blob.name}' deleted.")
#         else:
#             for blob in blobs:
#                 if not blob.name.endswith("/"):
#                     blob.delete()
#                     print(f"Blob '{blob.name}' deleted.")
# ========== **WARNING:** This function permanently deletes GCS files. Use with caution! ==========#


def upload_to_gcs_from_string(
    bucket_name: str,
    folder_path: str,
    file_name: str,
    data: str,
    content_type: str,
) -> None:
    file_path = f"{folder_path}/{file_name}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob.upload_from_string(data=data, content_type=content_type)

    print(f"Raw data uploaded to 'gs://{bucket_name}/{file_path}' successfully.")


def download_from_gcs_as_string(
    bucket_name: str,
    folder_path: str,
    file_name: str,
) -> str:

    file_path = f"{folder_path}/{file_name}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    file_content = blob.download_as_string().decode("utf-8")

    return file_content


def download_from_gcs_as_bytes(
    bucket_name: str,
    folder_path: str,
    file_name: str,
) -> str:

    file_path = f"{folder_path}/{file_name}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    file_content = blob.download_as_bytes()

    return file_content


def format_partition_cols(dataframe: pd.DataFrame, partition_cols: list[str]) -> str:
    results = []

    for col in partition_cols:
        if col in dataframe.columns:
            unique_value = str(dataframe[col].unique()[0])
            results.append(f"{col}={unique_value}")

    return "/".join(results)


def upload_to_gcs_from_parquet(
    dataframe: pd.DataFrame,
    bucket_name: str,
    folder_path: str,
    index: bool = False,
    engine: str = "pyarrow",
    partition_cols: list[str] | None = None,
) -> None:
    gcs_base_uri = f"gs://{bucket_name}/{folder_path}"

    dataframe.to_parquet(
        path=gcs_base_uri,
        index=index,
        engine=engine,
        partition_cols=partition_cols,
    )

    partition_cols_path = format_partition_cols(dataframe=dataframe, partition_cols=partition_cols)

    print(
        f"{dataframe.info()}\n{'-'*100}\nNumber of rows: ({len(dataframe):,}), Partition columns: {partition_cols}\n{'-'*100}"
    )
    print(
        f"{'Transformed' if 'processed' in gcs_base_uri else 'Original'} data uploaded to '{gcs_base_uri}/{partition_cols_path}/*.parquet' successfully."
    )


def download_from_gcs_as_dataframe(
    bucket_name: str,
    folder_path: str,
    partition_dict: dict[str, str] = None,
    filters: list[tuple] | list[list[tuple]] | None = None,
    schema: pa.Schema | None = None,
) -> pd.DataFrame:
    gcs_base_uri = f"gs://{bucket_name}/{folder_path}"

    if partition_dict:
        for col, value in partition_dict.items():
            gcs_base_uri += f"/{col}={value}"

    dataframe = pd.read_parquet(
        path=gcs_base_uri,
        filters=filters,
        schema=schema,
    )

    return dataframe


def convert_df_to_bq_schema(dataframe: pd.DataFrame) -> list[dict[str]]:
    schema = []

    for col in dataframe.columns:
        dtype = dataframe[col].dtype

        if col.lower() in ["date", "ingest_date"]:
            bq_type = "DATE"
        elif pd.api.types.is_datetime64_dtype(dtype):
            bq_type = "DATETIME"
        elif pd.api.types.is_string_dtype(dtype):
            bq_type = "STRING"
        elif pd.api.types.is_integer_dtype(dtype):
            bq_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = "FLOAT"
        else:
            bq_type = "STRING"

        schema.append({"name": col, "type": bq_type, "mode": "NULLABLE"})

    return schema


def convert_bq_schema_to_pa_schema(schema: list) -> pa.Schema:
    result = []

    for field in schema:
        name = field["name"]
        field_type = field["type"]

        if field_type.lower() in ["datetime", "timestamp"]:
            data_type = pa.timestamp("ms")
        elif field_type.lower() == "int":
            data_type = pa.int64()
        elif field_type.lower() == "float":
            data_type = pa.float64()
        else:
            data_type = pa.string()

        result.append((name, data_type))

    converted_schema = pa.schema(result)

    return converted_schema


def generate_bq_schema(dataframe: pd.DataFrame) -> list[dict[str]]:
    def convert_df_dtype_to_bq_type(column: str, dtype: str) -> str:
        if column == "ingest_date":
            return "DATE"
        elif pd.api.types.is_datetime64_dtype(dtype):
            return "DATETIME"
        elif pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        elif pd.api.types.is_numeric_dtype(dtype):
            return "INTEGER"
        elif pd.api.types.is_string_dtype(dtype):
            return "STRING"
        else:
            return "STRING"

    bigquery_schema = [
        {
            "name": column,
            "type": convert_df_dtype_to_bq_type(column, dtype),
            "mode": "NULLABLE",
        }
        for column, dtype in dataframe.dtypes.items()
    ]

    return bigquery_schema
