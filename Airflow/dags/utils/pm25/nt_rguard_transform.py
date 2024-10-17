import pandas as pd
from pendulum import DateTime

from airflow.exceptions import AirflowFailException


class TransformFunctions:
    @staticmethod
    def _initialize_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
        # Rename columns to snake_case, remove unnecessary prefixes, and clean up formatting
        dataframe.columns = (
            dataframe.columns.str.strip()
            .str.replace("[- ]|(?<=[a-z])(?=[A-Z])", "_", regex=True)
            .str.replace("(?i)[.]", "", regex=True)
            .str.lower()
        )

        dataframe = dataframe.rename(
            columns={
                "pm2_5": "pm25_value",
                "aqi": "pm25_aqi",
                "dev_id": "station_id",
                "name": "station_name_th",
            }
        )

        return dataframe

    @staticmethod
    def _clean_and_transform_dataframe(
        dataframe: pd.DataFrame,
        additional_columns: dict[str] | None = None,
        select_columns: list[str] | None = None,
        dtype_conversion: dict[str] | None = None,
    ) -> pd.DataFrame:
        # Add additional columns if provided
        if additional_columns:
            for col, value in additional_columns.items():
                dataframe[col] = value

        # Select and convert the relevant columns if provided
        if select_columns:
            dataframe = dataframe[select_columns]

        if dtype_conversion:
            dataframe = dataframe.astype(dtype_conversion)

        return dataframe

    @staticmethod
    def transform_discovery(
        logical_date: DateTime,
        bucket_name: str,
        folder_path: str,
        file_name: str | None = None,
    ) -> pd.DataFrame:
        from io import BytesIO
        from airflow.models import Variable
        from utils.utils import download_from_gcs_as_bytes

        try:
            # Download raw data from GCS as `bytes`
            file_content = download_from_gcs_as_bytes(
                bucket_name=bucket_name,
                folder_path=folder_path,
                file_name=file_name,
            )

            dataframe = pd.read_json(BytesIO(file_content), orient="index")
            dataframe = pd.json_normalize(data=dataframe["result"], record_path="data", sep="_")

            device_id_file_path = Variable.get("nt_dev_id_install_locations_file_name")
            device_id_dataframe = pd.read_csv(f"gs://envilink_master_data/{device_id_file_path}")

            # Merge with NT sensor install location dataframe
            dataframe = pd.merge(
                dataframe,
                device_id_dataframe,
                left_on="device",
                right_on="DevID",
                how="left",
            )

            dataframe = TransformFunctions._initialize_dataframe(dataframe=dataframe)

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "ingest_date": logical_date.to_date_string(),
                    "ingest_datetime": logical_date.to_datetime_string(),
                },
                dtype_conversion={
                    "timestamp": "datetime64[ms, UTC+07:00]",
                    "pm1": "Float64",
                    "pm25_value": "Float64",
                    "pm10": "Float64",
                    "pm25_aqi": "Int64",
                    "pollution_level": "str",
                    "is_forecast": "bool",
                    "temperature": "Float64",
                    "humidity": "Float64",
                    "pressure": "Float64",
                    "wind_direction": "Float64",
                    "wind_speed": "Float64",
                    "device": "str",
                    "id": "str",
                    "type": "str",
                    "station_name_th": "str",
                    "subdistrict": "str",
                    "district": "str",
                    "province_code": "str",
                    "province": "str",
                    "zipcode": "str",
                    "region": "str",
                    "longitude": "Float64",
                    "latitude": "Float64",
                    "station_id": "str",
                    "ingest_date": "str",
                    "ingest_datetime": "datetime64[ms]",
                },
            )

            return dataframe

        except Exception as e:
            raise AirflowFailException(e)

    @staticmethod
    def transform_processed(
        logical_date: DateTime,
        bucket_name: str,
        folder_path: str,
        file_name: str | None = None,
    ) -> pd.DataFrame:
        from utils.utils import convert_pm25_value_to_pm25_color_id, download_from_gcs_as_dataframe

        try:
            # Download raw data from GCS as `DataFrame`
            dataframe = download_from_gcs_as_dataframe(
                bucket_name=bucket_name,
                folder_path=folder_path,
                partition_dict={"ingest_date": logical_date.format("YYYY-MM-DD")},
                filters=[
                    ("ingest_datetime", "==", pd.Timestamp(logical_date.format("YYYY-MM-DD HH:mm:ss"))),
                ],
            )

            dataframe = TransformFunctions._initialize_dataframe(dataframe=dataframe)

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "data_owner": "NT",
                    "datetime": pd.to_datetime(dataframe["timestamp"]).dt.tz_localize(None),
                    "pm25_color_id": dataframe["pm25_value"].apply(convert_pm25_value_to_pm25_color_id),
                    "ingest_date": logical_date.to_date_string(),
                    "ingest_datetime": logical_date.to_datetime_string(),
                },
                select_columns=[
                    "ingest_datetime",
                    "datetime",
                    "data_owner",
                    "station_id",
                    "station_name_th",
                    "longitude",
                    "latitude",
                    "pm25_aqi",
                    "pm25_value",
                    "pm25_color_id",
                    "ingest_date",
                ],
                dtype_conversion={
                    "ingest_datetime": "datetime64[ms]",
                    "datetime": "datetime64[ms]",
                    "data_owner": "str",
                    "station_id": "str",
                    "station_name_th": "str",
                    "longitude": "Float64",
                    "latitude": "Float64",
                    "pm25_aqi": "Int64",
                    "pm25_value": "Float64",
                    "pm25_color_id": "Int64",
                    "ingest_date": "str",
                },
            )

            return dataframe

        except Exception as e:
            raise AirflowFailException(e)
