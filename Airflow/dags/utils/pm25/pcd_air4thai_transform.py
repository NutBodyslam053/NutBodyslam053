import numpy as np
import pandas as pd
from io import BytesIO
from pendulum import DateTime
from airflow.exceptions import AirflowFailException
from utils.utils import download_from_gcs_as_bytes, download_from_gcs_as_dataframe, convert_pm25_value_to_pm25_color_id


class TransformFunctions:

    @staticmethod
    def _initialize_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:

        # Clean up column names (snake_case, remove prefixes, clean formatting)
        dataframe.columns = (
            dataframe.columns.str.strip()
            .str.replace(pat="[- ]|(?<=[a-z])(?=[A-Z])", repl="_", regex=True)
            .str.replace(pat="(?i)[.]|(^aqilast_)", repl="", regex=True)
            .str.lower()
        )

        # Replace -1 and -999 with NaN
        dataframe.replace(to_replace=["-1", "-999"], value=np.nan, inplace=True)

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

        # Convert data types if provided
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

        try:
            # Download raw data from GCS as Bytes
            file_content = download_from_gcs_as_bytes(
                bucket_name=bucket_name,
                folder_path=folder_path,
                file_name=file_name,
            )

            dataframe = pd.read_json(BytesIO(file_content))
            dataframe = pd.json_normalize(data=dataframe["stations"], sep="_")

            dataframe = TransformFunctions._initialize_dataframe(dataframe=dataframe)

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "ingest_date": logical_date.to_date_string(),
                    "ingest_datetime": logical_date.to_datetime_string(),
                },
                dtype_conversion={
                    "station_id": "str",
                    "name_th": "str",
                    "name_en": "str",
                    "area_th": "str",
                    "area_en": "str",
                    "station_type": "str",
                    "long": "Float64",
                    "lat": "Float64",
                    "forecast": "str",
                    "date": "str",
                    "time": "str",
                    "pm25_color_id": "Int64",
                    "pm25_aqi": "Int64",
                    "pm25_value": "Float64",
                    "pm10_color_id": "Int64",
                    "pm10_aqi": "Int64",
                    "pm10_value": "Float64",
                    "o3_color_id": "Int64",
                    "o3_aqi": "Int64",
                    "o3_value": "Float64",
                    "co_color_id": "Int64",
                    "co_aqi": "Int64",
                    "co_value": "Float64",
                    "no2_color_id": "Int64",
                    "no2_aqi": "Int64",
                    "no2_value": "Float64",
                    "so2_color_id": "Int64",
                    "so2_aqi": "Int64",
                    "so2_value": "Float64",
                    "aqi_color_id": "Int64",
                    "aqi_aqi": "Int64",
                    "aqi_param": "str",
                    "ingest_date": "str",
                    "ingest_datetime": "datetime64[ms]",
                },
            )

            return dataframe

        except Exception as e:
            raise AirflowFailException(f"Error in stage (discovery): {e}")

    @staticmethod
    def transform_processed(
        logical_date: DateTime,
        bucket_name: str,
        folder_path: str,
        file_name: str | None = None,
    ) -> pd.DataFrame:

        try:
            # Download raw data from GCS as DataFrame
            dataframe = download_from_gcs_as_dataframe(
                bucket_name=bucket_name,
                folder_path=folder_path,
                partition_dict={"ingest_date": logical_date.format("YYYY-MM-DD")},
                filters=[
                    ("ingest_datetime", "==", pd.Timestamp(logical_date.format("YYYY-MM-DD HH:mm:ss"))),
                ],
            )

            dataframe.rename(
                columns={
                    "long": "longitude",
                    "lat": "latitude",
                    "name_th": "station_name_th",
                },
                inplace=True,
            )

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "data_owner": "PCD",
                    "datetime": dataframe["date"].astype(str) + " " + dataframe["time"].astype(str),
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
            raise AirflowFailException(f"Error in stage (processed): {e}")
