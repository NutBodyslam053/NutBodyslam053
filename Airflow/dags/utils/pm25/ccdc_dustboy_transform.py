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
            .str.replace(pat="(?i)[.]|(^properties_)", repl="", regex=True)
            .str.lower()
        )

        # Drop unused features
        dataframe.drop(columns=["type", "geometry_type", "geometry_coordinates"], inplace=True)

        # Exclude rows where `longitude` or `latitude` is an empty string
        dataframe = dataframe[~((dataframe["dustboy_lon"] == "") | (dataframe["dustboy_lat"] == ""))]

        # Filter rows where `longitude` and `latitude` within their valid ranges
        dataframe = dataframe[
            dataframe["dustboy_lon"].astype(float).between(-180, 180)
            & dataframe["dustboy_lat"].astype(float).between(-90, 90)
        ]

        # Remove commas (,) present in numeric columns
        columns_to_replace = [
            "id",
            "dustboy_lat",
            "dustboy_lon",
            "pm10",
            "pm25",
            "wind_speed",
            "wind_direction",
            "atmospheric",
            "pm10_th_aqi",
            "pm10_us_aqi",
            "pm25_th_aqi",
            "pm25_us_aqi",
            "temp",
            "humid",
            "us_aqi",
            "th_aqi",
            "daily_pm10",
            "daily_pm10_th_aqi",
            "daily_pm10_us_aqi",
            "daily_pm25",
            "daily_pm25_th_aqi",
            "daily_pm25_us_aqi",
            "daily_temp",
            "daily_humid",
            "daily_wind_speed",
            "daily_wind_direction",
            "daily_atmospheric",
            "province_id",
            "province_code",
        ]

        dataframe.loc[:, columns_to_replace].replace(to_replace=",", value="", inplace=True)

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
            dataframe = pd.json_normalize(data=dataframe["features"], sep="_")

            dataframe = TransformFunctions._initialize_dataframe(dataframe=dataframe)

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "ingest_date": logical_date.to_date_string(),
                    "ingest_datetime": logical_date.to_datetime_string(),
                },
                dtype_conversion={
                    "id": "str",
                    "dustboy_id": "str",
                    "dustboy_uri": "str",
                    "dustboy_name": "str",
                    "dustboy_name_en": "str",
                    "dustboy_lon": "Float64",
                    "dustboy_lat": "Float64",
                    "pm10": "Float64",
                    "pm25": "Float64",
                    "wind_speed": "Float64",
                    "wind_direction": "Float64",
                    "atmospheric": "Float64",
                    "pm10_th_aqi": "Float64",
                    "pm10_us_aqi": "Float64",
                    "pm25_th_aqi": "Int64",
                    "pm25_us_aqi": "Int64",
                    "temp": "Float64",
                    "humid": "Float64",
                    "us_aqi": "Int64",
                    "us_color": "str",
                    "us_dustboy_icon": "str",
                    "us_title": "str",
                    "us_title_en": "str",
                    "us_caption": "str",
                    "us_caption_en": "str",
                    "th_aqi": "Int64",
                    "th_color": "str",
                    "th_dustboy_icon": "str",
                    "th_title": "str",
                    "th_title_en": "str",
                    "th_caption": "str",
                    "th_caption_en": "str",
                    "daily_pm10": "Float64",
                    "daily_pm10_th_aqi": "Float64",
                    "daily_pm10_us_aqi": "Float64",
                    "daily_pm25": "Float64",
                    "daily_pm25_th_aqi": "Float64",
                    "daily_pm25_us_aqi": "Float64",
                    "daily_th_title": "str",
                    "daily_th_title_en": "str",
                    "daily_us_title": "str",
                    "daily_us_title_en": "str",
                    "daily_th_caption": "str",
                    "daily_th_caption_en": "str",
                    "daily_us_caption": "str",
                    "daily_us_caption_en": "str",
                    "daily_th_color": "str",
                    "daily_us_color": "str",
                    "daily_th_dustboy_icon": "str",
                    "daily_us_dustboy_icon": "str",
                    "daily_temp": "Float64",
                    "daily_humid": "Float64",
                    "daily_wind_speed": "Float64",
                    "daily_wind_direction": "Float64",
                    "daily_atmospheric": "Float64",
                    "province_id": "str",
                    "province_code": "str",
                    "log_datetime": "datetime64[ms]",
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
                    "dustboy_id": "station_id",
                    "dustboy_name": "station_name_th",
                    "log_datetime": "datetime",
                    "dustboy_lon": "longitude",
                    "dustboy_lat": "latitude",
                    "pm25_th_aqi": "pm25_aqi",
                    "pm25": "pm25_value",
                },
                inplace=True,
            )

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "data_owner": "CCDC",
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
