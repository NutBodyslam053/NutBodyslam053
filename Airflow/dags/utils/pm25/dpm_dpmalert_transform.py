import numpy as np
import pandas as pd
from io import BytesIO
from pendulum import DateTime
from airflow.exceptions import AirflowFailException
from utils.utils import (
    download_from_gcs_as_bytes,
    download_from_gcs_as_dataframe,
    convert_buddhist_year_to_gregorian_year,
    convert_pm25_value_to_pm25_color_id,
)


class TransformFunctions:

    @staticmethod
    def _initialize_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:

        # Clean up column names (snake_case, remove prefixes, clean formatting)
        dataframe.columns = (
            dataframe.columns.str.strip()
            .str.replace(pat="nO", repl="no", case=True)
            .str.replace(pat="pM", repl="pm", case=True)
            .str.replace(pat="[- ]|(?<=[a-z])(?=[A-Z])", repl="_", regex=True)
            .str.replace(pat="(?i)[.]", repl="_", regex=True)
            .str.lower()
        )

        # Drop duplicated columns
        dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()].copy()  # e.g. ["station_province_id"]

        # Convert Buddhist year to Gregorian year for specific columns
        columns_to_convert = [
            "air_quality_data_hourly_time",
            "station_installation_date",
            "station_update_time",
            "station_province_deletion_time",
            "station_province_last_modification_time",
            "station_province_creation_time",
            "station_deletion_time",
            "station_last_modification_time",
            "station_creation_time",
            "station_device_installation_date",
            "station_device_deletion_time",
            "station_device_last_modification_time",
            "station_device_creation_time",
            "province_deletion_time",
            "province_last_modification_time",
            "province_creation_time",
        ]

        dataframe[columns_to_convert] = dataframe[columns_to_convert].apply(convert_buddhist_year_to_gregorian_year)

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
            dataframe = pd.json_normalize(data=dataframe["items"], sep="_")

            dataframe = TransformFunctions._initialize_dataframe(dataframe=dataframe)

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "ingest_date": logical_date.to_date_string(),
                    "ingest_datetime": logical_date.to_datetime_string(),
                },
                dtype_conversion={
                    "station_fullname": "str",
                    "air_quality_data_hourly_temperature": "Float64",
                    "air_quality_data_hourly_dew_point": "Float64",
                    "air_quality_data_hourly_no2": "Float64",
                    "air_quality_data_hourly_ox": "Float64",
                    "air_quality_data_hourly_o3": "Float64",
                    "air_quality_data_hourly_relative_humidity": "Float64",
                    "air_quality_data_hourly_pm1": "Float64",
                    "air_quality_data_hourly_pm25": "Float64",
                    "air_quality_data_hourly_pm10": "Float64",
                    "air_quality_data_hourly_tsp": "Float64",
                    "air_quality_data_hourly_time": "datetime64[ms]",
                    "air_quality_data_hourly_station_id": "str",
                    "air_quality_data_hourly_id": "str",
                    "station_name": "str",
                    "station_location": "str",
                    "station_latitude": "Float64",
                    "station_longitude": "Float64",
                    "station_contact_name": "str",
                    "station_contact_phone": "str",
                    "station_sim_card_number": "str",
                    "station_installation_engineer": "str",
                    "station_installation_date": "datetime64[ms]",
                    "station_site_address": "str",
                    "station_logger_net_reference": "str",
                    "station_image": "str",
                    "station_update_by": "str",
                    "station_update_time": "datetime64[ms]",
                    "station_province_id": "str",
                    "station_amphoe_id": "str",
                    "station_tambon_id": "str",
                    "station_code": "str",
                    "station_display_name": "str",
                    "station_remark": "str",
                    "station_station_device": "str",
                    "station_province_name": "str",
                    "station_province_geo_code": "str",
                    "station_province_region": "str",
                    "station_province_region_id": "str",
                    "station_province_latitude": "Float64",
                    "station_province_longitude": "Float64",
                    "station_province_name_en": "str",
                    "station_province_is_deleted": "bool",
                    "station_province_deleter_id": "str",
                    "station_province_deletion_time": "datetime64[ms]",
                    "station_province_last_modification_time": "datetime64[ms]",
                    "station_province_last_modifier_id": "str",
                    "station_province_creation_time": "datetime64[ms]",
                    "station_province_creator_id": "str",
                    "station_amphoe": "str",
                    "station_tambon": "str",
                    "station_is_deleted": "bool",
                    "station_deleter_id": "str",
                    "station_deletion_time": "datetime64[ms]",
                    "station_last_modification_time": "datetime64[ms]",
                    "station_last_modifier_id": "str",
                    "station_creation_time": "datetime64[ms]",
                    "station_creator_id": "str",
                    "station_id": "str",
                    "station_device_name": "str",
                    "station_device_serial_number": "str",
                    "station_device_sensor_type": "Int64",
                    "station_device_station_id": "str",
                    "station_device_sim_card_number": "str",
                    "station_device_installation_engineer": "str",
                    "station_device_installation_date": "datetime64[ms]",
                    "station_device_active": "bool",
                    "station_device_is_deleted": "bool",
                    "station_device_deleter_id": "str",
                    "station_device_deletion_time": "datetime64[ms]",
                    "station_device_last_modification_time": "datetime64[ms]",
                    "station_device_last_modifier_id": "str",
                    "station_device_creation_time": "datetime64[ms]",
                    "station_device_creator_id": "str",
                    "station_device_id": "str",
                    "province_name": "str",
                    "province_geo_code": "str",
                    "province_region": "str",
                    "province_region_id": "str",
                    "province_latitude": "Float64",
                    "province_longitude": "Float64",
                    "province_name_en": "str",
                    "province_is_deleted": "bool",
                    "province_deleter_id": "str",
                    "province_deletion_time": "datetime64[ms]",
                    "province_last_modification_time": "datetime64[ms]",
                    "province_last_modifier_id": "str",
                    "province_creation_time": "datetime64[ms]",
                    "province_creator_id": "str",
                    "province_id": "str",
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
                    "air_quality_data_hourly_time": "datetime",
                    "station_longitude": "longitude",
                    "station_latitude": "latitude",
                    "air_quality_data_hourly_pm25": "pm25_value",
                },
                inplace=True,
            )

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "station_id": dataframe["station_device_serial_number"],
                    "station_name_th": dataframe["station_fullname"].str.replace(
                        pat="^[a-zA-Z]{2}\d{2}\s*-\s*", repl="", regex=True
                    ),
                    "data_owner": "DPM",
                    "pm25_aqi": np.nan,
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
