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
            .str.replace("(?i)[.]", "_", regex=True)
            .str.lower()
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
        from utils.utils import download_from_gcs_as_bytes

        try:
            # Download raw data from GCS as `bytes`
            file_content = download_from_gcs_as_bytes(
                bucket_name=bucket_name,
                folder_path=folder_path,
                file_name=file_name,
            )

            dataframe = pd.json_normalize(data=BytesIO(file_content)["items"], sep="_")

            dataframe = TransformFunctions._initialize_dataframe(dataframe=dataframe)

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "ingest_date": logical_date.to_date_string(),
                    "ingest_datetime": logical_date.to_datetime_string(),
                },
                dtype_conversion={
                    "stationFullname": "str",
                    "airQualityDataHourly_temperature": "str",
                    "airQualityDataHourly_dewPoint": "Float64",
                    "airQualityDataHourly_nO2": "Float64",
                    "airQualityDataHourly_ox": "Float64",
                    "airQualityDataHourly_o3": "Float64",
                    "airQualityDataHourly_relativeHumidity": "Float64",
                    "airQualityDataHourly_pM1": "Float64",
                    "airQualityDataHourly_pM25": "Float64",
                    "airQualityDataHourly_pM10": "Float64",
                    "airQualityDataHourly_tsp": "Float64",
                    "airQualityDataHourly_time": "datetime64[ms]",
                    "airQualityDataHourly_stationId": "str",
                    "airQualityDataHourly_id": "str",
                    "station_name": "str",
                    "station_location": "str",
                    "station_latitude": "Float64",
                    "station_longitude": "Float64",
                    "station_contactName": "str",
                    "station_contactPhone": "str",
                    "station_simCardNumber": "str",
                    "station_installationEngineer": "str",
                    "station_installationDate": "datetime64[ms]",
                    "station_siteAddress": "str",
                    "station_loggerNetReference": "str",
                    "station_image": "str",
                    "station_updateBy": "str",
                    "station_updateTime": "datetime64[ms]",
                    "station_provinceId": "str",
                    "station_amphoeId": "str",
                    "station_tambonId": "str",
                    "station_code": "str",
                    "station_displayName": "str",
                    "station_remark": "str",
                    "station_stationDevice": "str",
                    "station_province_name": "str",
                    "station_province_geoCode": "str",
                    "station_province_region": "str",
                    "station_province_regionId": "str",
                    "station_province_latitude": "Float64",
                    "station_province_longitude": "Float64",
                    "station_province_nameEN": "str",
                    "station_province_isDeleted": "bool",
                    "station_province_deleterId": "str",
                    "station_province_deletionTime": "datetime64[ms]",
                    "station_province_lastModificationTime": "datetime64[ms]",
                    "station_province_lastModifierId": "str",
                    "station_province_creationTime": "datetime64[ms]",
                    "station_province_creatorId": "str",
                    "station_province_id": "str",
                    "station_amphoe": "str",
                    "station_tambon": "str",
                    "station_isDeleted": "bool",
                    "station_deleterId": "str",
                    "station_deletionTime": "datetime64[ms]",
                    "station_lastModificationTime": "datetime64[ms]",
                    "station_lastModifierId": "str",
                    "station_creationTime": "datetime64[ms]",
                    "station_creatorId": "str",
                    "station_id": "str",
                    "stationDevice_name": "str",
                    "stationDevice_serialNumber": "str",
                    "stationDevice_sensorType": "Int64",
                    "stationDevice_stationId": "str",
                    "stationDevice_simCardNumber": "str",
                    "stationDevice_installationEngineer": "str",
                    "stationDevice_installationDate": "datetime64[ms]",
                    "stationDevice_active": "bool",
                    "stationDevice_isDeleted": "bool",
                    "stationDevice_deleterId": "str",
                    "stationDevice_deletionTime": "datetime64[ms]",
                    "stationDevice_lastModificationTime": "datetime64[ms]",
                    "stationDevice_lastModifierId": "str",
                    "stationDevice_creationTime": "datetime64[ms]",
                    "stationDevice_creatorId": "str",
                    "stationDevice_id": "str",
                    "province_name": "str",
                    "province_geoCode": "str",
                    "province_region": "str",
                    "province_regionId": "str",
                    "province_latitude": "Float64",
                    "province_longitude": "Float64",
                    "province_nameEN": "str",
                    "province_isDeleted": "bool",
                    "province_deleterId": "str",
                    "province_deletionTime": "datetime64[ms]",
                    "province_lastModificationTime": "datetime64[ms]",
                    "province_lastModifierId": "str",
                    "province_creationTime": "datetime64[ms]",
                    "province_creatorId": "str",
                    "province_id": "str",
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
        import numpy as np
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

            dataframe = dataframe.rename(
                columns={
                    "stationFullname": "station_name_th",
                    "airQualityDataHourly_time": "datetime",
                    "station_longitude": "longitude",
                    "station_latitude": "latitude",
                    "airQualityDataHourly_pM25": "pm25_value",
                }
            )

            dataframe = TransformFunctions._clean_and_transform_dataframe(
                dataframe=dataframe,
                additional_columns={
                    "station_id": dataframe["stationFullname"].str.extract(r"(\w+)\s?-"),
                    "data_owner": "DPM",
                    "datetime": pd.to_datetime(dataframe["datetime"], errors="coerce"),
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
            raise AirflowFailException(e)
