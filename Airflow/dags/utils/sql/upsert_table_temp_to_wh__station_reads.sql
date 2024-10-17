-- DDL statements to create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{ params.target_table }}` (
  station_info_id STRING,
  ingest_datetime DATETIME,
  datetime DATETIME,
  station_id STRING,
  pm25_aqi INT64,
  pm25_value FLOAT64,
  pm25_color_id INT64,
  FOREIGN KEY (station_info_id) REFERENCES `{{ params.station_info_table }}` (station_info_id) NOT ENFORCED
)
PARTITION BY DATETIME_TRUNC(ingest_datetime, DAY)
CLUSTER BY station_info_id
OPTIONS (
  partition_expiration_days = NULL,
  require_partition_filter = TRUE,
  labels = [
    ('category', 'pm25'), ('zone', 'wh'), ('department', 'dit')
  ]
);

-- DML statements to upsert data
BEGIN TRANSACTION;

MERGE `{{ params.target_table }}` AS TARGET
USING (
  SELECT
    r.station_info_id,
    s.ingest_datetime,
    s.datetime,
    s.station_id,
    s.pm25_aqi,
    s.pm25_value,
    s.pm25_color_id
  FROM
    `{{ params.source_table }}` as s
  LEFT JOIN
    `{{ params.station_info_table }}` as r
  ON
    s.data_owner = r.data_owner
    AND s.station_id = r.station_id
    AND r.is_current = TRUE
  WHERE
    DATE(s.ingest_datetime) = DATE('{{ data_interval_end }}', 'Asia/Bangkok')
) AS SOURCE
ON
  TARGET.ingest_datetime = SOURCE.ingest_datetime
  AND TARGET.datetime = SOURCE.datetime
  AND TARGET.station_id = SOURCE.station_id
WHEN NOT MATCHED THEN
INSERT (
  station_info_id,
  ingest_datetime,
  datetime,
  station_id,
  pm25_aqi,
  pm25_value,
  pm25_color_id
)
VALUES (
  SOURCE.station_info_id,
  SOURCE.ingest_datetime,
  SOURCE.datetime,
  SOURCE.station_id,
  SOURCE.pm25_aqi,
  SOURCE.pm25_value,
  SOURCE.pm25_color_id
);

COMMIT TRANSACTION;

-- DDL statements to drop the source table
-- DROP TABLE `{{ params.source_table }}`;