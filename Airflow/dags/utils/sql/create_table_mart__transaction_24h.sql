DECLARE now DATETIME DEFAULT DATETIME(TIMESTAMP('{{ data_interval_end }}'), 'Asia/Bangkok');

-- DDL statements to drop the table if it exists and create it anew
DROP TABLE IF EXISTS `{{ params.target_table }}`;

CREATE TABLE `{{ params.target_table }}` AS
WITH
  station_reads AS (
    SELECT
      station_info_id,
      ingest_datetime,
      datetime,
      pm25_aqi,
      pm25_value,
      pm25_color_id,
      ROW_NUMBER() OVER (PARTITION BY station_info_id, datetime ORDER BY ingest_datetime DESC) AS row_num
    FROM
      `{{ params.station_reads_table }}`
    WHERE
      ingest_datetime > DATETIME_SUB(now, INTERVAL 24 HOUR)
      AND datetime > DATETIME_SUB(now, INTERVAL 24 HOUR)
  ),
  station_info AS (
    SELECT
      station_info_id,
      data_owner,
      longitude,
      latitude,
      tb_id,
      ap_id,
      pv_id
    FROM
      `{{ params.station_info_table }}`
    WHERE
      is_current IS TRUE
  ),
  master_timetable_last_24h AS (
    SELECT
      DATETIME_SUB(DATETIME_TRUNC(now, HOUR), INTERVAL seq HOUR) AS master_datetime
    FROM
      UNNEST(GENERATE_ARRAY(0, 23)) AS seq
  ),
  station_info_last_24h AS (
    SELECT
      station_info_id,
      master_datetime,
      data_owner,
      longitude,
      latitude,
      tb_id,
      ap_id,
      pv_id
    FROM
      station_info
    CROSS JOIN
      master_timetable_last_24h
  )

SELECT
  sti.station_info_id,
  DATETIME_SUB(sti.master_datetime, INTERVAL 7 HOUR) AS master_datetime_utc,
  COALESCE(DATETIME_SUB(str.datetime, INTERVAL 7 HOUR), DATETIME("1990-01-01 00:00:00")) AS datetime_utc,
  sti.data_owner,
  COALESCE(str.pm25_aqi, -999) AS pm25_aqi,
  COALESCE(ROUND(str.pm25_value, 1), -999) AS pm25_value,
  COALESCE(str.pm25_color_id, 0) AS pm25_color_id,
  sti.longitude,
  sti.latitude,
  sti.tb_id,
  sti.ap_id,
  sti.pv_id
FROM
  station_info_last_24h AS sti
LEFT JOIN
  station_reads AS str
ON sti.station_info_id = str.station_info_id
  AND sti.master_datetime = str.datetime
  AND str.row_num = 1  -- เลือกแถวแรกสุดของแต่ละ station_info_id
ORDER BY  -- เพื่อจัดเรียงข้อมูลให้อ่านเร็วขึ้น
  sti.data_owner,
  sti.station_info_id,
  sti.master_datetime;
