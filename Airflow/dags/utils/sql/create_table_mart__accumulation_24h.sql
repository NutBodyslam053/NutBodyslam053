DECLARE now DATETIME DEFAULT DATETIME(TIMESTAMP('{{ data_interval_end }}'), 'Asia/Bangkok');

-- DDL statements to drop the table if it exists and create it anew
DROP TABLE IF EXISTS `{{ params.target_table }}`;

CREATE TABLE `{{ params.target_table }}` AS
WITH
  transaction_last_24h AS (
    SELECT
      station_info_id,
      master_datetime_utc,
      datetime_utc,
      pm25_aqi,
      pm25_value,
      pm25_color_id,
      tb_id,
      ap_id,
      pv_id,
      MAX(NULLIF(master_datetime_utc, "1990-01-01 00:00:00")) OVER (PARTITION BY station_info_id) AS max_master_datetime_utc,
      MAX(NULLIF(datetime_utc, "1990-01-01 00:00:00")) OVER (PARTITION BY station_info_id) AS max_datetime_utc
    FROM
      `{{ params.source_table }}`
  ),
  station_info AS (
    SELECT
      station_info_id,
      data_owner,
      longitude,
      latitude,
      tb_th,
      tb_en,
      ap_th,
      ap_en,
      pv_th,
      pv_en
    FROM
      `{{ params.station_info_table }}`
    WHERE
      is_current IS TRUE
  ),
  sensor_reads_now AS (
    SELECT
      tsl.station_info_id,
      tsl.master_datetime_utc,
      tsl.datetime_utc,
      sti.data_owner,
      tsl.pm25_aqi,
      tsl.pm25_value,
      tsl.pm25_color_id,
      CONCAT(sti.data_owner, "-", tsl.pm25_color_id) AS pm25_label,
      sti.longitude,
      sti.latitude,
      tsl.tb_id,
      sti.tb_th,
      sti.tb_en,
      tsl.ap_id,
      sti.ap_th,
      sti.ap_en,
      tsl.pv_id,
      sti.pv_th,
      sti.pv_en
    FROM
      transaction_last_24h AS tsl
    LEFT JOIN
      station_info AS sti USING (station_info_id)
    WHERE
      (max_datetime_utc IS NULL AND master_datetime_utc = max_master_datetime_utc)
      OR (max_datetime_utc IS NOT NULL AND datetime_utc = max_datetime_utc)
  ),
  pm25_color_id_accum_last_24h AS (
    SELECT
      station_info_id,
      SUM(CASE WHEN pm25_color_id = 0 THEN 1 ELSE 0 END) AS accum_color_0,
      SUM(CASE WHEN pm25_color_id = 1 THEN 1 ELSE 0 END) AS accum_color_1,
      SUM(CASE WHEN pm25_color_id = 2 THEN 1 ELSE 0 END) AS accum_color_2,
      SUM(CASE WHEN pm25_color_id = 3 THEN 1 ELSE 0 END) AS accum_color_3,
      SUM(CASE WHEN pm25_color_id = 4 THEN 1 ELSE 0 END) AS accum_color_4,
      SUM(CASE WHEN pm25_color_id = 5 THEN 1 ELSE 0 END) AS accum_color_5
    FROM
      transaction_last_24h
    GROUP BY
      station_info_id
  )

SELECT
  srn.station_info_id,
  DATETIME_SUB(DATETIME_TRUNC(now, HOUR), INTERVAL 7 HOUR) AS current_datetime_utc,
  srn.datetime_utc,
  srn.data_owner,
  srn.pm25_aqi,
  srn.pm25_value,
  srn.pm25_color_id,
  srn.pm25_label,
  pca.accum_color_0,
  pca.accum_color_1,
  pca.accum_color_2,
  pca.accum_color_3,
  pca.accum_color_4,
  pca.accum_color_5,
  srn.longitude,
  srn.latitude,
  srn.tb_id,
  srn.tb_th,
  srn.tb_en,
  srn.ap_id,
  srn.ap_th,
  srn.ap_en,
  srn.pv_id,
  srn.pv_th,
  srn.pv_en
FROM
  sensor_reads_now AS srn
LEFT JOIN
  pm25_color_id_accum_last_24h AS pca USING (station_info_id)
ORDER BY
  srn.data_owner,
  srn.datetime_utc;
