DECLARE today DATE DEFAULT DATE('{{ data_interval_end }}', 'Asia/Bangkok');

-- DDL statements to create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{ params.target_table }}` (
  station_info_id STRING PRIMARY KEY NOT ENFORCED,
  data_owner STRING,
  station_id STRING,
  station_name_th STRING,
  longitude FLOAT64,
  latitude FLOAT64,
  tb_id STRING,
  tb_th STRING,
  tb_en STRING,
  ap_id STRING,
  ap_th STRING,
  ap_en STRING,
  pv_id STRING,
  pv_th STRING,
  pv_en STRING,
  start_date DATE,
  end_date DATE,
  is_current BOOLEAN
)
CLUSTER BY data_owner
OPTIONS (
  labels = [
    ('category', 'pm25'), ('zone', 'wh'), ('department', 'dit')
  ]
);

-- DML statements to upsert data applying SCD (Type 2)
BEGIN TRANSACTION;

CREATE TEMPORARY TABLE source_data AS (
  SELECT
    GENERATE_UUID () AS station_info_id,
    s.data_owner,
    s.station_id,
    s.station_name_th,
    s.longitude,
    s.latitude,
    g.TB_IDN AS tb_id,
    g.TB_TN AS tb_th,
    g.TB_EN AS tb_en,
    g.AP_IDN AS ap_id,
    g.AP_TN AS ap_th,
    g.AP_EN AS ap_en,
    g.PV_IDN AS pv_id,
    g.PV_TN AS pv_th,
    g.PV_EN AS pv_en
  FROM
    `{{ params.source_table }}` AS s
  LEFT JOIN
    `{{ params.geog_table }}` AS g
  ON
    ST_CONTAINS(g.geometry, ST_GEOGPOINT(s.longitude, s.latitude))
);

MERGE `{{ params.target_table }}` AS TARGET
USING source_data AS SOURCE 
ON TARGET.data_owner = SOURCE.data_owner
AND TARGET.station_id = SOURCE.station_id
AND TARGET.is_current = TRUE
WHEN MATCHED AND (
  TARGET.station_name_th != SOURCE.station_name_th
  OR TARGET.longitude != SOURCE.longitude
  OR TARGET.latitude != SOURCE.latitude
  OR TARGET.tb_id != SOURCE.tb_id
  OR TARGET.tb_th != SOURCE.tb_th
  OR TARGET.tb_en != SOURCE.tb_en
  OR TARGET.ap_id != SOURCE.ap_id
  OR TARGET.ap_th != SOURCE.ap_th
  OR TARGET.ap_en != SOURCE.ap_en
  OR TARGET.pv_id != SOURCE.pv_id
  OR TARGET.pv_th != SOURCE.pv_th
  OR TARGET.pv_en != SOURCE.pv_en
) THEN
UPDATE SET
  TARGET.end_date = today,
  TARGET.is_current = FALSE
WHEN NOT MATCHED THEN
INSERT (
  station_info_id,
  data_owner,
  station_id,
  station_name_th,
  longitude,
  latitude,
  tb_id,
  tb_th,
  tb_en,
  ap_id,
  ap_th,
  ap_en,
  pv_id,
  pv_th,
  pv_en,
  start_date,
  end_date,
  is_current
)
VALUES (
  SOURCE.station_info_id,
  SOURCE.data_owner,
  SOURCE.station_id,
  SOURCE.station_name_th,
  SOURCE.longitude,
  SOURCE.latitude,
  SOURCE.tb_id,
  SOURCE.tb_th,
  SOURCE.tb_en,
  SOURCE.ap_id,
  SOURCE.ap_th,
  SOURCE.ap_en,
  SOURCE.pv_id,
  SOURCE.pv_th,
  SOURCE.pv_en,
  today,
  NULL,
  TRUE
);

INSERT INTO `{{ params.target_table }}` (
  station_info_id,
  data_owner,
  station_id,
  station_name_th,
  longitude,
  latitude,
  tb_id,
  tb_th,
  tb_en,
  ap_id,
  ap_th,
  ap_en,
  pv_id,
  pv_th,
  pv_en,
  start_date,
  end_date,
  is_current
)
SELECT
  GENERATE_UUID (),
  SOURCE.data_owner,
  SOURCE.station_id,
  SOURCE.station_name_th,
  SOURCE.longitude,
  SOURCE.latitude,
  SOURCE.tb_id,
  SOURCE.tb_th,
  SOURCE.tb_en,
  SOURCE.ap_id,
  SOURCE.ap_th,
  SOURCE.ap_en,
  SOURCE.pv_id,
  SOURCE.pv_th,
  SOURCE.pv_en,
  today,
  NULL,
  TRUE
FROM
  source_data AS SOURCE
LEFT JOIN
  `{{ params.target_table }}` AS TARGET
ON
  SOURCE.data_owner = TARGET.data_owner
  AND SOURCE.station_id = TARGET.station_id
  AND TARGET.is_current = TRUE
WHERE
  TARGET.data_owner IS NULL
  OR (
    TARGET.station_name_th != SOURCE.station_name_th
    OR TARGET.longitude != SOURCE.longitude
    OR TARGET.latitude != SOURCE.latitude
    OR TARGET.tb_id != SOURCE.tb_id
    OR TARGET.tb_th != SOURCE.tb_th
    OR TARGET.tb_en != SOURCE.tb_en
    OR TARGET.ap_id != SOURCE.ap_id
    OR TARGET.ap_th != SOURCE.ap_th
    OR TARGET.ap_en != SOURCE.ap_en
    OR TARGET.pv_id != SOURCE.pv_id
    OR TARGET.pv_th != SOURCE.pv_th
    OR TARGET.pv_en != SOURCE.pv_en
  );

COMMIT TRANSACTION;

-- DDL statements to drop the source table
-- DROP TABLE `{{ params.source_table }}`;