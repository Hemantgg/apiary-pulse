-- Dead-letter stream for schema-compatible but business-invalid readings.

CREATE TABLE `hive_readings_dlq`
WITH ('changelog.mode' = 'append')
AS
SELECT
  r.reading_id,
  r.hive_id,
  r.apiary_id,
  r.gateway_id,
  r.weight_kg,
  r.temp_c,
  r.humidity_pct,
  r.activity_count,
  r.observed_at,
  CASE
    WHEN r.hive_id IS NULL OR r.reading_id IS NULL THEN 'MISSING_ID'
    WHEN r.weight_kg < 5 OR r.weight_kg > 150 THEN 'WEIGHT_OUT_OF_RANGE'
    WHEN r.temp_c < -5 OR r.temp_c > 55 THEN 'TEMP_OUT_OF_RANGE'
    WHEN r.humidity_pct < 0 OR r.humidity_pct > 100 THEN 'HUMIDITY_OUT_OF_RANGE'
    ELSE 'UNKNOWN'
  END AS reject_reason
FROM `hive_readings` AS r
WHERE r.hive_id IS NULL
   OR r.reading_id IS NULL
   OR r.weight_kg NOT BETWEEN 5 AND 150
   OR r.temp_c NOT BETWEEN -5 AND 55
   OR r.humidity_pct NOT BETWEEN 0 AND 100;
