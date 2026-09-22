-- Governance gate: append-only validated stream.
-- Step A: table DDL keeps observed_at as a time attribute (watermark).
-- Step B: continuous INSERT (run once; leave job running or completed per Console).

CREATE TABLE IF NOT EXISTS `hive_readings_validated` (
  `reading_id` STRING,
  `hive_id` STRING,
  `apiary_id` STRING,
  `gateway_id` STRING,
  `weight_kg` DOUBLE,
  `temp_c` DOUBLE,
  `humidity_pct` DOUBLE,
  `activity_count` INT,
  `observed_at` TIMESTAMP(3),
  WATERMARK FOR `observed_at` AS `observed_at` - INTERVAL '5' SECOND
)
DISTRIBUTED INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'value.format' = 'avro-registry'
);

INSERT INTO `hive_readings_validated`
SELECT
  reading_id,
  hive_id,
  apiary_id,
  gateway_id,
  weight_kg,
  temp_c,
  humidity_pct,
  activity_count,
  observed_at
FROM `hive_readings`
WHERE hive_id IS NOT NULL
  AND reading_id IS NOT NULL
  AND weight_kg BETWEEN 5 AND 150
  AND temp_c BETWEEN -5 AND 55
  AND humidity_pct BETWEEN 0 AND 100;
