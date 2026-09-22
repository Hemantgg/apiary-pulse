-- Source stream: edge gateways publish governed Avro readings here.
-- Run in Confluent Cloud Flink SQL (catalog = environment, database = cluster).

CREATE TABLE IF NOT EXISTS `hive_readings` (
  `reading_id` STRING,
  `hive_id` STRING,
  `apiary_id` STRING,
  `gateway_id` STRING,
  `weight_kg` DOUBLE,
  `temp_c` DOUBLE,
  `humidity_pct` DOUBLE,
  `activity_count` INT,
  `observed_at` TIMESTAMP_LTZ(3),
  WATERMARK FOR `observed_at` AS `observed_at` - INTERVAL '5' SECOND
)
DISTRIBUTED INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'value.format' = 'avro-registry'
);
