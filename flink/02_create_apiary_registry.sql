-- Dimension table for hive metadata (upsert by hive_id).

CREATE TABLE IF NOT EXISTS `apiary_registry` (
  `hive_id` STRING,
  `apiary_id` STRING,
  `apiary_name` STRING,
  `region` STRING,
  `baseline_weight_kg` DOUBLE,
  `updated_at` TIMESTAMP_LTZ(3),
  WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '1' DAY,
  PRIMARY KEY (`hive_id`) NOT ENFORCED
)
DISTRIBUTED BY (`hive_id`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'connector' = 'confluent',
  'value.format' = 'avro-registry'
);
