-- Rule-based alerts: tumble on `hive_readings` (rowtime + watermark).
-- `hive_readings_validated` from CTAS loses rowtime; this reads the source stream.

CREATE TABLE `hive_alerts`
WITH ('changelog.mode' = 'append')
AS
WITH windowed AS (
  SELECT
    hive_id,
    apiary_id,
    window_start,
    window_end,
    window_time,
    AVG(weight_kg) AS avg_weight_kg,
    MAX(weight_kg) AS max_weight_kg,
    MIN(weight_kg) AS min_weight_kg,
    MAX(weight_kg) - MIN(weight_kg) AS weight_swing_kg,
    AVG(temp_c) AS avg_temp_c,
    AVG(humidity_pct) AS avg_humidity_pct,
    MAX(activity_count) AS peak_activity
  FROM TABLE(
    TUMBLE(
      TABLE `hive_readings`,
      DESCRIPTOR(observed_at),
      INTERVAL '3' MINUTE
    )
  )
  WHERE hive_id IS NOT NULL
    AND reading_id IS NOT NULL
    AND weight_kg BETWEEN 5 AND 150
    AND temp_c BETWEEN -5 AND 55
    AND humidity_pct BETWEEN 0 AND 100
  GROUP BY hive_id, apiary_id, window_start, window_end, window_time
)
SELECT
  MD5(CONCAT(hive_id, CAST(window_end AS STRING), alert_type)) AS alert_id,
  hive_id,
  apiary_id,
  alert_type,
  severity,
  window_end AS triggered_at,
  summary,
  weight_delta_kg,
  CAST(3 AS INT) AS window_minutes
FROM (
  SELECT
    hive_id,
    apiary_id,
    window_end,
    weight_swing_kg AS weight_delta_kg,
    'SWARM_OR_THEFT_SUSPECT' AS alert_type,
    'CRITICAL' AS severity,
    CONCAT(
      apiary_id, ' / ', hive_id,
      ': rapid weight swing ', CAST(weight_swing_kg AS STRING),
      ' kg in 3 minutes — inspect for swarm or hive theft.'
    ) AS summary
  FROM windowed
  WHERE weight_swing_kg >= 10

  UNION ALL

  SELECT
    hive_id,
    apiary_id,
    window_end,
    CAST(NULL AS DOUBLE) AS weight_delta_kg,
    'BROOD_TEMP_STRESS' AS alert_type,
    'WARN' AS severity,
    CONCAT(
      apiary_id, ' / ', hive_id,
      ': brood nest temperature avg ', CAST(avg_temp_c AS STRING),
      ' °C outside 32–36 °C band.'
    ) AS summary
  FROM windowed
  WHERE avg_temp_c < 32 OR avg_temp_c > 36

  UNION ALL

  SELECT
    hive_id,
    apiary_id,
    window_end,
    CAST(NULL AS DOUBLE) AS weight_delta_kg,
    'HUMIDITY_STRESS' AS alert_type,
    'WARN' AS severity,
    CONCAT(
      apiary_id, ' / ', hive_id,
      ': humidity avg ', CAST(avg_humidity_pct AS STRING),
      '% outside 40–70% band.'
    ) AS summary
  FROM windowed
  WHERE avg_humidity_pct < 40 OR avg_humidity_pct > 70
);
