-- Optional sample rows (Flink SQL workspace). Run after 01_create_hive_readings.sql.
-- Repeat or change UUIDs/timestamps to simulate ongoing telemetry.

INSERT INTO `hive_readings` VALUES
  ('11111111-1111-1111-1111-111111111101', 'HIVE-ALPHA', 'APIARY-BKK-01', 'gw-bkk-01', 42.1, 34.0, 55.0, 45, TIMESTAMP '2026-09-22 14:55:00'),
  ('11111111-1111-1111-1111-111111111102', 'HIVE-BETA',  'APIARY-BKK-01', 'gw-bkk-02', 38.7, 33.5, 52.0, 40, TIMESTAMP '2026-09-22 14:55:00'),
  ('11111111-1111-1111-1111-111111111103', 'HIVE-GAMMA', 'APIARY-CNX-02', 'gw-cnx-01', 47.9, 34.2, 58.0, 50, TIMESTAMP '2026-09-22 14:55:00');

-- Swarm-style weight drop (re-run after a few minutes with lower weights for alerts):
-- INSERT INTO `hive_readings` VALUES
--   ('22222222-2222-2222-2222-222222222201', 'HIVE-ALPHA', 'APIARY-BKK-01', 'gw-bkk-01', 28.0, 34.1, 56.0, 120, TIMESTAMP '2026-09-22 14:58:00');
