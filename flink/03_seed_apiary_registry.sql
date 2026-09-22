-- One-time seed for three demo hives (idempotent if you truncate the topic first).

INSERT INTO `apiary_registry` VALUES
  ('HIVE-ALPHA', 'APIARY-BKK-01', 'Lumpini Rooftop Apiary', 'TH-BKK', 42.5, TIMESTAMP_LTZ '2026-03-20 08:00:00'),
  ('HIVE-BETA',  'APIARY-BKK-01', 'Lumpini Rooftop Apiary', 'TH-BKK', 39.0, TIMESTAMP_LTZ '2026-03-20 08:00:00'),
  ('HIVE-GAMMA', 'APIARY-CNX-02', 'Chiang Mai Highland Co-op', 'TH-CNX', 48.2, TIMESTAMP_LTZ '2026-03-20 08:00:00');
