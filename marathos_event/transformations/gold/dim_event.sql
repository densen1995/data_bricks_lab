CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Dim event - gold layer" AS
SELECT
  event_id,
  event_name,
  MAX(event_type)                 AS event_type,
  MAX(event_distance_length)      AS event_distance_length,
  MAX(event_distance_km)          AS event_distance_km,
  MAX(event_duration_hours)       AS event_duration_hours,
  MAX(event_number_of_finishers)  AS event_number_of_finishers
FROM
  marathos.silver.races_obt
GROUP BY
  event_id, event_name;
