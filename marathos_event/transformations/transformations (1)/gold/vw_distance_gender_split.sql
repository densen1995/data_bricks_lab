CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.vw_distance_gender_split
  COMMENT "Serving view - distance events split by gender" AS
SELECT
  e.event_distance_length,
  e.event_distance_km,
  a.athlete_gender,
  COUNT(*)                                           AS n_finishes,
  ROUND(AVG(f.athlete_average_speed), 3)             AS avg_speed_kmh,
  ROUND(AVG(f.performance_time_seconds) / 3600.0, 2) AS avg_finish_hours,
  ROUND(MIN(f.performance_time_seconds) / 3600.0, 2) AS fastest_finish_hours
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
JOIN marathos.gold.dim_athlete a USING (athlete_id)
WHERE e.event_type = 'distance'
  AND a.athlete_gender IN ('M', 'W')
GROUP BY e.event_distance_length, e.event_distance_km, a.athlete_gender;
