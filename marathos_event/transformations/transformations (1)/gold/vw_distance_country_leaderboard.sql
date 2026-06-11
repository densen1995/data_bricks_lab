CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.vw_distance_country_leaderboard
  COMMENT "Serving view - distance events leaderboard by country" AS
SELECT
  a.athlete_country,
  e.event_distance_length,
  COUNT(*)                                 AS n_finishes,
  AVG(f.athlete_average_speed)             AS avg_speed_kmh,
  MIN(f.performance_time_seconds) / 3600.0 AS fastest_finish_hours
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
JOIN marathos.gold.dim_athlete a USING (athlete_id)
WHERE e.event_type = 'distance'
GROUP BY a.athlete_country, e.event_distance_length;
