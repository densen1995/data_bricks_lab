CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.vw_time_country_leaderboard
  COMMENT "Serving view - time events leaderboard by country" AS
SELECT
  a.athlete_country,
  e.event_duration_hours,
  COUNT(*)                       AS n_finishes,
  AVG(f.performance_distance_km) AS avg_distance_km,
  MAX(f.performance_distance_km) AS max_distance_km
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
JOIN marathos.gold.dim_athlete a USING (athlete_id)
WHERE e.event_type = 'time'
GROUP BY a.athlete_country, e.event_duration_hours;
