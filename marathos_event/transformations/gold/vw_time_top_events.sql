CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.vw_time_top_events
  COMMENT "Serving view - time events ranked by finisher count" AS
SELECT
  e.event_name,
  e.event_duration_hours,
  COUNT(*)                       AS n_finishers_total,
  AVG(f.performance_distance_km) AS avg_distance_km,
  MAX(f.performance_distance_km) AS max_distance_km
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
WHERE e.event_type = 'time'
GROUP BY e.event_name, e.event_duration_hours;
