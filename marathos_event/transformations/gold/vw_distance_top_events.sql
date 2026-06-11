CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.vw_distance_top_events
  COMMENT "Serving view - distance events ranked by finisher count" AS
SELECT
  e.event_name,
  e.event_distance_length,
  e.event_distance_km,
  COUNT(*)                                 AS n_finishers_total,
  AVG(f.performance_time_seconds) / 3600.0 AS avg_finish_hours,
  MIN(f.performance_time_seconds) / 3600.0 AS fastest_finish_hours
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
WHERE e.event_type = 'distance'
GROUP BY e.event_name, e.event_distance_length, e.event_distance_km;
