CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.vw_time_yearly_progression
  COMMENT "Serving view - time events yearly progression" AS
SELECT
  d.year                                   AS event_year,
  e.event_duration_hours,
  COUNT(*)                                 AS n_finishes,
  ROUND(AVG(f.performance_distance_km), 2) AS avg_distance_km,
  ROUND(MAX(f.performance_distance_km), 2) AS max_distance_km
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
JOIN marathos.gold.dim_date    d USING (date_id)
WHERE e.event_type = 'time'
GROUP BY d.year, e.event_duration_hours;
