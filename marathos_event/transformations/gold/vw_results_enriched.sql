CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.vw_results_enriched
  COMMENT "Serving view - star schema flattened for dashboard and Genie" AS
SELECT
  f.result_id,
  e.event_name,
  e.event_type,
  e.event_distance_length,
  e.event_distance_km,
  e.event_duration_hours,
  a.athlete_country,
  a.athlete_gender,
  d.full_date AS event_date,
  d.year      AS event_year,
  f.athlete_age,
  f.athlete_age_category,
  f.athlete_average_speed,
  f.performance_time_seconds,
  f.performance_distance_km
FROM marathos.gold.fct_results f
JOIN marathos.gold.dim_event   e USING (event_id)
JOIN marathos.gold.dim_athlete a USING (athlete_id)
LEFT JOIN marathos.gold.dim_date d USING (date_id);
