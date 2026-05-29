-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Task 5 — Gold business views
-- MAGIC
-- MAGIC Two views per marathon type, plus a couple of cross-cutting views.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ## Distance events (km / mi) views

-- COMMAND ----------

CREATE OR REPLACE VIEW marathos.gold.vw_distance_top_events AS
SELECT
  e.event_name,
  e.event_distance_length,
  e.event_distance_km,
  COUNT(*)                                 AS n_finishers_total,
  AVG(f.performance_time_seconds) / 3600.0 AS avg_finish_hours,
  MIN(f.performance_time_seconds) / 3600.0 AS fastest_finish_hours
FROM   marathos.gold.fct_results f
JOIN   marathos.gold.dim_event   e USING (event_id)
WHERE  e.event_type = 'distance'
GROUP  BY e.event_name, e.event_distance_length, e.event_distance_km
ORDER  BY n_finishers_total DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW marathos.gold.vw_distance_country_leaderboard AS
SELECT
  a.athlete_country,
  e.event_distance_length,
  COUNT(*)                                   AS n_finishes,
  AVG(f.athlete_average_speed)               AS avg_speed_kmh,
  MIN(f.performance_time_seconds) / 3600.0   AS fastest_finish_hours
FROM   marathos.gold.fct_results f
JOIN   marathos.gold.dim_event   e USING (event_id)
JOIN   marathos.gold.dim_athlete a USING (athlete_id)
WHERE  e.event_type = 'distance'
GROUP  BY a.athlete_country, e.event_distance_length
ORDER  BY n_finishes DESC;

-- COMMAND ----------

-- MAGIC %md ## Time events (h) views

-- COMMAND ----------

CREATE OR REPLACE VIEW marathos.gold.vw_time_top_events AS
SELECT
  e.event_name,
  e.event_duration_hours,
  COUNT(*)                              AS n_finishers_total,
  AVG(f.performance_distance_km)        AS avg_distance_km,
  MAX(f.performance_distance_km)        AS max_distance_km
FROM   marathos.gold.fct_results f
JOIN   marathos.gold.dim_event   e USING (event_id)
WHERE  e.event_type = 'time'
GROUP  BY e.event_name, e.event_duration_hours
ORDER  BY n_finishers_total DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW marathos.gold.vw_time_country_leaderboard AS
SELECT
  a.athlete_country,
  e.event_duration_hours,
  COUNT(*)                       AS n_finishes,
  AVG(f.performance_distance_km) AS avg_distance_km,
  MAX(f.performance_distance_km) AS max_distance_km
FROM   marathos.gold.fct_results f
JOIN   marathos.gold.dim_event   e USING (event_id)
JOIN   marathos.gold.dim_athlete a USING (athlete_id)
WHERE  e.event_type = 'time'
GROUP  BY a.athlete_country, e.event_duration_hours
ORDER  BY n_finishes DESC;

-- COMMAND ----------

-- MAGIC %md ## Cross-cutting summary view (dashboard-ready)

-- COMMAND ----------

CREATE OR REPLACE VIEW marathos.gold.vw_results_enriched AS
SELECT
  f.result_id,
  e.event_name,
  e.event_type,
  e.event_distance_length,
  e.event_distance_km,
  e.event_duration_hours,
  a.athlete_country,
  a.athlete_gender,
  d.full_date     AS event_date,
  d.year          AS event_year,
  f.athlete_age,
  f.athlete_age_category,
  f.athlete_average_speed,
  f.performance_time_seconds,
  f.performance_distance_km
FROM   marathos.gold.fct_results f
JOIN   marathos.gold.dim_event   e USING (event_id)
JOIN   marathos.gold.dim_athlete a USING (athlete_id)
LEFT JOIN marathos.gold.dim_date d  USING (date_id);
