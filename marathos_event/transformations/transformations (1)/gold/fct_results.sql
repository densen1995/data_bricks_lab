CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_results
  COMMENT "Fact table - one row per race result, streamed from silver" AS
SELECT
  result_id,
  event_id,
  athlete_id,
  CAST(date_format(event_start_date, 'yyyyMMdd') AS INT) AS date_id,
  performance_time_seconds,
  performance_distance_km,
  athlete_average_speed,
  athlete_age,
  athlete_age_category
FROM
  STREAM marathos.silver.races_obt;
