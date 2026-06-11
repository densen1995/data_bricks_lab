CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_date
  COMMENT "Dim date - gold layer (one row per distinct event date)" AS
SELECT DISTINCT
  CAST(date_format(event_start_date, 'yyyyMMdd') AS INT) AS date_id,
  event_start_date                       AS full_date,
  YEAR(event_start_date)                 AS year,
  QUARTER(event_start_date)              AS quarter,
  MONTH(event_start_date)                AS month,
  DATE_FORMAT(event_start_date, 'MMMM')  AS month_name,
  DAY(event_start_date)                  AS day,
  DAYOFWEEK(event_start_date)            AS day_of_week,
  DATE_FORMAT(event_start_date, 'EEEE')  AS day_name,
  DAYOFWEEK(event_start_date) IN (1, 7)  AS is_weekend
FROM
  marathos.silver.races_obt
WHERE
  event_start_date IS NOT NULL;
