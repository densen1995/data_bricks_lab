# Databricks notebook source
# MAGIC %md
# MAGIC # Task 7 — Genie answer validation
# MAGIC
# MAGIC Before handing Marathos Genie to business stakeholders, we cross-check a few
# MAGIC of its recommended-question answers against ground-truth SQL/PySpark on the
# MAGIC gold layer. Each cell below answers one expected Genie question manually;
# MAGIC compare these numbers to what Genie returns in its space.

# COMMAND ----------

from pyspark.sql import functions as F

results_view  = "marathos.gold.vw_results_enriched"
dim_event     = "marathos.gold.dim_event"
dim_athlete   = "marathos.gold.dim_athlete"
fct_results   = "marathos.gold.fct_results"

# COMMAND ----------

# MAGIC %md ## Q1. How many race finishes are recorded per year?

# COMMAND ----------

spark.sql(f"""
    SELECT event_year, COUNT(*) AS n_finishes
    FROM   {results_view}
    GROUP  BY event_year
    ORDER  BY event_year
""").display()

# COMMAND ----------

# MAGIC %md ## Q2. Which 10 events have the most finishers all-time?

# COMMAND ----------

spark.sql(f"""
    SELECT event_name, COUNT(*) AS n_finishes
    FROM   {results_view}
    GROUP  BY event_name
    ORDER  BY n_finishes DESC
    LIMIT  10
""").display()

# COMMAND ----------

# MAGIC %md ## Q3. Fastest 100km finish in the dataset

# COMMAND ----------

spark.sql(f"""
    SELECT event_name,
           event_date,
           athlete_country,
           athlete_gender,
           athlete_age,
           performance_time_seconds / 3600.0 AS finish_hours
    FROM   {results_view}
    WHERE  event_distance_length = '100km'
      AND  performance_time_seconds IS NOT NULL
    ORDER  BY performance_time_seconds ASC
    LIMIT  10
""").display()

# COMMAND ----------

# MAGIC %md ## Q4. Average finish time per distance bucket (km / mi events)

# COMMAND ----------

spark.sql(f"""
    SELECT event_distance_length,
           COUNT(*) AS n_finishes,
           AVG(performance_time_seconds) / 3600.0 AS avg_finish_hours,
           AVG(athlete_average_speed)             AS avg_speed_kmh
    FROM   {results_view}
    WHERE  event_type = 'distance'
    GROUP  BY event_distance_length
    ORDER  BY n_finishes DESC
    LIMIT  20
""").display()

# COMMAND ----------

# MAGIC %md ## Q5. Top countries — number of finishes and average pace

# COMMAND ----------

spark.sql(f"""
    SELECT athlete_country,
           COUNT(*) AS n_finishes,
           AVG(athlete_average_speed) AS avg_speed_kmh
    FROM   {results_view}
    GROUP  BY athlete_country
    ORDER  BY n_finishes DESC
    LIMIT  15
""").display()

# COMMAND ----------

# MAGIC %md ## Q6. Gender split of finishers per year

# COMMAND ----------

spark.sql(f"""
    SELECT event_year,
           athlete_gender,
           COUNT(*) AS n_finishes
    FROM   {results_view}
    WHERE  athlete_gender IS NOT NULL
    GROUP  BY event_year, athlete_gender
    ORDER  BY event_year, athlete_gender
""").display()

# COMMAND ----------

# MAGIC %md ## Q7. Age distribution of finishers (overall histogram)

# COMMAND ----------

spark.sql(f"""
    SELECT  FLOOR(athlete_age / 5) * 5 AS age_bucket_start,
            COUNT(*) AS n_finishes
    FROM    {results_view}
    WHERE   athlete_age BETWEEN 10 AND 100
    GROUP   BY age_bucket_start
    ORDER   BY age_bucket_start
""").display()

# COMMAND ----------

# MAGIC %md ## Q8. For 24h events — average distance covered, by year

# COMMAND ----------

spark.sql(f"""
    SELECT event_year,
           AVG(performance_distance_km) AS avg_distance_km,
           MAX(performance_distance_km) AS max_distance_km,
           COUNT(*)                     AS n_finishes
    FROM   {results_view}
    WHERE  event_distance_length = '24h'
    GROUP  BY event_year
    ORDER  BY event_year
""").display()

# COMMAND ----------

# MAGIC %md ## Validation summary
# MAGIC
# MAGIC Manually verify Genie's recommended question outputs by:
# MAGIC 1. Opening the **Marathos Genie** space.
# MAGIC 2. Clicking each recommended question (which should map to Q1-Q8 above).
# MAGIC 3. Comparing the table/chart Genie returns to the result of the matching cell here.
# MAGIC
# MAGIC If numbers diverge, check Genie's generated SQL → likely cause is a missing
# MAGIC table-comment or column-comment so Genie misinterprets a field. Add the
# MAGIC instruction or column comment via `ALTER TABLE ... ALTER COLUMN ... COMMENT '...'`.
