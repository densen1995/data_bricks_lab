# Databricks notebook source
# MAGIC %md
# MAGIC # Task 5 — Gold: dimensional model build
# MAGIC
# MAGIC Reads `marathos.silver.races_obt` and materialises the star schema:
# MAGIC `dim_event`, `dim_athlete`, `dim_date`, `fct_results`.

# COMMAND ----------

from pyspark.sql import functions as F

SILVER = "marathos.silver.races_obt"
silver = spark.table(SILVER)

# COMMAND ----------

# MAGIC %md ## dim_event

# COMMAND ----------

dim_event = (
    silver
    .groupBy("event_id", "event_name")
    .agg(
        F.first("event_type").alias("event_type"),
        F.first("event_distance_length").alias("event_distance_length"),
        F.max("event_distance_km").alias("event_distance_km"),
        F.max("event_duration_hours").alias("event_duration_hours"),
        F.max("event_number_of_finishers").alias("event_number_of_finishers"),
    )
)

dim_event.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("marathos.gold.dim_event")
print(f"dim_event: {spark.table('marathos.gold.dim_event').count():,} rows")

# COMMAND ----------

# MAGIC %md ## dim_athlete

# COMMAND ----------

dim_athlete = (
    silver
    .groupBy("athlete_id", "athlete_source_id")
    .agg(
        F.first("athlete_country").alias("athlete_country"),
        F.first("athlete_club").alias("athlete_club"),
        F.first("athlete_gender").alias("athlete_gender"),
        F.first("athlete_year_of_birth").alias("athlete_year_of_birth"),
    )
)

dim_athlete.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("marathos.gold.dim_athlete")
print(f"dim_athlete: {spark.table('marathos.gold.dim_athlete').count():,} rows")

# COMMAND ----------

# MAGIC %md ## dim_date — BONUS
# MAGIC Built from the distinct event dates present in the data.

# COMMAND ----------

dim_date = (
    silver
    .select("event_start_date")
    .where(F.col("event_start_date").isNotNull())
    .distinct()
    .withColumnRenamed("event_start_date", "full_date")
    .withColumn(
        "date_id",
        (F.year("full_date") * 10000 + F.month("full_date") * 100 + F.dayofmonth("full_date")).cast("int"),
    )
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
)

dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("marathos.gold.dim_date")
print(f"dim_date: {spark.table('marathos.gold.dim_date').count():,} rows")

# COMMAND ----------

# MAGIC %md ## fct_results

# COMMAND ----------

fct_results = (
    silver
    .withColumn(
        "date_id",
        (F.year("event_start_date") * 10000 + F.month("event_start_date") * 100 + F.dayofmonth("event_start_date")).cast("int"),
    )
    .select(
        "result_id",
        "event_id",
        "athlete_id",
        "date_id",
        "performance_time_seconds",
        "performance_distance_km",
        "athlete_average_speed",
        "athlete_age",
        "athlete_age_category",
    )
)

fct_results.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("marathos.gold.fct_results")
print(f"fct_results: {spark.table('marathos.gold.fct_results').count():,} rows")
