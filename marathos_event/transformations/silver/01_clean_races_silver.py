# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3 — Silver: cleaned one-big-table (OBT)
# MAGIC
# MAGIC Cleans the bronze table and writes `marathos.silver.races_obt`.
# MAGIC
# MAGIC Cleaning rules:
# MAGIC
# MAGIC 0. Source column normalization 
# MAGIC 1. Parse `event_distance_length` → numeric value + unit (`km`, `mi`, `h`, `d`).
# MAGIC 2. Parse `athlete_performance` → numeric value + unit (`h` for time, `km` for distance).
# MAGIC 3. **Validity**:
# MAGIC    - distance events (km / mi) must have a time-format performance ("Hh:Mm:Ss h").
# MAGIC    - time events (h) must have a km-format performance.
# MAGIC    - `d` (days) events are dropped as invalid per the lab spec.
# MAGIC 4. Time strings converted to total seconds (DoubleType).
# MAGIC 5. `event_id`, `athlete_id`, `result_id` produced via `sha2()` .

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE_TABLE = "marathos.bronze.races_raw"
SILVER_TABLE = "marathos.silver.races_obt"

# Bronze already unions historical + simulated CSVs into one table, so silver
# just reads it. Per-row provenance lives in `source_file` from bronze.
df = spark.table(BRONZE_TABLE)

# Helper: cast a string column to double safely — empty strings become NULL
# instead of throwing CAST_INVALID_INPUT.
def safe_double(col):
    return F.when(col.isNull() | (col == ""), F.lit(None).cast("double")) \
            .otherwise(col.cast("double"))

# COMMAND ----------

# MAGIC %md ## 0. Source column normalization
# MAGIC
# MAGIC Empty strings → NULL, trim + uppercase country codes, fold gender to a
# MAGIC two-value vocabulary, and null out implausible numeric values so they
# MAGIC don't pollute downstream age computations or the dashboard.

# COMMAND ----------

# Treat all genders as the M/W convention (matches ultramarathon datasets +
# the stream generator). Map F -> W so the dashboard isn't split across both.
gender_clean = F.upper(F.trim(F.col("athlete_gender")))
country_clean = F.upper(F.trim(F.col("athlete_country")))
club_clean = F.trim(F.col("athlete_club"))
agecat_clean = F.trim(F.col("athlete_age_category"))
current_year = F.year(F.current_date())

df = (
    df
    .withColumn(
        "athlete_country",
        F.when(country_clean == "", F.lit(None).cast("string")).otherwise(country_clean),
    )
    .withColumn(
        "athlete_gender",
        F.when(gender_clean == "M", F.lit("M"))
         .when(gender_clean.isin("W", "F"), F.lit("W")),
    )
    .withColumn(
        "athlete_club",
        F.when(club_clean == "", F.lit(None).cast("string")).otherwise(club_clean),
    )
    .withColumn(
        "athlete_age_category",
        F.when(agecat_clean == "", F.lit(None).cast("string")).otherwise(agecat_clean),
    )
    .withColumn(
        "athlete_year_of_birth",
        F.when(
            F.col("athlete_year_of_birth").between(1850, current_year - 5),
            F.col("athlete_year_of_birth"),
        ),
    )
    .withColumn(
        "athlete_average_speed",
        F.when(
            (F.col("athlete_average_speed") > 0) & (F.col("athlete_average_speed") <= 50),
            F.col("athlete_average_speed"),
        ),
    )
)

# COMMAND ----------

# MAGIC %md ## 1. Parse event distance/length
# MAGIC `event_distance_length` is like "50km", "100mi", "24h", "6d".

# COMMAND ----------

df = (
    df
    .withColumn(
        "event_distance_value",
        safe_double(F.regexp_extract("event_distance_length", r"([0-9]*\.?[0-9]+)", 1)),
    )
    .withColumn(
        "event_unit",
        F.lower(F.regexp_extract("event_distance_length", r"([a-zA-Z]+)$", 1)),
    )
)

# COMMAND ----------

# MAGIC %md ## 2. Parse athlete performance
# MAGIC
# MAGIC Performance strings:
# MAGIC - distance events: `"4:51:39 h"` (or `"H:MM:SS h"`, sometimes `"D HH:MM:SS h"` for multi-day finishes)
# MAGIC - time events: `"123.456 km"` or `"123.4km"`

# COMMAND ----------

perf_clean = F.trim("athlete_performance")

df = (
    df
    .withColumn(
        "performance_unit",
        F.when(perf_clean.rlike(r"(?i)\s*h\s*$"), F.lit("h"))
         .when(perf_clean.rlike(r"(?i)km\s*$"), F.lit("km"))
         .otherwise(F.lit(None).cast("string")),
    )
    # distance: extract leading number from "123.45 km"
    .withColumn(
        "performance_distance_km",
        F.when(
            F.col("performance_unit") == "km",
            safe_double(F.regexp_extract(perf_clean, r"([0-9]*\.?[0-9]+)", 1)),
        ),
    )
    # time: extract H:M:S (and optional leading days) from "4:51:39 h" or "1 04:30:00 h"
    .withColumn(
        "_perf_days_str",
        F.when(
            F.col("performance_unit") == "h",
            F.regexp_extract(perf_clean, r"^(\d+)\s+\d+:\d+:\d+", 1),
        ),
    )
    .withColumn(
        "_perf_hms",
        F.when(
            F.col("performance_unit") == "h",
            F.regexp_extract(perf_clean, r"(\d+:\d+:\d+)\s*h", 1),
        ),
    )
)

df = df.withColumn(
    "performance_time_seconds",
    F.when(
        (F.col("performance_unit") == "h") & (F.col("_perf_hms") != ""),
        (F.coalesce(safe_double(F.col("_perf_days_str")), F.lit(0.0)) * 86400)
        + safe_double(F.split("_perf_hms", ":")[0]) * 3600
        + safe_double(F.split("_perf_hms", ":")[1]) * 60
        + safe_double(F.split("_perf_hms", ":")[2]),
    ),
).drop("_perf_days_str", "_perf_hms")

# COMMAND ----------

# MAGIC %md ## 3. Validity check

# COMMAND ----------

df = df.withColumn(
    "is_valid",
    (
        # distance events must have time performance
        ((F.col("event_unit").isin("km", "mi")) & (F.col("performance_unit") == "h") & F.col("performance_time_seconds").isNotNull())
        # time events must have distance performance
        | ((F.col("event_unit") == "h") & (F.col("performance_unit") == "km") & F.col("performance_distance_km").isNotNull())
    ),
)

invalid_count = df.filter(~F.col("is_valid")).count()
total_count = df.count()
print(f"invalid rows dropped: {invalid_count:,} / {total_count:,} ({invalid_count / total_count:.2%})")

df = df.filter(F.col("is_valid")).drop("is_valid")

# COMMAND ----------

# MAGIC %md ## 4. Convert event distance to km
# MAGIC Standardises distance for downstream comparisons (1 mi ≈ 1.609344 km).

# COMMAND ----------

MI_TO_KM = 1.609344
df = (
    df
    .withColumn(
        "event_distance_km",
        F.when(F.col("event_unit") == "km", F.col("event_distance_value"))
         .when(F.col("event_unit") == "mi", F.col("event_distance_value") * MI_TO_KM)
         .otherwise(F.lit(None).cast("double")),
    )
    .withColumn(
        "event_duration_hours",
        F.when(F.col("event_unit") == "h", F.col("event_distance_value")).otherwise(F.lit(None).cast("double")),
    )
    .withColumn(
        "event_type",
        F.when(F.col("event_unit").isin("km", "mi"), F.lit("distance"))
         .when(F.col("event_unit") == "h", F.lit("time"))
         .otherwise(F.lit(None).cast("string")),
    )
)

# COMMAND ----------

# MAGIC %md ## 5. Athlete age + parse event date

# COMMAND ----------

df = (
    df
    .withColumn(
        "_raw_age",
        (F.col("year_of_event") - F.col("athlete_year_of_birth")).cast("int"),
    )
    .withColumn(
        "athlete_age",
        F.when(F.col("_raw_age").between(10, 100), F.col("_raw_age")),
    )
    .drop("_raw_age")
    .withColumn(
        "_event_date_str",
        F.regexp_extract("event_dates", r"^(\d{2}\.\d{2}\.\d{4})", 1),
    )
    .withColumn(
        "event_start_date",
        F.expr("try_to_date(nullif(_event_date_str, ''), 'dd.MM.yyyy')"),
    )
    .drop("_event_date_str")
)

# COMMAND ----------

# MAGIC %md ## 6. Surrogate IDs via sha2 (streaming-safe, collision-resistant)
# MAGIC
# MAGIC  We use `sha2(..., 256)` : a deterministic
# MAGIC per-row hash that returns a 256-bit hex string. Same logic works in batch
# MAGIC *and* a streaming silver pipeline, IDs are stable across re-runs, and the
# MAGIC hash space is so large that collisions are effectively impossible
# MAGIC (xxhash64 produced ~250 collisions on this dataset; sha2 produces zero).

# COMMAND ----------

df = (
    df
    .withColumn(
        "event_id",
        F.sha2(F.coalesce(F.col("event_name"), F.lit("")), 256),
    )
    .withColumn(
        "athlete_id",
        F.sha2(
            F.coalesce(F.col("athlete_source_id").cast("string"), F.lit("")),
            256,
        ),
    )
    .withColumn(
        "result_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.coalesce(F.col("event_name"), F.lit("")),
                F.coalesce(F.col("year_of_event").cast("string"), F.lit("")),
                F.coalesce(F.col("athlete_source_id").cast("string"), F.lit("")),
                F.coalesce(F.col("athlete_performance"), F.lit("")),
            ),
            256,
        ),
    )
)

# COMMAND ----------

# MAGIC %md ## 7. Final select + write

# COMMAND ----------

silver = df.select(
    "result_id",
    "event_id",
    "athlete_id",
    "year_of_event",
    "event_start_date",
    "event_name",
    "event_distance_length",
    "event_type",
    "event_distance_km",
    "event_duration_hours",
    "event_number_of_finishers",
    "athlete_source_id",
    "athlete_country",
    "athlete_club",
    "athlete_gender",
    "athlete_year_of_birth",
    "athlete_age",
    "athlete_age_category",
    "athlete_average_speed",
    "performance_time_seconds",
    "performance_distance_km",
)

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"wrote {SILVER_TABLE}: {spark.table(SILVER_TABLE).count():,} rows")
silver.limit(10).display()
