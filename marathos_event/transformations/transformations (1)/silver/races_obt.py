from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils.utils import safe_double

# Cleaning rules:
#   1. Parse event_distance_length  -> numeric value + unit (km / mi / h / d).
#   2. Parse athlete_performance    -> numeric value + unit (h for time, km for distance).
#   3. Validity:
#        - distance events (km / mi) must have a time-format performance ("H:MM:SS h").
#        - time events (h)           must have a km-format performance.
#        - d (days) events are dropped as invalid per the lab spec.
#   4. Time strings -> total seconds (double).
#   5. event_id / athlete_id / result_id via sha2 (streaming-safe, collision-resistant) —
#      window functions like dense_rank/row_number are forbidden on streaming DataFrames.

MI_TO_KM = 1.609344


@dp.table(
    name="marathos.silver.races_obt",
    comment="Cleaned one-big-table: parsed distances and performances, validity-filtered, surrogate keys.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def races_obt():
    df = spark.readStream.table("marathos.bronze.races_raw")

    # 0. Source column normalization -------------------------------------------
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
        # Fold gender to the M/W convention (map F -> W) used by the dataset and the generator.
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

    # 1. Parse event distance/length ("50km", "100mi", "24h", "6d") -------------
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

    # 2. Parse athlete performance ---------------------------------------------
    #    distance events: "4:51:39 h" (or "D HH:MM:SS h" for multi-day finishes)
    #    time events:     "123.456 km"
    perf_clean = F.trim("athlete_performance")
    df = (
        df
        .withColumn(
            "performance_unit",
            F.when(perf_clean.rlike(r"(?i)\s*h\s*$"), F.lit("h"))
             .when(perf_clean.rlike(r"(?i)km\s*$"), F.lit("km"))
             .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "performance_distance_km",
            F.when(
                F.col("performance_unit") == "km",
                safe_double(F.regexp_extract(perf_clean, r"([0-9]*\.?[0-9]+)", 1)),
            ),
        )
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

    # 3. Validity filter --------------------------------------------------------
    df = df.withColumn(
        "is_valid",
        (
            ((F.col("event_unit").isin("km", "mi")) & (F.col("performance_unit") == "h") & F.col("performance_time_seconds").isNotNull())
            | ((F.col("event_unit") == "h") & (F.col("performance_unit") == "km") & F.col("performance_distance_km").isNotNull())
        ),
    )
    df = df.filter(F.col("is_valid")).drop("is_valid")

    # 4. Standardise distance to km + classify event type ----------------------
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

    # 5. Athlete age + parse event date ----------------------------------------
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

    # 6. Surrogate keys via sha2 (deterministic, stable across re-runs) ----------
    df = (
        df
        .withColumn(
            "event_id",
            F.sha2(F.coalesce(F.col("event_name"), F.lit("")), 256),
        )
        .withColumn(
            "athlete_id",
            F.sha2(F.coalesce(F.col("athlete_source_id").cast("string"), F.lit("")), 256),
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

    # 7. Final projection -------------------------------------------------------
    return df.select(
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
