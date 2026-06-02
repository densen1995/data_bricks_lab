# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2 — Bronze ingest
# MAGIC
# MAGIC Reads CSVs from two folders under the raw volume:
# MAGIC
# MAGIC - `historical/` — the original `TWO_CENTURIES_OF_UM_RACES.csv`
# MAGIC - `simulated/`  — synthetic events produced by the simulator notebook
# MAGIC
# MAGIC Both folders share the same source schema, so they're read with one schema
# MAGIC definition, unioned, and written to `marathos.bronze.races_raw` as a single
# MAGIC Delta table. `source_file` carries per-row lineage so you can always tell
# MAGIC which CSV a row came from.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

HISTORICAL_DIR = "/Volumes/marathos/default/raw/historical"
SIMULATED_DIR  = "/Volumes/marathos/default/raw/simulated"
BRONZE_TABLE   = "marathos.bronze.races_raw"

# Make sure both folders exist so a fresh workspace (no simulator run yet)
# doesn't crash the read.
dbutils.fs.mkdirs(HISTORICAL_DIR)
dbutils.fs.mkdirs(SIMULATED_DIR)

# COMMAND ----------

schema = StructType([
    StructField("Year of event", IntegerType(), True),
    StructField("Event dates", StringType(), True),
    StructField("Event name", StringType(), True),
    StructField("Event distance/length", StringType(), True),
    StructField("Event number of finishers", IntegerType(), True),
    StructField("Athlete performance", StringType(), True),
    StructField("Athlete club", StringType(), True),
    StructField("Athlete country", StringType(), True),
    StructField("Athlete year of birth", DoubleType(), True),
    StructField("Athlete gender", StringType(), True),
    StructField("Athlete age category", StringType(), True),
    StructField("Athlete average speed", DoubleType(), True),
    StructField("Athlete ID", IntegerType(), True),
])


def read_csv_dir(path: str):
    """Read every CSV under `path` with the shared schema. Returns an empty
    DataFrame (with the right schema) if the folder has no CSVs yet."""
    files = [f for f in dbutils.fs.ls(path) if f.name.endswith(".csv") or f.isDir()]
    if not files:
        return spark.createDataFrame([], schema).withColumn(
            "source_file", F.lit(None).cast("string")
        )
    return (
        spark.read
            .option("header", True)
            .option("recursiveFileLookup", "true")  # pick up part-files in subfolders
            .schema(schema)
            .csv(path)
            # F.input_file_name() is blocked by Unity Catalog; _metadata.file_path
            # is the UC-supported equivalent.
            .withColumn("source_file", F.col("_metadata.file_path"))
    )


historical_df = read_csv_dir(HISTORICAL_DIR)
simulated_df  = read_csv_dir(SIMULATED_DIR)

df_raw = historical_df.unionByName(simulated_df)
print(
    f"unioned: {historical_df.count():,} historical + "
    f"{simulated_df.count():,} simulated rows"
)

# COMMAND ----------

# MAGIC %md ## Rename to snake_case + add ingest metadata

# COMMAND ----------

rename_map = {
    "Year of event": "year_of_event",
    "Event dates": "event_dates",
    "Event name": "event_name",
    "Event distance/length": "event_distance_length",
    "Event number of finishers": "event_number_of_finishers",
    "Athlete performance": "athlete_performance",
    "Athlete club": "athlete_club",
    "Athlete country": "athlete_country",
    "Athlete year of birth": "athlete_year_of_birth",
    "Athlete gender": "athlete_gender",
    "Athlete age category": "athlete_age_category",
    "Athlete average speed": "athlete_average_speed",
    "Athlete ID": "athlete_source_id",
}

df_bronze = df_raw
for old, new in rename_map.items():
    df_bronze = df_bronze.withColumnRenamed(old, new)

df_bronze = df_bronze.withColumn("ingest_ts", F.current_timestamp())

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md ## Write to Delta

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"wrote {BRONZE_TABLE}: {spark.table(BRONZE_TABLE).count():,} rows")
