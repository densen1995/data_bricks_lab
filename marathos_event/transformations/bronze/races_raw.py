from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType,
)

RAW_VOLUME = "/Volumes/marathos/default/raw"

# Shared source schema for the historical CSV and the simulated-stream CSVs.
# A defined schema is required for a streaming read anyway, so we keep it explicit.
source_schema = StructType([
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

# Source header -> snake_case, applied once at the bronze edge.
RENAME = {
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


@dp.table(
    name="marathos.bronze.races_raw",
    comment="Raw ultramarathon results streamed from the raw volume (historical CSV + simulated-stream CSVs).",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def races_raw():
    # recursiveFileLookup picks up both /historical and /simulated subfolders;
    # new simulated CSVs dropped by the generator are ingested incrementally.
    df = (
        spark.readStream.format("csv")
        .schema(source_schema)
        .option("header", True)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.csv")
        .load(RAW_VOLUME)
        # _metadata.file_path is the Unity-Catalog-supported row-lineage column.
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingest_ts", current_timestamp())
    )
    for old, new in RENAME.items():
        df = df.withColumnRenamed(old, new)
    return df
