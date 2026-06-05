# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1 — Exploratory Data Analysis
# MAGIC
# MAGIC Dataset: `TWO_CENTURIES_OF_UM_RACES.csv` — ultramarathon race results.
# MAGIC Source: `/Volumes/marathos/default/raw/historical/TWO_CENTURIES_OF_UM_RACES.csv`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

RAW_PATH = "/Volumes/marathos/default/raw/historical/TWO_CENTURIES_OF_UM_RACES.csv"




# COMMAND ----------

# MAGIC %md
# MAGIC ## Explicit schema
# MAGIC `Year of birth` is read as DoubleType because it has nulls in the source.

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

df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(RAW_PATH)
)

df.printSchema()
df.limit(10).display()

# COMMAND ----------

# MAGIC %md ## Shape

# COMMAND ----------

n_rows = df.count()
n_cols = len(df.columns)
print(f"rows: {n_rows:,}")
print(f"columns: {n_cols}")

# COMMAND ----------

# MAGIC %md ## Descriptive summary of numerical fields

# COMMAND ----------

numerical = [
    "Year of event",
    "Event number of finishers",
    "Athlete year of birth",
    "Athlete average speed",
    "Athlete ID",
]
df.select(numerical).summary().display()

# COMMAND ----------

# MAGIC %md ## Null counts per column

# COMMAND ----------

null_counts = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
])
null_counts.display()

# COMMAND ----------

# MAGIC %md ## Unique events

# COMMAND ----------

n_events = df.select("Event name").distinct().count()
print(f"unique events: {n_events:,}")

df.groupBy("Event name").count().orderBy(F.desc("count")).limit(20).display()

# COMMAND ----------

# MAGIC %md ## Age distribution
# MAGIC Athlete age = `Year of event - Athlete year of birth`.

# COMMAND ----------

df_age = df.withColumn(
    "age",
    F.col("Year of event") - F.col("Athlete year of birth").cast("int"),
).filter(F.col("age").between(10, 100))

df_age.select(F.percentile_approx("age", [0.05, 0.25, 0.5, 0.75, 0.95]).alias("age_percentiles")).display()

df_age.groupBy(((F.col("age") / 5).cast("int") * 5).alias("age_bucket")) \
    .count() \
    .orderBy("age_bucket") \
    .display()

# COMMAND ----------

# MAGIC %md ## Country representation

# COMMAND ----------

df.groupBy("Athlete country") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(25) \
    .display()

# COMMAND ----------

# MAGIC %md ## Sample event-distance / performance pairings
# MAGIC Used in Task 3 to design the cleaning rules.

# COMMAND ----------

df.select("Event distance/length", "Athlete performance") \
    .sample(0.0001, seed=42) \
    .limit(30) \
    .display()
