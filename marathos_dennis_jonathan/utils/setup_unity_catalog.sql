-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Marathos — Unity Catalog setup
-- MAGIC
-- MAGIC Creates the `marathos` catalog with bronze/silver/gold/default schemas
-- MAGIC and a `raw` volume under default for landing source files.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS marathos
  COMMENT 'Marathos lab — ultramarathon race results medallion architecture';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS marathos.bronze
  COMMENT 'Raw ingested data, schema-on-read';

CREATE SCHEMA IF NOT EXISTS marathos.silver
  COMMENT 'Cleaned and conformed one-big-table';

CREATE SCHEMA IF NOT EXISTS marathos.gold
  COMMENT 'Star-schema facts, dimensions, and business views';

CREATE SCHEMA IF NOT EXISTS marathos.default
  COMMENT 'Default schema — hosts the raw landing volume';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS marathos.default.raw
  COMMENT 'Landing zone for source files (CSV, parquet, etc.)';

-- COMMAND ----------

SHOW SCHEMAS IN marathos;
