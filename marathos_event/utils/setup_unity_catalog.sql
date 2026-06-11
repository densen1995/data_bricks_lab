-- Marathos - Unity Catalog setup
-- Creates the marathos catalog with bronze/silver/gold/default schemas and a
-- `raw` volume under default for landing source files. Run once before the
-- pipeline. The Lakeflow pipeline itself creates the bronze/silver/gold tables.

CREATE CATALOG IF NOT EXISTS marathos
  COMMENT 'Marathos lab - ultramarathon race results medallion architecture';

CREATE SCHEMA IF NOT EXISTS marathos.bronze
  COMMENT 'Raw ingested data, schema-on-read';

CREATE SCHEMA IF NOT EXISTS marathos.silver
  COMMENT 'Cleaned and conformed one-big-table';

CREATE SCHEMA IF NOT EXISTS marathos.gold
  COMMENT 'Star-schema facts, dimensions, and serving views';

CREATE SCHEMA IF NOT EXISTS marathos.default
  COMMENT 'Default schema - hosts the raw landing volume';

CREATE VOLUME IF NOT EXISTS marathos.default.raw
  COMMENT 'Landing zone for source files (historical CSV + simulated stream)';
