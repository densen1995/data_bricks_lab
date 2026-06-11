-- Marathos - Unity Catalog setup
-- Creates the marathos catalog with bronze/silver/gold/default schemas and a
-- `raw` volume under default for landing source files. Run once before the
-- pipeline. The Lakeflow pipeline itself creates the bronze/silver/gold tables.

CREATE CATALOG IF NOT EXISTS marathos
  

CREATE SCHEMA IF NOT EXISTS marathos.bronze
  

CREATE SCHEMA IF NOT EXISTS marathos.silver
  

CREATE SCHEMA IF NOT EXISTS marathos.gold
  

CREATE SCHEMA IF NOT EXISTS marathos.default
  
CREATE VOLUME IF NOT EXISTS marathos.default.raw
  
