# Marathos ETL pipeline 


This project builds a Databricks medallion pipeline for marathon race results from the TWO_CENTURIES_OF_UM_RACES dataset.
The purpose is to create a data platform that helps Marathos business stakeholders explore race results, marathon types, athlete performance, and country participation.

Databricks medallion-architecture project for the **Marathos race**. Ingests two
centuries of ultramarathon race results into a `marathos` Unity Catalog and
exposes a star-schema gold layer for dashboards and a Genie space.


## Tech stack
- Databricks(Databricks dashboards / Genie)
- Unity Catalog
- PySpark
- SQL
- Git and GitHub





The medallion layers are built as a **Lakeflow Declarative Pipeline** — each layer
is a streaming table or materialized view, not a batch `overwrite`. New CSVs (e.g.
from the simulated stream) flow through bronze → silver → gold incrementally.

## Layout

```
marathos_event/
├── dimensional_modeling/
│   ├── marathos_star_schema.dbml
│   └── marathosdb(1).png
├── explorations/
│   ├── 01_eda.py
│   ├── 02_genie_answer_validation.py
│   └── 03_dashboard_queries.sql
├── transformations/                    # <- the Lakeflow pipeline source
│   ├── bronze/races_raw.py             # STREAMING TABLE (readStream from raw volume)
│   ├── silver/races_obt.py             # STREAMING TABLE (FROM STREAM bronze)
│   └── gold/
│       ├── fct_results.sql             # STREAMING TABLE (FROM STREAM silver)
│       ├── dim_event.sql               # MATERIALIZED VIEW
│       ├── dim_athlete.sql             # MATERIALIZED VIEW
│       ├── dim_date.sql                # MATERIALIZED VIEW
│       ├── dim_country.py              # MATERIALIZED VIEW (curated lookup)
│       └── vw_*.sql                    # MATERIALIZED VIEWs (serving layer)
└── utils/
    ├── utils.py                        # shared helpers (imported by the pipeline)
    ├── setup_unity_catalog.sql
    ├── 02_simulated_marathon_stream.py
    └── marathos_pipeline.json          # Lakeflow pipeline spec
```

## Unity Catalog

```
marathos
├── default
│   └── raw                         
├── bronze
│   ├── races_raw                   
│   └── races_stream                
├── silver
│   └── races_obt                   
└── gold
    ├── dim_event
    ├── dim_athlete
    ├── dim_date                    
    ├── dim_country                  
    ├── fct_results
    ├── vw_distance_top_events
    ├── vw_distance_gender_split
    ├── vw_distance_country_leaderboard
    ├── vw_time_top_events
    ├── vw_time_country_leaderboard
    ├── vw_time_yearly_progression
    └── vw_results_enriched          
```

## Medallion architecture

- Bronze (**streaming table**):
Streams the raw race CSVs from the volume into `bronze.races_raw` with Auto Loader,
keeping `source_file` lineage. New files are ingested incrementally.

- Silver (**streaming table**):
Reads `FROM STREAM` bronze, cleans the data, and builds one big table (OBT).
Invalid/out-of-scope rows are removed per the cleaning rules. Surrogate keys use
`sha2()` so the transforms are streaming-safe (window functions are not).

- Gold:
The fact table `fct_results` is a **streaming table** (`FROM STREAM` silver).
Dimensions and serving views aggregate, so they are **materialized views**.

## Running it

1. Run `utils/setup_unity_catalog.sql` once (catalog, schemas, raw volume).
2. Upload the historical CSV to `/Volumes/marathos/default/raw/historical`.
3. Create the Lakeflow pipeline from `utils/marathos_pipeline.json`
   (or in the UI: **Lakeflow Pipelines → Create**, root = `marathos_event`,
   source = `transformations/`, serverless). Click **Run** — it builds
   bronze → silver → gold in dependency order.
4. (Optional) Run `utils/02_simulated_marathon_stream.py` to drop a new race CSV;
   the next pipeline run ingests it incrementally — this demonstrates streaming.

## Source attribution

LLM generators(country IOC etc) and kaggle data.



