# Marathos ETL pipeline — Dennis Jonathan

Databricks medallion-architecture project for the **Marathos lab**. Ingests two
centuries of ultramarathon race results into a `marathos` Unity Catalog and
exposes a star-schema gold layer for dashboards and a Genie space.

## Layout

```
marathos_dennis_jonathan/
├── dimensional_modeling/
│   └── marathos_star_schema.dbml    
├── explorations/
│   ├── 01_eda.py                   
│   ├── 02_genie_answer_validation.py 
│   └── 03_dashboard_queries.sql     
├── transformations/
│   ├── bronze/01_ingest_races_bronze.py   
│   ├── silver/01_clean_races_silver.py   
│   └── gold/
│       ├── 01_build_gold_tables.py        
│       └── 02_create_views.sql             
└── utils/
    ├── setup_unity_catalog.sql     
    ├── 01_country_abbreviations.py 
    ├── 02_simulated_marathon_stream.py 
    └── marathos_pipeline_job.json  
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
    ├── vw_distance_country_leaderboard
    ├── vw_time_top_events
    ├── vw_time_country_leaderboard
    └── vw_results_enriched          
```

## Pipeline order (manual or via `utils/marathos_pipeline_job.json`)

1. `utils/setup_unity_catalog.sql`
2. CSV upload to `/Volumes/marathos/default/raw/` 
3. `transformations/bronze/01_ingest_races_bronze`
4. `transformations/silver/01_clean_races_silver`
5. `utils/01_country_abbreviations` 
6. `transformations/gold/01_build_gold_tables`
7. `transformations/gold/02_create_views`
8. `utils/02_simulated_marathon_stream`

## Source attribution

LLM, guide from my teacher Kokchung and lectures from the class was used as a sounding board for code snippets and the
initial draft of `dim_country` and simulated marathos event (then cross-checked against the IOC code list).
The `sha()` ID-generation pattern is from the lab spec. Every other
piece of code is original.
