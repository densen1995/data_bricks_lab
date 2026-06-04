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





## Layout

```
marathos_event/
├── dimensional_modeling/
│   └── marathos_star_schema.dbml
│   └── marathosdb(1).png    
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

- Bronze: 
The Bronze layer stores the raw race data from the CSV file as a Delta table with minimal changes.

- Silver:
The Silver layer cleans the data and creates one big table (OBT). Invalid or out-of-scope rows are removed based on documented cleaning rules.

- Gold:
The Gold layer contains dimensional tables, a fact table, and views for analysis and dashboarding.



## Pipeline order (manual)

1. `utils/setup_unity_catalog.sql`
2. CSV upload to `/Volumes/marathos/default/raw/historical` 
3. `transformations/bronze/01_ingest_races_bronze + simulated marathon data(utils/02_simulated_marathon_stream)`
4. `transformations/silver/01_clean_races_silver`
5. `utils/01_country_abbreviations` 
6. `transformations/gold/01_build_gold_tables`
7. `transformations/gold/02_create_views`

## Source attribution

LLM generators(country IOC etc) and kaggle data.



