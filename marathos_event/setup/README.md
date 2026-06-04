# Marathos ETL pipeline 

Databricks medallion-architecture project for the **Marathos race**. Ingests two
centuries of ultramarathon race results into a `marathos` Unity Catalog and
exposes a star-schema gold layer for dashboards and a Genie space.

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



