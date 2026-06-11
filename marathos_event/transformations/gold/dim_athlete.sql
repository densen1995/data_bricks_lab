CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Dim athlete - gold layer (latest attribute value per athlete)" AS
SELECT
  athlete_id,
  athlete_source_id,
  MAX_BY(athlete_country, year_of_event)        AS athlete_country,
  MAX_BY(athlete_club, year_of_event)           AS athlete_club,
  MAX_BY(athlete_gender, year_of_event)         AS athlete_gender,
  MAX_BY(athlete_year_of_birth, year_of_event)  AS athlete_year_of_birth
FROM
  marathos.silver.races_obt
GROUP BY
  athlete_id, athlete_source_id;
