
  
    
    

    create  table
      "dev"."covid_data_warehouse"."fct_covid_us_metrics"
  
    as (
      

WITH 
date_mapping AS (
    SELECT
        date AS fact_date,
        date_id
    FROM "dev"."covid_data_warehouse"."dim_calendar"
)
SELECT
    md5(concat(cdru.uid, dm.date_id)) AS fact_covid_us_metrics_id,
	cdru.uid AS location_id,
    dm.date_id AS date_id,
	cdru.confirmed,
	cdru.active,
	cdru.deaths,
	cdru.incident_rate,
	cdru.case_fatality_ratio,
	cdru.last_update,
	cdru.report_date,
	cdru.ingestion_timestamp
FROM
    "dev"."covid_data_warehouse"."stg_csse_covid_19_daily_reports_us" cdru
JOIN date_mapping dm
ON cdru.report_date = dm.fact_date
JOIN "dev"."covid_data_warehouse"."dim_locations" loc
ON cdru.uid = loc.location_id
WHERE loc.location_id IS NOT NULL
    );
  
  
  