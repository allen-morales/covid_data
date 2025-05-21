
  
    
    

    create  table
      "dev"."covid_data_warehouse"."fct_covid_global_metrics"
  
    as (
      

WITH 
date_mapping AS (
    SELECT
        date AS fact_date,
        date_id
    FROM "dev"."covid_data_warehouse"."dim_calendar"
)
SELECT
	md5(concat(cdr.uid, dm.date_id)) AS fact_covid_global_metrics_id,
    cdr.uid AS location_id,
    dm.date_id AS date_id,
	cdr.confirmed,
	cdr.active,
	cdr.deaths,
	cdr.incident_rate,
	cdr.case_fatality_ratio,
	cdr.last_update,
	cdr.report_date,
	cdr.ingestion_timestamp
FROM
    "dev"."covid_data_warehouse"."stg_csse_covid_19_daily_reports" cdr
JOIN date_mapping dm
ON cdr.report_date = dm.fact_date
JOIN "dev"."covid_data_warehouse"."dim_locations" loc
ON cdr.uid = loc.location_id
WHERE loc.location_id IS NOT NULL
    );
  
  
  