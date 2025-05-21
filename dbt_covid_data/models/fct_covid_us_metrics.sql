{{ config(
    materialized='incremental',
    unique_key='fact_covid_us_metrics_id'
) }}

WITH 
date_mapping AS (
    SELECT
        date AS fact_date,
        date_id
    FROM {{ ref('dim_calendar') }}
)
SELECT
    md5(concat(loc.location_id, dm.date_id)) AS fact_covid_us_metrics_id,
	cdru.uid AS location_id,
    dm.date_id AS date_id,
	cdru.confirmed,
	cdru.active,
	cdru.deaths,
	cdru.incident_rate,
	cdru.case_fatality_ratio,
	cdru.last_update,
	cdru.date AS report_date,
	cdru.ingestion_timestamp
FROM
	{{ source('covid_data_raw', 'csse_covid_19_daily_reports_us')  }} cdru
JOIN date_mapping dm
    ON cdru.report_date = dm.fact_date
JOIN {{ ref('dim_locations') }} loc
    ON cdru.uid = loc.location_id
WHERE loc.location_id IS NOT NULL
{% if is_incremental() %}
AND cdru.ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}