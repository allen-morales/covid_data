{{ config(
    materialized='incremental',
    unique_key='fact_covid_global_metrics_id'
) }}

WITH 
date_mapping AS (
    SELECT
        date AS fact_date,
        date_id
    FROM {{ ref('dim_calendar') }}
)
SELECT
	md5(concat(idl.uid, dm.date_id)) AS fact_covid_global_metrics_id,
    cdr.uid AS location_id,
    dm.date_id AS date_id,
	cdr.confirmed,
	cdr.active,
	cdr.deaths,
	cdr.incident_rate,
	cdr.case_fatality_ratio,
	cdr.last_update,
	cdr.date AS report_date,
	cdr.ingestion_timestamp
FROM
	{{ source('covid_data_raw', 'csse_covid_19_daily_reports')  }} cdr
JOIN {{ source('covid_data_raw', 'uid_iso_fips_lookup_table')  }} idl
    ON TRUE
    AND cdr.combined_key = idl.combined_key
JOIN date_mapping dm
    ON cdr.report_date = dm.fact_date
JOIN {{ ref('dim_locations') }} loc
    ON cdr.uid = loc.location_id
WHERE loc.location_id IS NOT NULL
{% if is_incremental() %}
AND cdr.ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}