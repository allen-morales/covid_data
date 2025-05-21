SELECT
	idl.uid AS uid,
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
JOIN {{ source('covid_data_raw', 'uid_iso_fips_lookup_table')  }} idl
ON TRUE
AND cdru.uid = idl.uid
{% if table_exists(this)  %}
AND cdr.ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}
