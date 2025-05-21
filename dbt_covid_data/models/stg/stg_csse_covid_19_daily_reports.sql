SELECT
	idl.uid AS uid,
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
{% if table_exists(this)  %}
AND cdr.ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}
