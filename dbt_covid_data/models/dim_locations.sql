SELECT
	uid AS location_id,
	iso2,
	iso3,
	code3,
	fips,
	admin2,
	province_state,
	country_region,
	lat AS latitude,
	long_ AS longitude,
	combined_key,
	population,
	ingestion_timestamp
FROM
	{{ source('covid_data_raw', 'uid_iso_fips_lookup_table') }}
WHERE code3 IS NOT NULL
ORDER BY country_region, province_state