depends_on: {{ ref('dim_calendar') }}
{{ get_top_5_values_with_frequency('dim_calendar') }}
depends_on: {{ ref('dim_locations') }}
{{ get_top_5_values_with_frequency('dim_locations') }}
depends_on: {{ ref('fct_covid_global_metrics') }}
{{ get_top_5_values_with_frequency('fct_covid_global_metrics') }}
depends_on: {{ ref('fct_covid_us_metrics') }}
{{ get_top_5_values_with_frequency('fct_covid_us_metrics') }}