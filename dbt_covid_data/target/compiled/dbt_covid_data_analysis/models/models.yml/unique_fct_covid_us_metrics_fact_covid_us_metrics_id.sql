
    
    

select
    fact_covid_us_metrics_id as unique_field,
    count(*) as n_records

from "dev"."covid_data_warehouse"."fct_covid_us_metrics"
where fact_covid_us_metrics_id is not null
group by fact_covid_us_metrics_id
having count(*) > 1


