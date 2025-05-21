select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select fact_covid_us_metrics_id
from "dev"."covid_data_warehouse"."fct_covid_us_metrics"
where fact_covid_us_metrics_id is null



      
    ) dbt_internal_test