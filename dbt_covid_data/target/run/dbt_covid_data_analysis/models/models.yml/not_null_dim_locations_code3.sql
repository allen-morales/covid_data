select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select code3
from "dev"."covid_data_warehouse"."dim_locations"
where code3 is null



      
    ) dbt_internal_test