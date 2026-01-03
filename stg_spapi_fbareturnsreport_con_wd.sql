{{ config 
    (materialized="table" ,
   tags=["watchdog"]
    )
}}


{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables
from  `amz-atlas-client-warehouse`.Atlas.INFORMATION_SCHEMA.TABLES where table_name like '%FBAReturnsReport'
and lower(table_name) not like '%audit%'
{% endset %}

{% set results = run_query(table_name_query) %}

{# Return the first column #}
{% if execute %} {% set results_list = results.columns[0].values() %}
{% else %} {% set results_list = [] %}
{% endif %}

{% for i in results_list %}
    {% set id =i.split('.')[2].split('_')[0] %}
select *
from
    (
        select
            *,
            row_number() over (
                partition by order_id, sku, license_plate_number, _daton_batch_runtime
                order by _daton_batch_id desc
            ) row_num_1
        from
            (
                select
                    '{{id}}' as client_name,
                    marketplacename,
                    cast(return_date as timestamp) return_date,
                    asin,
                    quantity,
                    _daton_batch_runtime,
                    -- ReportstartDate,ReportendDate,
                    -- fnsku,product_name,fulfillment_center_id,detailed_disposition,reason,status,customer_comments,
                    license_plate_number,sku,order_id,_daton_user_id,_daton_batch_id,    
                    row_number() over (
                        partition by order_id, sku, license_plate_number
                        order by _daton_batch_runtime desc
                    ) row_num
                from {{ i }}
            )
        where row_num = 1
    )
where row_num_1 = 1
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
