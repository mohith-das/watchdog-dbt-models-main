
{{ config(
    materialized='view',
       tags=["watchdog"]
)}}


with cte as(
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables
from `amz-atlas-client-warehouse`.Atlas.INFORMATION_SCHEMA.TABLES where table_name like '%CatalogItems'
and lower(table_name) not like '%audit%'
{% endset %}
 
{% set results = run_query(table_name_query) %}
 
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
{% for i in results_list %}
    {% set id =i.split('.')[2].split('_')[0] %}
        select
        '{{id}}' as Client_name,
        case when marketplaceName = 'UK' then 'Amazon.co.uk' 
        when marketplaceName = 'US' then 'Amazon.com'
        when marketplaceName = 'Germany' then 'Amazon.de'
        when marketplaceName = 'Sweden' then 'Amazon.se'
        when marketplaceName = 'Spain' then 'Amazon.es'
        when marketplaceName = 'Italy' then 'Amazon.it'
        when marketplaceName = 'France' then 'Amazon.fr'
        when marketplaceName = 'Turkey' then 'Amazon.com.tr'
        when marketplaceName = 'Poland' then 'Amazon.pl'
        when marketplaceName = 'Mexico' then 'Amazon.com.mx' else marketplaceName end as Country, 
        ReferenceASIN as asin, 
        image.images as image_nest,
        _daton_user_id, 
        _daton_batch_runtime, 
        _daton_batch_id,
         DATE(TIMESTAMP_MILLIS(CAST(_daton_batch_runtime AS INT64))) as part_key,
        from {{i}},
        UNNEST(images) image  
        qualify DENSE_RANK() OVER (PARTITION BY asin order by _daton_batch_runtime  desc) = 1 
    {% if not loop.last %} union all {% endif %}
{% endfor %})
Select * except(image_nest,variant,height,width), 
from cte, UNNEST(image_nest) image_nest
qualify Row_Number() OVER (PARTITION BY asin order by _daton_batch_runtime  desc) = 1
