{{
 config(
   materialized = 'incremental',
      tags=["watchdog"],
   incremental_strategy = 'insert_overwrite',
   partition_by = {
     'field': 'part_key', 
     'data_type': 'date',
     'granularity': 'day'
   }
 )
}}

{% set ns = namespace() %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT MAX(_daton_batch_runtime) - 259200000 FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set ns.max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set ns.max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}

select concat(table_catalog,'.',table_schema, '.',table_name) as tables 
from `amz-atlas-client-warehouse`.Atlas.INFORMATION_SCHEMA.TABLES where table_name like '%_SalesAndTrafficReportByChildASIN'
and lower(table_name) not like '%audit%'
{% endset %}
 
{% set results = run_query(table_name_query) %}
 
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


with Consolidated as(
 
{% for i in results_list %}
    {% set id =i.split('.')[2].split('_')[0] %}
         select 
         * 
         except(row_num)
         from (SELECT
            '{{ id }}' AS client_name,
            *,
            DATE(TIMESTAMP_MILLIS(CAST(_daton_batch_runtime AS INT64))) as part_key,
            ROW_NUMBER() OVER (PARTITION BY date, parentAsin, childAsin ORDER BY _daton_batch_runtime DESC) AS row_num
            FROM
            `{{i}}`
            {% if is_incremental() %}
            WHERE _daton_batch_runtime  >= {{ns.max_loaded}}
            {% endif %}
            ) where row_num = 1
     {% if not loop.last %} union all {% endif %}
{% endfor %}
)
select * from Consolidated
{% if is_incremental() %}
where part_key >= DATE(TIMESTAMP_MILLIS(CAST({{ns.max_loaded}} AS INT64))) + 1
{% endif %}