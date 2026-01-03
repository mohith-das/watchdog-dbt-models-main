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

with cte as (
{% set tab = dbt_utils.get_column_values(table=ref('sp_productadsreport_info_wd'),column='tables',default=[]) %}
    {% for i in tab %}
    {% set id =i.split('.')[2].split('_')[0] %}
    select * except(row_num),{{ dbt_utils.surrogate_key(['client_name','reportDate','asin','campaignId','AdGroupId','adid'])}} as primary_key
    from (
    select '{{id}}' as client_name,
    reportDate,
    countryName,
    asin,
    attributedSales7d,
    attributedSales14d,
    cost,
    attributedConversions7d,
    clicks,
    impressions,
    campaignId,
    campaignName,
    adGroupId,
    adId,
    _daton_batch_runtime,
    DATE(TIMESTAMP_MILLIS(CAST(_daton_batch_runtime AS INT64))) as part_key,
    -- profileId,accountName,accountId,
    -- adGroupName,
    -- currency,attributedConversions1d,attributedConversions14d,
    -- attributedConversions30d,attributedConversions1dSameSKU,attributedConversions7dSameSKU,attributedConversions14dSameSKU,
    -- attributedConversions30dSameSKU,attributedUnitsOrdered1d,attributedUnitsOrdered7d,attributedUnitsOrdered14d,attributedUnitsOrdered30d,
    -- attributedSales1d,attributedSales30d,attributedSales1dSameSKU,attributedSales7dSameSKU,
    -- attributedSales14dSameSKU,attributedSales30dSameSKU,attributedUnitsOrdered1dSameSKU,attributedUnitsOrdered7dSameSKU,attributedUnitsOrdered14dSameSKU,
    -- attributedUnitsOrdered30dSameSKU,_daton_user_id,_daton_batch_id,sku, 
    ROW_NUMBER() OVER (PARTITION BY reportDate ,
      campaignId,
      AdGroupId,
      asin,
      adid
    order by _daton_batch_runtime desc) row_num
    from {{i}}
        {% if is_incremental() %}
    WHERE CAST(_daton_batch_runtime as int64) >= {{ns.max_loaded}}
    {% endif %}
    ) 
    where row_num = 1
     {% if not loop.last %} union all {% endif %}
{% endfor %} 
)
select *
from cte 
{% if is_incremental() %}
where part_key >= DATE(TIMESTAMP_MILLIS(CAST({{ns.max_loaded}} AS INT64))) + 1
{% endif %}

