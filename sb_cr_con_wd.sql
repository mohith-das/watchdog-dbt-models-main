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
{% set tab = dbt_utils.get_column_values(table=ref('sb_cr_info_wd'),column='tables',default=[]) %}
{% for i in tab %}
    {% set id =i.split('.')[2].split('_')[0] %}
    SELECT * except(row_num),
    {{ dbt_utils.surrogate_key([ 'reportDate','client_name', 'countryName', 'accountId', 'campaignName', 'campaignId'])}} as primary_key
    from (
    select     
'{{id}}' as client_name,
profileId,
countryName,
accountName,
accountId,
reportDate,
campaignName,
campaignId,
campaignStatus,
campaignBudget,
campaignBudgetType,
impressions,
clicks,
cost,
attributedDetailPageViewsClicks14d,
attributedSales14d,
attributedSales14dSameSKU,
attributedConversions14d,
attributedConversions14dSameSKU,
attributedOrdersNewToBrand14d,
attributedOrdersNewToBrandPercentage14d,
attributedOrderRateNewToBrand14d,
attributedSalesNewToBrand14d,
attributedSalesNewToBrandPercentage14d,
attributedUnitsOrderedNewToBrand14d,
attributedUnitsOrderedNewToBrandPercentage14d,
unitsSold14d,
dpv14d,
_daton_user_id,
_daton_batch_runtime,
_daton_batch_id, 
DATE(TIMESTAMP_MILLIS(CAST(_daton_batch_runtime AS INT64))) as part_key,
    ROW_NUMBER() OVER (PARTITION BY campaignId,reportDate order by _daton_batch_runtime desc) row_num
    from {{i}}
    {% if is_incremental() %}
    WHERE _daton_batch_runtime  >= {{ns.max_loaded}}
    {% endif %}
    ) where row_num = 1
     {% if not loop.last %} union all {% endif %}
{% endfor %}
)
select *
from cte 
{% if is_incremental() %}
where part_key >= DATE(TIMESTAMP_MILLIS(CAST({{ns.max_loaded}} AS INT64))) + 1
{% endif %}
