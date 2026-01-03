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

with Consolidated as(
{% set tab = dbt_utils.get_column_values(table=ref('sp_cr_info_wd'),column='tables',default=[]) %}
    {% for i in tab %}
    {% set id =i.split('.')[2].split('_')[0] %}
    SELECT * except(row_num),{{ dbt_utils.surrogate_key(['reportDate' ,'campaignname','campaignId'])}} as primary_key
    From (
    select '{{id}}' as client_name,
    accountId,
    accountName,
    attributedConversions14d,
    attributedConversions14dSameSKU,
    attributedConversions1d,
    attributedConversions1dSameSKU,
    attributedConversions30d,
    attributedConversions30dSameSKU,
    attributedConversions7d,
    attributedConversions7dSameSKU,
    attributedSales14d,
    attributedSales14dSameSKU,
    attributedSales1d,
    attributedSales1dSameSKU,
    attributedSales30d,
    attributedSales30dSameSKU,
    attributedSales7d,
    attributedSales7dSameSKU,
    attributedUnitsOrdered14d,
    attributedUnitsOrdered14dSameSKU,
    attributedUnitsOrdered1d,
    attributedUnitsOrdered1dSameSKU,
    attributedUnitsOrdered30d,
    attributedUnitsOrdered30dSameSKU,
    attributedUnitsOrdered7d,
    attributedUnitsOrdered7dSameSKU,
    bidPlus,
    campaignBudget,
    campaignId,
    campaignName,
    campaignStatus,
    clicks,
    cost,
    countryName,
    impressions,
    profileId,
    reportDate,
    _daton_batch_id,
    _daton_batch_runtime,
    _daton_user_id,
    DATE(TIMESTAMP_MILLIS(CAST(_daton_batch_runtime AS INT64))) as part_key,
    ROW_NUMBER() OVER (PARTITION BY campaignname,campaignId,reportDate order by _daton_batch_runtime desc) row_num
    from {{i}} 
    {% if is_incremental() %}
    WHERE _daton_batch_runtime  >= {{ns.max_loaded}}
    {% endif %}
    ) 
    where row_num = 1
     {% if not loop.last %} union all {% endif %}
{% endfor %}
)
select * from Consolidated
{% if is_incremental() %}
where part_key >= DATE(TIMESTAMP_MILLIS(CAST({{ns.max_loaded}} AS INT64))) + 1
{% endif %}
