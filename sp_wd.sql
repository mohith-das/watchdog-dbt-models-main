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
{% set ns.part_key = "part_key" %}

{% if is_incremental() %}

{%- if execute -%}
{% set curr_model %} {{ this }} {% endset %}
{% set this_model = curr_model.split('.')[2].replace("`","").strip() %}
{% do log(this_model, info=true) %}
{% set models = graph.nodes.values() %}

{% set model = (models | selectattr('name', 'equalto', this_model) | list).pop() %}

{% set refs = model['refs'] %}
    {% set ns.unique_refs = [] %}    
    {% for ref in refs if ref not in ns.unique_refs %}
        {% do ns.unique_refs.append(ref[0]) %}
    {% endfor %}


{% set ns.parent_models = ns.unique_refs %}

{% do log("parent_models:", info=true) %} 
{% do log(ns.parent_models, info=true) %} 

    {% for model_name in ns.parent_models %} 

        {% set ns.query %} SELECT MAX({{ns.part_key}}) FROM {{ ref(model_name) }} {% endset %} 
        {% set query_result = run_query(ns.query) %}
        {% set min_dbr = query_result.rows[0].values()[0] %}

        {% if not ns.min or ns.min > min_dbr %}
            {% set ns.min = min_dbr %}
        {%endif%}
    {% endfor %}

{% set ns.max_loaded = ns.min %}
{% do log("min_of_max_loaded:", info=true) %} 
{% do log(ns.max_loaded, info=true) %} 

{% else %}
{% set ns.max_loaded = 0 %}
{%- endif -%}
{% endif %}

with cte as (
      SELECT
      ad.client_name,
    --   ad.client_name as Business_name,
      ad.countryname, 
    --   ac.currencycode as currency,
      'SponsoredProduct' AS Adtype,
      ad.reportDate,
    --   CAST(pt.name as STRING) PortfolioName,
    --   c.profileId,
      ad.campaignName,
      CAST(ad.impressions AS NUMERIC) impressions,
      ad.clicks clicks,
      CAST(ad.cost AS NUMERIC) Adspend,
       ad.attributedsales7d AdSales,
       attributedConversions7d Conversions,
       attributedUnitsOrdered7d AdUnits,
       ad.part_key as part_key
    FROM
      {{ref('sp_cr_con_wd')}} ad
{{ dbt_utils.group_by(12) }})
    select * from cte 
    {% if is_incremental() %}
    where part_key >= DATE('{{ns.max_loaded}}') - 1 
    {% endif %}

