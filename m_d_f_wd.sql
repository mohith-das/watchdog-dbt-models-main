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


with kk as  
(select a.*, b.link
from 
-- pointstory.data_transformation.stg_spapi_FlatFileAllOrdersReportbyLastUpdate_Consolidated
{{ref('stg_spapi_flatfileallordersreportbylastupdate_con_wd')}} a
left join 
-- pointstory.data_transformation.stg_spapi_CatalogItems_Consolidated
{{ref('stg_spapi_catalogitems_con_wd')}} b
on a.client_name=b.client_name and a.asin = b.asin and a.sales_channel = b.Country

 where order_status in ('Shipped','Shipping','Pending','Cancelled') and  sales_channel like "%Amazon.%"

     ), 
    mws AS (
    SELECT
      client_name,
      date(cast(purchase_date as timestamp), "America/Los_Angeles") as purchase_date,
    --   CAST(CAST(purchase_date as TIMESTAMP) as date) as purchase_date,
      sku,
      asin,
      amazon_order_id,
      order_status,
      product_name,
      sales_channel,
      fulfillment_channel,
      countryname,
      ship_country,
      currency,
      link,
      part_key,
    --   _daton_batch_runtime,
      SUM(quantity) Units_Orders,
      sum(item_price) Totalsales,
      count(distinct(amazon_order_id)) numberoforders
      from kk
    {{ dbt_utils.group_by(14) }}) 
            select *,
      {{
        dbt_utils.surrogate_key(
            ["client_name", "purchase_date", "amazon_order_id", "order_status", "sku", "asin"]
        )
    }} as primary_key
      from mws
      where lower(client_name) not in ('krounds','matisse','champion','bulletproof')
    {% if is_incremental() %}
    and part_key >= DATE('{{ns.max_loaded}}') - 1 
    {% endif %}