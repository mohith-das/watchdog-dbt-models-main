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
client_name,
date as purchase_date,
CASE when marketplacename = 'US' then 'United States' else marketplacename end as countryname,
ParentASIN as parent_asin,
ChildASIN as asin,
null as Title,
Sessions,
Cast(Replace(Replace(cast(SessionPercentage as string), "%", ""), ",", "") as Float64)/100 as Session_Percentage,
PageViews as Page_Views,
Cast(Replace(Replace(cast(PageViewsPercentage as string), "%", ""), ",", "") as FLOAT64)/100 as Page_Views_Percentage,
Cast(Replace(Replace(cast(BuyBoxPercentage as string), "%", ""), ",", "") as FLOAT64)/100 as Featured_Offer_Buy_Box_Percentage,
unitsordered as Units_Ordered,
Cast(Replace(Replace(cast(UnitSessionPercentage as string), "%", ""), ",", "") as FLOAT64)/100 as Unit_Session_Percentage,
Cast(Replace(Replace(cast(OrderedProductSales_amount as string), "$", ""), ",", "") as FLOAT64) as Ordered_Product_Sales,
totalorderitems as Total_Order_Items,
UnitsOrderedB2B as Units_Ordered_B2B, 
Cast(Replace(Replace(cast(UnitsessionpercentageB2B as string), "%", ""), ",", "") as FLOAT64)/100 as Unit_Session_Percentage_B2B, 
Cast(Replace(Replace(cast(OrderedProductSalesB2B_amount as string), "$", ""), ",", "") as FLOAT64) as Ordered_Product_Sales_B2B, 
TotalOrderItemsB2B as Total_Order_Items_B2B,
_daton_batch_runtime,
part_key
from  
-- pointstory.data_transformation.stg_spapi_SalesAndTrafficReportByChildASIN_Consolidated
{{ref('stg_spapi_SalesAndTrafficReportByChildASIN_con_wd')}}
)

SELECT * except(row_num)
From (select *,
ROW_NUMBER() OVER (PARTITION BY purchase_date,parent_asin,asin order by _daton_batch_runtime desc) as row_num
from cte
{% if is_incremental() %}
where part_key >= DATE('{{ns.max_loaded}}') - 1 
{% endif %}
) where row_num = 1