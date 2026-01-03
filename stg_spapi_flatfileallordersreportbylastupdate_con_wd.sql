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

with consolidated as 
(
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables
from  `amz-atlas-client-warehouse`.Atlas.INFORMATION_SCHEMA.TABLES where table_name like '%FlatFileAllOrdersReportbyLastUpdate'
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
    select * from (
    select *, ROW_NUMBER() OVER (PARTITION BY amazon_order_id, asin, _daton_batch_runtime order by _daton_batch_id desc) row_num_1
    from (
        select  '{{id}}' as client_name, cast(ReportstartDate as TIMESTAMP) ReportstartDate, cast(ReportendDate as TIMESTAMP)ReportendDate, 
        amazon_order_id, merchant_order_id, cast(purchase_date as datetime) purchase_date,cast(last_updated_date as string) last_updated_date,
        order_status, fulfillment_channel, sales_channel, order_channel, url, ship_service_level, product_name, sku, asin, cast(number_of_items as int64) number_of_items,
        item_status, quantity, currency, item_price, cast(item_tax as numeric) item_tax, shipping_price, cast(shipping_tax as numeric) shipping_tax, gift_wrap_price,
        cast(gift_wrap_tax as numeric) gift_wrap_tax, cast(item_promotion_discount as numeric) item_promotion_discount, cast(ship_promotion_discount as numeric) ship_promotion_discount,
        address_type, ship_city, ship_state, cast(ship_postal_code as string) ship_postal_code, ship_country, promotion_ids,cast(item_extensions_data as int64) item_extensions_data,
        cast(is_business_order as string) is_business_order, purchase_order_number, price_designation, buyer_company_name, customized_url, customized_page, cast(is_replacement_order as string) is_replacement_order,
        original_order_id, licensee_name, license_number, license_state, license_expiration_date, _daton_user_id, _daton_batch_runtime, _daton_batch_id,
    ROW_NUMBER() OVER (PARTITION BY amazon_order_id, asin order by _daton_batch_runtime desc) row_num
    from {{i}}
    {% if is_incremental() %}
    WHERE _daton_batch_runtime  >= {{ns.max_loaded}}
    {% endif %}
    ) where row_num = 1)
    where row_num_1 =1  
     
      {% if not loop.last %} union all {% endif %}
{% endfor %}
),

mws as
(
  SELECT ads.client_name, ads.currency, ads.ReportstartDate, ads.ReportendDate, ads.amazon_order_id, ads.merchant_order_id,
   CAST(ads.purchase_date as TIMESTAMP) as purchase_date,
--    CASE WHEN date(purchase_date) between "2021-03-28" and "2021-10-31" THEN TIMESTAMP_ADD(TIMESTAMP(purchase_date), INTERVAL ts.UTC_Offset HOUR)
--    ELSE TIMESTAMP_ADD(TIMESTAMP(purchase_date), 
--    INTERVAL ts.UTC_Offset_without_Daylight_savings HOUR) END as purchase_date_Local_Time, 
 ads.last_updated_date, ads.order_status, ads.fulfillment_channel, 
   ads.sales_channel,
   case when ads.sales_channel='Amazon.com' THEN 'United States'
         when ads.sales_channel='Amazon.ca' THEN 'Canada'
         when ads.sales_channel='Amazon.com.mx' THEN 'Mexico'
         end as CountryName, ads.order_channel, ads.url, ads.ship_service_level,ads.sku, ads.asin, ads.number_of_items, ads.item_status,
    ads.quantity, ads.item_price, ads.item_tax, ads.shipping_price, ads.shipping_tax, ads.gift_wrap_price, ads.gift_wrap_tax, 
    ads.item_promotion_discount, ads.ship_promotion_discount, ads.address_type, ads.ship_city, ads.ship_state, ads.ship_postal_code, 
    ads.ship_country, ads.promotion_ids, ads.item_extensions_data, ads.is_business_order, ads.purchase_order_number, ads.price_designation, 
    ads.buyer_company_name, ads.customized_url, ads.customized_page, ads.is_replacement_order, ads.original_order_id, ads.licensee_name, 
    ads.license_number, ads.license_state, ads.license_expiration_date, ads._daton_user_id, ads._daton_batch_runtime, ads._daton_batch_id,
    ads.product_name, DATE(TIMESTAMP_MILLIS(CAST(ads._daton_batch_runtime AS INT64))) as part_key,
    FROM consolidated as ads
-- left join (select distinct currency, UTC_Offset, UTC_Offset_without_Daylight_savings,sales_channel from pointstory.data_transformation.MappingOffset
-- where sales_channel != 'Non-Amazon'
-- ) as ts
-- on ads.currency = ts.currency and
-- ads.sales_channel=ts.sales_channel

)
Select *,
{{ dbt_utils.surrogate_key([ 'client_name', 'purchase_date', 'amazon_order_id', 'sku', 'asin'])}} as primary_key
from mws
where lower(client_name) not in ('krounds','matisse','champion')
{% if is_incremental() %}
and part_key >= DATE(TIMESTAMP_MILLIS(CAST({{ns.max_loaded}} AS INT64))) + 1
{% endif %}

