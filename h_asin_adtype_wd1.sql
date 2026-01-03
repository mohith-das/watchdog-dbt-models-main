{{ config(materialized="table") }}
{% set ns = namespace() %}
{% set ns.array0 = ['Date', 'client_name', 'countryname', 'asin', 'product_name', 'Link', 'AdType' ]%}
{% set ns.array1 = ['TotalSales', 'Units', 'Orders', 'Clicks', 'AdSales', 'AdSpend',  'AdOrders',
 'AdClicks',  'AdImpressions', 'BuyBoxPercent' ]
%}
{% set ns.array2 = ['ACOS', 'AdCR', 'AdCTR', 'ReturnRate', 'DaysCoverage'] %}

{% set ns.array3 = [
            ['AdSpend', 'AdSales', 'ACOS', 0],
            ['AdOrders', 'AdClicks', 'AdCR', 0],
            ['Clicks', 'AdImpressions', 'AdCTR', 0],
            ['ReturnedUnits', 'Units', 'ReturnRate', 0],
            ['StockUnits', 'AvgUnitsSold', 'DaysCoverage', 1]
            ] %}

{% set ns.array4 = [
    'ReturnedUnits', 
    'StockUnits',
    'AvgUnitsSold'
] %}

WITH 
TotalsTable AS (SELECT
    (purchase_date) AS Date,
    client_name,
    countryname,
    asin AS ASIN,
    product_name,
    ROUND(SUM(Totalsales),0) AS TotalSales,
    CAST(SUM(numberoforders) AS NUMERIC) AS Orders,
    CAST(SUM(Units_Orders) AS NUMERIC) AS Units,
    MAX(link) AS Link,
    -- ROUND(CAST(SUM(CAST(item_promotion_discount AS FLOAT64)) + SUM(CAST(ship_promotion_discount AS FLOAT64)) AS NUMERIC),0) AS Total_Discounts
    FROM     (select *,  row_number() over (
                        partition by amazon_order_id, asin 
                        order by part_key desc
                    ) rn from
    {{ref('m_d_f_wd')}}) where rn =1
    GROUP BY 1,2,3,4,5
),

AvgUnitsTable as (SELECT
    (purchase_date) AS Date,
    client_name,
    countryname,
    asin AS ASIN,
    product_name,
    CAST(SUM(Units_Orders)/30 AS NUMERIC) AvgUnitsSold,
    FROM     (select *,  row_number() over (
                        partition by amazon_order_id, asin 
                        order by part_key desc
                    ) rn from
    {{ref('m_d_f_wd')}}) where rn =1
    and purchase_date >= current_date() - 30 and purchase_date != current_date()
    GROUP BY 1,2,3,4,5
),

AdsTable AS( SELECT 
    (reportDate) AS Date,
    client_name,
    countryname,
    asin AS ASIN,
    Adtype,
    ROUND(IFNULL(SUM(AdSales), 0),0) AS AdSales,
    ROUND(IFNULL(SUM(Adspend), 0),0) AS AdSpend,
    ROUND(IFNULL(SUM(Conversions), 0),0) AS AdOrders,
    IFNULL(SUM(clicks), 0) AS AdClicks,
    IFNULL(SUM(impressions), 0) AS AdImpressions,
    
    FROM     (select *,  row_number() over (
                        partition by reportDate, client_name, countryname, Adtype, asin 
                        order by part_key desc
                    ) rn from
    {{ref('ads_data_with_asin_final_wd')}}) where rn =1
    GROUP BY 1,2,3,4,5
),

SessionsTable AS 
(
    SELECT 
        (purchase_date) AS Date,
        client_name,
        countryname,
        asin AS ASIN,
        SUM(CAST(IFNULL((CAST(Sessions AS FLOAT64)), 0) AS NUMERIC)) AS Clicks,
        AVG(CAST(IFNULL(Featured_Offer_Buy_Box_Percentage,0) AS FLOAT64)) AS BuyBoxPercent
    FROM     (select *,  row_number() over (
                        partition by purchase_date, parent_asin, asin 
                        order by part_key desc
                    ) rn from
    {{ref('detailPageSales_traffic_by_child_asin_wd')}}) where rn =1
    GROUP BY 1,2,3,4
),

ReturnsTable AS (
    SELECT Date(return_date) AS date,
    client_name,
    case when marketplacename='US' then 'United States'
    when marketplacename='CA' then 'Canada'
    else marketplacename end AS countryname,
    asin as ASIN,
    cast (sum(quantity) as numeric) ReturnedUnits
    FROM {{ ref("stg_spapi_fbareturnsreport_con_wd") }}

    GROUP BY Date, client_name, countryname,asin
),

InventoryTable AS (
 SELECT 
 date,
 client_name,ASIN,product_name, countryname,
 IFNULL(SUM(Stock_Units), 0) as StockUnits,
 from
    (
        select
            *,
            row_number() over (
                partition by
                    primary_key
                order by part_key desc
            ) as row_num
        from
                {{ ref("sp_fbami_con_wd") }}
    )
    where row_num = 1
    group by 1,2,3,4,5
),

ComboTable AS(
SELECT 
    {% for kpi in ns.array0 %}
        {{kpi}},
    {% endfor %}
    {% for kpi in ns.array1 + ns.array4 %}
        ifnull({{kpi}}, 0) {{kpi}},
    {% endfor %}

    {% for metric1, metric2, kpi, kind in ns.array3 %}
        {% if kind == 0 %}
            CASE WHEN (ifnull({{metric2}},0) = 0 and {{metric1}} > 0) Then 1
                ELSE
                    ifnull(safe_divide(
                        {{metric1}},
                        {{metric2}}
                    ),0) END {{kpi}},
        {% elif kind == 1 %}
            CASE WHEN (ifnull({{metric2}},0) = 0 and {{metric1}} > 0) Then 100
                ELSE
                ifnull(safe_divide(
                        {{metric1}}  *100 ,
                        {{metric2}}
                    ),0) END {{kpi}}
        {% endif %}
    {% endfor %}

    FROM TotalsTable 
    left JOIN AvgUnitsTable
    using (Date, client_name, countryname, asin, product_name)
    left JOIN AdsTable
    USING (Date, client_name, countryname, asin)
    left JOIN SessionsTable
    USING (Date, client_name, countryname, asin)
    LEFT JOIN ReturnsTable
    USING (Date, client_name, countryname, asin)
    LEFT JOIN InventoryTable
    USING (Date, client_name, countryname, asin, product_name)
)

select * from ComboTable