{{ config(materialized="table") }}
{% set ns = namespace() %}
{% set ns.array0 = ['Date', 'client_name', 'countryname', 'campaignId', 'campaignName', 'AdType' ]%}
{% set ns.array1 = ['AdSales', 'AdSpend',  'AdOrders', 'AdClicks',  'AdImpressions' ]%}
{% set ns.array2 = ['ACOS', 'AdCR', 'AdCTR'] %}
{% set ns.array3 = [
            ['AdSpend', 'AdSales', 'ACOS', 0],
            ['AdOrders', 'AdClicks', 'AdCR', 0],
            ['AdClicks', 'AdImpressions', 'AdCTR', 0]
            ] %}

{% set ns.array4 = [
] %}

{% set ns.array5 = [
] %}

{% set thresholdMapper = {
                            'AdSpend': 'AdSpend',
                            'AdImpressions': 'AdImpressions',
                            'Orders': 'Orders',
                            'AdOrders': 'Orders',
                            'AdSales': 'AdSales',                            
                            'Clicks': 'Clicks',
                            'AdClicks': 'Clicks',                            
                            'TotalSales': 'TotalSales',
                            'Units': 'Units',
                            'AdUnits': 'Units',
                            'ACOS': 'ACOS',
                            'AdCTR': 'AdCTR',
                            'AdUnitPrice': 'UnitPrice',
                            'ReturnRate': 'ReturnRate',
                            'BuyBoxPercent': 'BuyBoxPercent', 
                            'AdCR': 'AdCR'

} %}

WITH 
AdsTable AS( SELECT 
    (reportDate) AS Date,
    client_name,
    countryname,
    campaignId,
    campaignName,
    Adtype,
    ROUND(IFNULL(SUM(AdSales), 0),0) AS AdSales,
    ROUND(IFNULL(SUM(Adspend), 0),0) AS AdSpend,
    ROUND(IFNULL(SUM(Conversions), 0),0) AS AdOrders,
    IFNULL(SUM(clicks), 0) AS AdClicks,
    IFNULL(SUM(impressions), 0) AS AdImpressions,
    
    FROM     (select *,  row_number() over (
                        partition by reportDate, client_name, countryname, Adtype, campaignId, campaignName
                        order by part_key desc
                    ) rn from
    {{ref('ads_data_with_asin_final_wd')}}) where rn =1 
    GROUP BY 1,2,3,4,5,6
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

    FROM AdsTable
)
select * from ComboTable
