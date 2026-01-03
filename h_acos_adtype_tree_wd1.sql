{{ config(materialized="table", schema="final") }}
{% set ns = namespace() %}
{% set ns.array1 = [
                            'AdSpend',
                            'AdSpend_SponsoredBrands',
                            'AdSpend_SponsoredDisplay',
                            'AdSpend_SponsoredProduct',

                            'AdImpressions',
                            'AdImpressions_SponsoredBrands',
                            'AdImpressions_SponsoredDisplay',
                            'AdImpressions_SponsoredProduct',


                            'Orders',
                            'AdOrders',
                            'AdOrders_SponsoredBrands',
                            'AdOrders_SponsoredDisplay',
                            'AdOrders_SponsoredProduct',
                            'OrganicOrders',


                            'AdSales',
                            'AdSales_SponsoredBrands',
                            'AdSales_SponsoredDisplay',
                            'AdSales_SponsoredProduct',
                            'OrganicSales',
                            
                            'Clicks',
                            'AdClicks',
                            'AdClicks_SponsoredBrands',
                            'AdClicks_SponsoredDisplay',
                            'AdClicks_SponsoredProduct',

                            'TotalSales',

                            'Units',
                            'AdUnits',
                            'AdUnits_SponsoredBrands',
                            'AdUnits_SponsoredDisplay',
                            'AdUnits_SponsoredProduct',
                            'OrganicUnits'
                            ]
%}
{% set ns.array2 = [
                            'ACOS',
                            'ACOS_SponsoredBrands',
                            'ACOS_SponsoredDisplay',
                            'ACOS_SponsoredProduct',

                            'AdAOV',
                            'AdAOV_SponsoredBrands',
                            'AdAOV_SponsoredDisplay',
                            'AdAOV_SponsoredProduct',
                            'OrganicAOV',

                            'AdCR',
                            'AdCR_SponsoredBrands',
                            'AdCR_SponsoredDisplay',
                            'AdCR_SponsoredProduct',

                            'AdCTR',
                            'AdCTR_SponsoredBrands',
                            'AdCTR_SponsoredDisplay',
                            'AdCTR_SponsoredProduct',

                            'AdUnitPrice',
                            'AdUnitPrice_SponsoredBrands',
                            'AdUnitPrice_SponsoredDisplay',
                            'AdUnitPrice_SponsoredProduct',
                            'OrganicUnitPrice',

                            'ReturnRate',
                            'TACOS',

                            'AdUnitsPerOrder',
                            'AdUnitsPerOrder_SponsoredBrands',
                            'AdUnitsPerOrder_SponsoredDisplay',
                            'AdUnitsPerOrder_SponsoredProduct',
                            'OrganicUnitsPerOrder'
                            
                            ] %}

{% set ns.array3 = [
            ['AdSpend', 'AdSales', 'ACOS', 1],
            ['AdSpend_SponsoredProduct', 'AdSales_SponsoredProduct', 'ACOS_SponsoredProduct', 1],
            ['AdSpend_SponsoredBrands', 'AdSales_SponsoredBrands', 'ACOS_SponsoredBrands', 1],
            ['AdSpend_SponsoredDisplay', 'AdSales_SponsoredDisplay', 'ACOS_SponsoredDisplay', 1],

            ['AdSales', 'AdOrders', 'AdAOV', 0],
            ['AdSales_SponsoredProduct', 'AdOrders_SponsoredProduct', 'AdAOV_SponsoredProduct', 0],
            ['AdSales_SponsoredBrands', 'AdOrders_SponsoredBrands', 'AdAOV_SponsoredBrands', 0],
            ['AdSales_sponsoredDisplay', 'AdOrders_SponsoredDisplay', 'AdAOV_SponsoredDisplay', 0],
            ['OrganicSales', 'OrganicOrders', 'OrganicAOV', 0],

            ['AdOrders', 'AdClicks', 'AdCR', 1],
            ['AdOrders_SponsoredProduct', 'AdClicks_SponsoredProduct', 'AdCR_SponsoredProduct', 1],
            ['AdOrders_SponsoredBrands', 'AdClicks_SponsoredBrands', 'AdCR_SponsoredBrands', 1],
            ['AdOrders_SponsoredDisplay', 'AdClicks_SponsoredDisplay', 'AdCR_SponsoredDisplay', 1],

            ['AdClicks', 'AdImpressions', 'AdCTR', 1],
            ['AdClicks_SponsoredProduct', 'AdImpressions_SponsoredProduct', 'AdCTR_SponsoredProduct', 1],
            ['AdClicks_SponsoredBrands', 'AdImpressions_SponsoredBrands', 'AdCTR_SponsoredBrands', 1],
            ['AdClicks_SponsoredDisplay', 'AdImpressions_SponsoredDisplay', 'AdCTR_SponsoredDisplay', 1],

            ['AdSales', 'AdUnits', 'AdUnitPrice', 0],
            ['AdSales_SponsoredProduct', 'AdUnits_SponsoredProduct', 'AdUnitPrice_SponsoredProduct', 0],
            ['AdSales_SponsoredBrands', 'AdUnits_SponsoredBrands', 'AdUnitPrice_SponsoredBrands', 0],
            ['AdSales_SponsoredDisplay', 'AdUnits_SponsoredDisplay', 'AdUnitPrice_SponsoredDisplay', 0],
            ['OrganicSales', 'OrganicUnits', 'OrganicUnitPrice', 0],

            ['AdSpend', 'TotalSales', 'TACOS', 1],

            ['AdUnits', 'AdOrders', 'AdUnitsPerOrder', 0],
            ['AdUnits_SponsoredProduct', 'AdOrders_SponsoredProduct', 'AdUnitsPerOrder_SponsoredProduct', 0],
            ['AdUnits_SponsoredBrands', 'AdOrders_SponsoredBrands', 'AdUnitsPerOrder_SponsoredBrands', 0],
            ['AdUnits_SponsoredDisplay', 'AdOrders_SponsoredDisplay', 'AdUnitsPerOrder_SponsoredDisplay', 0],
            ['OrganicUnits', 'OrganicOrders', 'OrganicUnitsPerOrder', 0],
            ] %}

{% set ns.array4 = [
    'ReturnedUnits'
] %}


with
    a_d_f_wd as (
        select      reportdate as date,
                    client_name,
                    countryname,
                    adtype,
                    ifnull(sum(adspend), 0) as AdSpend,
                    ifnull(sum(adsales), 0) as AdSales,
                    ifnull(sum(impressions), 0) as AdImpressions,
                    ifnull(sum(clicks), 0) as AdClicks,
                    ifnull(sum(conversions), 0) as AdOrders,
                    ifnull(sum(adunits), 0) as AdUnits,
        from
            (
                select *,
                    row_number() over (
                        partition by
                            primary_key
                        order by part_key desc
                    ) rn
                from {{ ref("a_d_f_wd") }}
            )
        where rn = 1
        group by 1,2,3,4
    ),
    a_d_f_wd_all as (
        select      date,
                    client_name,
                    countryname,
                    sum(AdSpend) AdSpend,
                    sum(AdSales) AdSales,
                    sum(AdImpressions) AdImpressions,
                    sum(AdClicks) AdClicks,
                    sum(AdOrders) AdOrders,
                    sum(AdUnits) AdUnits
                from a_d_f_wd
        group by 1,2,3
    ),
    m_d_f_wd as (
        select purchase_date as date,
                    client_name,
                    countryname,
                    ifnull(round(sum(totalsales), 0), 0) as TotalSales,
                    ifnull(cast(sum(units_orders) as numeric), 0) as Units,
                    ifnull(cast(sum(numberoforders) as numeric), 0) as Orders,
        from
            (
                select
                    *,
                    row_number() over (
                        partition by primary_key order by part_key desc
                    ) rn
                -- `pointstory.data_transformation_final.mws_data_final`
                from {{ ref("m_d_f_wd") }}
            )
        where rn = 1
        group by 1,2,3
    ),
    detailpagesales_traffic_by_child_asin_wd as (
        select purchase_date as date,
                    client_name,
                    countryname,
                    cast(ifnull(sum(cast(sessions as float64)), 0) as numeric) as Clicks,
        from
            (
                select
                    *,
                    row_number() over (
                        partition by purchase_date, parent_asin, asin
                        order by part_key desc
                    ) rn
                from {{ ref("detailPageSales_traffic_by_child_asin_wd") }}
            )
        where rn = 1
        group by 1,2,3
    ),
     
    cte as (
        select *
        from
            a_d_f_wd pivot (
                sum(AdSpend) as AdSpend,
                sum(AdSales) AdSales,
                sum(AdImpressions) AdImpressions,
                sum(AdClicks) AdClicks,
                sum(AdOrders) AdOrders,
                sum(AdUnits) AdUnits
                for adtype in (
                    'SponsoredProduct',
                    'SponsoredBrands',
                    'SponsoredDisplay',
                    'SponsoredBrandsVideo'
                )
            )

    ),

    OrganicTable as (
        SELECT *,
        CASE WHEN AdSales > TotalSales THEN 0 ELSE (TotalSales - AdSales) END as OrganicSales,
        CASE WHEN AdUnits > Units THEN 0 ELSE (Units - AdUnits) END as OrganicUnits,
        CASE WHEN AdOrders > Orders THEN 0 ELSE (Orders - AdOrders) END as OrganicOrders
        from m_d_f_wd
        left join a_d_f_wd_all using (date, client_name, countryname)
    ),
     
    ComboTable as (
        select *,
            {% for metric1, metric2, kpi, kind in ns.array3 %}
                IFNULL(SAFE_DIVIDE({{metric1}} , {{metric2}}), 0) as {{kpi}},
            {% endfor %}
        
            from OrganicTable
            left join detailpagesales_traffic_by_child_asin_wd using (date, client_name, countryname)
            left join cte using (date, client_name, countryname)
            -- left join offercount using (date, client_name, countryname)
    ),

    pre_final as (
        Select *,
                {% for kpi in ns.array1 %}
                    ifnull(
                        lag({{kpi}}, 7) over (
                            partition by client_name, countryname
                            order by unix_date(date)
                        ),
                        0
                    ) {{kpi}}_lag1w,
                    ifnull(
                        lag({{kpi}}, 14) over (
                            partition by client_name, countryname
                            order by unix_date(date)
                        ),
                        0
                    ) {{kpi}}_lag2w,
                    ifnull(
                        lag({{kpi}}, 21) over (
                            partition by client_name, countryname
                            order by unix_date(date)
                        ),
                        0
                    ) {{kpi}}_lag3w,
                    ifnull(
                        lag({{kpi}}, 28) over (
                            partition by client_name, countryname
                            order by unix_date(date)
                        ),
                        0
                    ) as {{kpi}}_lag4w,

                    ifnull(
                        lag({{kpi}}, 364) over (
                            partition by client_name, countryname
                            order by unix_date(date)
                        ),
                        0
                    ) {{kpi}}_lag52w,
                    AVG({{kpi}}) OVER(PARTITION BY client_name, countryname
                    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) {{kpi}}_30Avg,
                    AVG({{kpi}}) OVER(PARTITION BY client_name, countryname
                    ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING) {{kpi}}_90Avg,
            {% endfor %}

        from ComboTable
    )

    select * from pre_final
 