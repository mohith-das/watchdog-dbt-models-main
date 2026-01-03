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

{% set thresholdMapper = {
                            'AdSpend': 'AdSpend',
                            'AdSpend_SponsoredBrands': 'AdSpend',
                            'AdSpend_SponsoredDisplay': 'AdSpend',
                            'AdSpend_SponsoredProduct': 'AdSpend',

                            'AdImpressions': 'AdImpressions',
                            'AdImpressions_SponsoredBrands': 'AdImpressions',
                            'AdImpressions_SponsoredDisplay': 'AdImpressions',
                            'AdImpressions_SponsoredProduct': 'AdImpressions',


                            'Orders': 'Orders',
                            'AdOrders': 'Orders',
                            'AdOrders_SponsoredBrands': 'Orders',
                            'AdOrders_SponsoredDisplay': 'Orders',
                            'AdOrders_SponsoredProduct': 'Orders',
                            'OrganicOrders': 'Orders',


                            'AdSales': 'AdSales',
                            'AdSales_SponsoredBrands': 'AdSales',
                            'AdSales_SponsoredDisplay': 'AdSales',
                            'AdSales_SponsoredProduct': 'AdSales',
                            'OrganicSales': 'AdSales',
                            
                            'Clicks': 'Clicks',
                            'AdClicks': 'Clicks',
                            'AdClicks_SponsoredBrands': 'Clicks',
                            'AdClicks_SponsoredDisplay': 'Clicks',
                            'AdClicks_SponsoredProduct': 'Clicks',

                            'TotalSales': 'TotalSales',

                            'Units': 'Units',
                            'AdUnits': 'Units',
                            'AdUnits_SponsoredBrands': 'Units',
                            'AdUnits_SponsoredDisplay': 'Units',
                            'AdUnits_SponsoredProduct': 'Units',
                            'OrganicUnits': 'Units', 
                            
                            'ACOS': 'ACOS',
                            'ACOS_SponsoredBrands': 'ACOS',
                            'ACOS_SponsoredDisplay': 'ACOS',
                            'ACOS_SponsoredProduct': 'ACOS',

                            'AdAOV': 'AOV',
                            'AdAOV_SponsoredBrands': 'AOV',
                            'AdAOV_SponsoredDisplay': 'AOV',
                            'AdAOV_SponsoredProduct': 'AOV',
                            'OrganicAOV': 'AOV',

                            'AdCR':'AdCR',
                            'AdCR_SponsoredBrands':'AdCR',
                            'AdCR_SponsoredDisplay': 'AdCR',
                            'AdCR_SponsoredProduct': 'AdCR',

                            'AdCTR': 'AdCTR',
                            'AdCTR_SponsoredBrands': 'AdCTR',
                            'AdCTR_SponsoredDisplay': 'AdCTR',
                            'AdCTR_SponsoredProduct': 'AdCTR',

                            'AdUnitPrice': 'UnitPrice',
                            'AdUnitPrice_SponsoredBrands': 'UnitPrice',
                            'AdUnitPrice_SponsoredDisplay': 'UnitPrice',
                            'AdUnitPrice_SponsoredProduct': 'UnitPrice',
                            'OrganicUnitPrice': 'UnitPrice',

                            'ReturnRate': 'ReturnRate',
                            'TACOS': 'TACOS',

                            'AdUnitsPerOrder': 'UnitsPerOrder',
                            'AdUnitsPerOrder_SponsoredBrands': 'UnitsPerOrder',
                            'AdUnitsPerOrder_SponsoredDisplay': 'UnitsPerOrder',
                            'AdUnitsPerOrder_SponsoredProduct': 'UnitsPerOrder',
                            'OrganicUnitsPerOrder': 'UnitsPerOrder'



} %}

with
    pre_final as 
    (
        select * from {{ ref("h_acos_adtype_tree_wd1") }}
    ),

    thresholdstable as (
            select *
            from
                (
                    select
                        client_name client_name_t,
                        countryname countryname_t,
                        kpi_name,
                        month,
                        threshold_value,
                        threshold_percent
                    from `amz-atlas-client-warehouse.watchdog_dbt.threshold_latest`
                ) pivot (
                    sum(threshold_value) thv,
                    sum(threshold_percent) thp for kpi_name in (
                                'ACOS',
                                'AdSpend',
                                'AOV',
                                'AvgOfferCount',
                                'AdCR',
                                'AdCTR',
                                'AdImpressions',
                                'Orders',
                                'UnitPrice',
                                'ReturnRate',
                                'ReturnedUnits',
                                'AdSales',
                                'TotalSales',
                                'TACOS',
                                'Clicks',
                                'Units',
                                'UnitsPerOrder'
                    )
                )
    ),
     
    Avg_final1 as (
        select
            date,
            client_name,
            countryname,
            {% for kpi in ns.array1 + ns.array2 %}
                {{kpi}},
            {% endfor %}
            'Last 4 SDLW Avg' Comparison_Type, 
            {% for kpi in ns.array1 %}
                ({{kpi}}_lag1w + {{kpi}}_lag2w + {{kpi}}_lag3w + {{kpi}}_lag4w)/4  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                {% if kind == 0 %}
                    safe_divide(
                        {{metric1}}_lag1w + {{metric1}}_lag2w + {{metric1}}_lag3w + {{metric1}}_lag4w ,
                        {{metric2}}_lag1w + {{metric2}}_lag2w + {{metric2}}_lag3w + {{metric2}}_lag4w 
                    ) {{kpi}}_Avg,
                {% elif kind == 1 %}
                    safe_divide(
                            ({{metric1}}_lag1w + {{metric1}}_lag2w + {{metric1}}_lag3w + {{metric1}}_lag4w) *1 ,
                            {{metric2}}_lag1w + {{metric2}}_lag2w + {{metric2}}_lag3w + {{metric2}}_lag4w 
                        ) {{kpi}}_Avg,
                {% endif %}
            {% endfor %}

            from pre_final
    ),

    
     Avg_final2 as (
        select
            date,
            client_name,
            countryname,
            {% for kpi in ns.array1 + ns.array2 %}
                {{kpi}},
            {% endfor %}
            'SDLW' Comparison_Type, 
            {% for kpi in ns.array1 %}
                {{kpi}}_lag1w  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                {% if kind == 0 %}
                    safe_divide(
                        {{metric1}}_lag1w ,
                        {{metric2}}_lag1w 
                    ) {{kpi}}_Avg,
                {% elif kind == 1 %}
                    safe_divide(
                            {{metric1}}_lag1w  *1 ,
                            {{metric2}}_lag1w
                        ) {{kpi}}_Avg,
                {% endif %}
            {% endfor %}

            from pre_final
    ),

     
    Avg_final3 as (
        select
            date,
            client_name,
            countryname,
            {% for kpi in ns.array1 + ns.array2 %}
                {{kpi}},
            {% endfor %}
            'SDLY' Comparison_Type, 
            {% for kpi in ns.array1 %}
                {{kpi}}_lag52w  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                {% if kind == 0 %}
                    safe_divide(
                        {{metric1}}_lag52w ,
                        {{metric2}}_lag52w 
                    ) {{kpi}}_Avg,
                {% elif kind == 1 %}
                    safe_divide(
                            {{metric1}}_lag52w  *1 ,
                            {{metric2}}_lag52w
                        ) {{kpi}}_Avg,
                {% endif %}
            {% endfor %}

            from pre_final
    ),
     
   Avg_final4 as (
        select
            date,
            client_name,
            countryname,
            {% for kpi in ns.array1 + ns.array2 %}
                {{kpi}},
            {% endfor %}
            'Last 30d Avg' Comparison_Type, 
            {% for kpi in ns.array1 %}
                {{kpi}}_30Avg  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                {% if kind == 0 %}
                    safe_divide(
                        {{metric1}}_30Avg ,
                        {{metric2}}_30Avg
                    ) {{kpi}}_Avg,
                {% elif kind == 1 %}
                    safe_divide(
                            {{metric1}}_30Avg  *1 ,
                            {{metric2}}_30Avg
                        ) {{kpi}}_Avg,
                {% endif %}
            {% endfor %}
            from pre_final
    ),

 
    Avg_final5 as (
        select
            date,
            client_name,
            countryname,
            {% for kpi in ns.array1 + ns.array2 %}
                {{kpi}},
            {% endfor %}
            'Last 90d Avg' Comparison_Type, 
            {% for kpi in ns.array1 %}
                {{kpi}}_90Avg  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                {% if kind == 0 %}
                    safe_divide(
                        {{metric1}}_90Avg ,
                        {{metric2}}_90Avg
                    ) {{kpi}}_Avg,
                {% elif kind == 1 %}
                    safe_divide(
                            {{metric1}}_90Avg  *1 ,
                            {{metric2}}_90Avg
                        ) {{kpi}}_Avg,
                {% endif %}
            {% endfor %}

            from pre_final
    ),
 
    Avg_final as (
        select * from Avg_final1 
        UNION all
        select * from Avg_final2
        UNION all
        select * from Avg_final3
        UNION all
        select * from Avg_final4
        UNION all
        select * from Avg_final5
    ),

    -- where Date ='2023-01-03' and client_name ='Else' and countryname ='United
    -- States'
    -- select * from last
     
    var_final as(
                select
                    *,
                    {% for kpi in ns.array1 %}
                        ifnull(
                            safe_divide(
                                ({{kpi}} - {{kpi}}_Avg) * 1,
                                {{kpi}}_Avg
                            ),
                            0
                        ) {{kpi}}_var,
                    {% endfor %}   

                    {% for kpi in ns.array2 %}
                        ifnull(
                            safe_divide(
                                ({{kpi}} - {{kpi}}_Avg) * 1,
                                {{kpi}}_Avg
                            ),
                            0
                        ) {{kpi}}_var,
                    {% endfor %}                 
                from Avg_final
    ),
     
    status_final as (
        select
            *,
            {% for kpi in ns.array1 %}
                case
                when
                    {{kpi}}_var < - thp_{{thresholdMapper[kpi]}}
                    and abs({{kpi}} - {{kpi}}_Avg)
                    > thv_{{thresholdMapper[kpi]}}
                then -1
                when
                    {{kpi}}_var > thp_{{thresholdMapper[kpi]}}
                    and abs({{kpi}} - {{kpi}}_Avg)
                    > thv_{{thresholdMapper[kpi]}}
                then 1
                else 0
                end {{kpi}}_status,
            {% endfor %}

            {% for kpi in ns.array2 %}
                    case
                    when
                        {{kpi}}_var < - thp_{{thresholdMapper[kpi]}}
                        and abs({{kpi}} - {{kpi}}_Avg)
                        > thv_{{thresholdMapper[kpi]}}
                    then -1
                    when
                        {{kpi}}_var > thp_{{thresholdMapper[kpi]}}
                        and abs({{kpi}} - {{kpi}}_Avg)
                        > thv_{{thresholdMapper[kpi]}}
                    then 1
                    else 0
                    end {{kpi}}_status,
            {% endfor %}
        from var_final vf
         left join
                    thresholdstable tt
                    on vf.client_name = tt.client_name_t
                    and vf.countryname = tt.countryname_t
                where format_datetime("%B", vf.date) = tt.month
    )

select distinct
    date,
    client_name,
    countryname,
    Comparison_Type,
    {% for kpi in ns.array1 + ns.array2 %}
        CASE WHEN {{kpi}} = 0 THEN 0.0000001 ELSE coalesce({{kpi}}, 0.0000001) end as {{kpi}},
        CASE WHEN {{kpi}}_var = 0 THEN 0.0000001 ELSE coalesce({{kpi}}_var, 0.0000001) end as {{kpi}}_var,
        coalesce({{kpi}}_status, 0) as {{kpi}}_status,
        CASE WHEN {{kpi}}_Status = 0 THEN CASE WHEN {{kpi}} = 0 THEN 0.0000001/0.8
        ELSE coalesce({{kpi}}/0.8, 0.0000001/0.8) end
             WHEN {{kpi}}_Status = 1 THEN CASE WHEN {{kpi}} = 0 THEN 0.0000001/1.4 ELSE {{kpi}}/1.4 end
             WHEN {{kpi}}_Status = -1 then CASE WHEN {{kpi}} = 0 THEN 0.0000001/0.6 ELSE {{kpi}}/0.6 end
             end as {{kpi}}_Target1,
        CASE WHEN {{kpi}}_Status = 0 THEN 
                    CASE WHEN {{kpi}}_Var >= 0 THEN {{kpi}}_Var/0.2 ELSE {{kpi}}_Var/(-0.2) END 
            WHEN  {{kpi}}_Status =1 THEN {{kpi}}_Var/0.4 ELSE {{kpi}}_Var/(-0.4) END {{kpi}}_Target2,
    {% endfor %}
    -- ifnull(average_offer_count,0) || " (" || ifnull(average_offer_count_var,0) || "%" || ")" as average_offer_count_fin,
    -- average_offer_count_status
from status_final

-- where Date ='2023-02-05' 
-- and client_name ='Else' and countryname ='United States'
-- order by date desc
-- client_name, countryname