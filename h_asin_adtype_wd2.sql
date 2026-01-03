{{ config(materialized="table", schema="final") }}
{% set ns = namespace() %}
{% set ns.array0 = ['Date', 'client_name', 'countryname', 'asin', 'product_name', 'Link', 'AdType' ]%}
{% set ns.array1 = ['TotalSales', 'Units', 'Orders', 'Clicks', 'AdSales', 'AdSpend',  'AdOrders',
 'AdClicks',  'AdImpressions', 'BuyBoxPercent' ]
%}
{% set ns.array2 = ['ACOS', 'AdCR', 'AdCTR'] %}

{% set ns.array3 = [
            ['AdSpend', 'AdSales', 'ACOS', 0],
            ['AdOrders', 'AdClicks', 'AdCR', 0],
            ['Clicks', 'AdImpressions', 'AdCTR', 0],
            ['ReturnedUnits', 'Units', 'ReturnRate', -1],
            ['StockUnits', 'AvgUnitsSold', 'DaysCoverage', -1]
            ] %}

{% set ns.array4 = [
    'AvgUnitsSold',
    'ReturnedUnits', 
    'StockUnits',
] %}

{% set ns.array5 = [
    'ReturnRate', 
    'DaysCoverage',
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



with
    ComboTable as 
    (
        select * from {{ ref("h_asin_adtype_wd1") }}
    ),

    pre_final_1w as (
        select ct1.*,
        {% for kpi in ns.array1 %}
            ifnull(ct2.{{kpi}}, 0) {{kpi}}_lag1w,
        {% endfor %}
        from ComboTable ct1 join ComboTable ct2
        on ct1.client_name = ct2.client_name and 
            ct1.countryname = ct2.countryname and 
            ct1.asin = ct2.asin and 
            ct1.product_name = ct2.product_name and 
            ct1.Link = ct2.Link and
            ct1.adtype = ct2.adtype and 
            ct2.date = ct1.date - 7
    ),
    pre_final_2w as (
        select ct1.*,
        {% for kpi in ns.array1 %}
            ifnull(ct2.{{kpi}}, 0) {{kpi}}_lag2w,
        {% endfor %}
        from ComboTable ct1 join ComboTable ct2
        on ct1.client_name = ct2.client_name and 
            ct1.countryname = ct2.countryname and 
            ct1.asin = ct2.asin and 
            ct1.product_name = ct2.product_name and 
            ct1.Link = ct2.Link and
            ct1.adtype = ct2.adtype and 
            ct2.date = ct1.date - 14
    ),
    pre_final_3w as (
        select ct1.*,
        {% for kpi in ns.array1 %}
            ifnull(ct2.{{kpi}}, 0) {{kpi}}_lag3w,
        {% endfor %}
        from ComboTable ct1 join ComboTable ct2
        on ct1.client_name = ct2.client_name and 
            ct1.countryname = ct2.countryname and 
            ct1.asin = ct2.asin and 
            ct1.product_name = ct2.product_name and 
            ct1.Link = ct2.Link and
            ct1.adtype = ct2.adtype and 
            ct2.date = ct1.date - 21
    ),
    pre_final_4w as (
        select ct1.*,
        {% for kpi in ns.array1 %}
            ifnull(ct2.{{kpi}}, 0) {{kpi}}_lag4w,
        {% endfor %}
        from ComboTable ct1 join ComboTable ct2
        on ct1.client_name = ct2.client_name and 
            ct1.countryname = ct2.countryname and 
            ct1.asin = ct2.asin and 
            ct1.product_name = ct2.product_name and 
            ct1.Link = ct2.Link and
            ct1.adtype = ct2.adtype and 
            ct2.date = ct1.date - 28
    ),
    pre_final_52w as (
        select ct1.*,
        {% for kpi in ns.array1 %}
            ifnull(ct2.{{kpi}}, 0) {{kpi}}_lag52w,
        {% endfor %}
        from ComboTable ct1 join ComboTable ct2
        on ct1.client_name = ct2.client_name and 
            ct1.countryname = ct2.countryname and 
            ct1.asin = ct2.asin and 
            ct1.product_name = ct2.product_name and 
            ct1.Link = ct2.Link and
            ct1.adtype = ct2.adtype and 
            ct2.date = ct1.date - 364
    ),

    pre_final_30d as (
        select ct1.*,
        {% for kpi in ns.array1 %}
            AVG(ifnull(ct2.{{kpi}}, 0)) {{kpi}}_30Avg,
        {% endfor %}
        from ComboTable ct1 join ComboTable ct2
        on ct1.client_name = ct2.client_name and 
            ct1.countryname = ct2.countryname and 
            ct1.asin = ct2.asin and 
            ct1.product_name = ct2.product_name and 
            ct1.Link = ct2.Link and
            ct1.adtype = ct2.adtype and 
            ct2.date >= ct1.date - 30 and ct2.date != ct1.date
        group by date, client_name, countryname, asin, product_name, Link, adtype, 
        {% for kpi in ns.array1 + ns.array2 + ns.array4 + ns.array5 %}
            {{kpi}} {% if not loop.last %} , {% endif %}
        {% endfor %}
    ),

    pre_final_90d as (
        select ct1.*,
        {% for kpi in ns.array1 %}
            AVG(ifnull(ct2.{{kpi}}, 0)) {{kpi}}_90Avg,
        {% endfor %}
        from ComboTable ct1 join ComboTable ct2
             on ct1.client_name = ct2.client_name and 
            ct1.countryname = ct2.countryname and 
            ct1.asin = ct2.asin and 
            ct1.product_name = ct2.product_name and 
            ct1.Link = ct2.Link and
            ct1.adtype = ct2.adtype and 
            ct2.date >= ct1.date - 90 and ct2.date != ct1.date
        group by date, client_name, countryname, asin, product_name, Link, adtype, 
        {% for kpi in ns.array1 + ns.array2  + ns.array4 + ns.array5 %}
            {{kpi}} {% if not loop.last %} , {% endif %}
        {% endfor %}
    ),

    pre_final as (
        select 
        {% for kpi in ns.array0 %}
            coalesce(pre_final_1w.{{kpi}}, pre_final_2w.{{kpi}}, pre_final_3w.{{kpi}}, pre_final_4w.{{kpi}}, pre_final_52w.{{kpi}}, pre_final_30d.{{kpi}}, pre_final_90d.{{kpi}}, '2000-01-01') {{kpi}},
        {% endfor %}
        {% for kpi in ns.array1 + ns.array4 + ns.array2 + ns.array5 %}
            coalesce(pre_final_1w.{{kpi}}, pre_final_2w.{{kpi}}, pre_final_3w.{{kpi}}, pre_final_4w.{{kpi}}, pre_final_52w.{{kpi}}, pre_final_30d.{{kpi}}, pre_final_90d.{{kpi}}, 0) {{kpi}},
        {% endfor %}

        {% for kpi in ns.array1 %}
            ifnull(pre_final_1w.{{kpi}}_lag1w, 0) {{kpi}}_lag1w,
            ifnull(pre_final_2w.{{kpi}}_lag2w, 0) {{kpi}}_lag2w,
            ifnull(pre_final_3w.{{kpi}}_lag3w, 0) {{kpi}}_lag3w,
            ifnull(pre_final_4w.{{kpi}}_lag4w, 0) {{kpi}}_lag4w,
            ifnull(pre_final_52w.{{kpi}}_lag52w, 0) {{kpi}}_lag52w,
            ifnull(pre_final_30d.{{kpi}}_30Avg, 0) {{kpi}}_30Avg,
            ifnull(pre_final_90d.{{kpi}}_90Avg, 0) {{kpi}}_90Avg,
        {% endfor %}
        from pre_final_1w 
        full join pre_final_2w using (date, client_name, countryname, asin, product_name, Link, adtype)
        full join pre_final_3w using (date, client_name, countryname, asin, product_name, Link, adtype)
        full join pre_final_4w using (date, client_name, countryname, asin, product_name, Link, adtype)
        full join pre_final_52w using (date, client_name, countryname, asin, product_name, Link, adtype)
        full join pre_final_30d using (date, client_name, countryname, asin, product_name, Link, adtype)
        full join pre_final_90d using (date, client_name, countryname, asin, product_name, Link, adtype)
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
                            'BuyBoxPercent',
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
            asin,
            product_name,
            link,
            adtype,
            {% for kpi in ns.array1 + ns.array2 + ns.array4 + ns.array5 %}
                {{kpi}},
            {% endfor %}
            'Last 4 SDLW Avg' Comparison_Type, 
            {% for kpi in ns.array1 %}
                ({{kpi}}_lag1w + {{kpi}}_lag2w + {{kpi}}_lag3w + {{kpi}}_lag4w)/4  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                    {% if kind == 0 %}
                        CASE WHEN ({{metric2}}_lag1w + {{metric2}}_lag2w + {{metric2}}_lag3w + {{metric2}}_lag4w  = 0 
                        and {{metric1}}_lag1w + {{metric1}}_lag2w + {{metric1}}_lag3w + {{metric1}}_lag4w > 0 ) Then 1
                        Else
                            ifnull(safe_divide(
                                {{metric1}}_lag1w + {{metric1}}_lag2w + {{metric1}}_lag3w + {{metric1}}_lag4w ,
                                {{metric2}}_lag1w + {{metric2}}_lag2w + {{metric2}}_lag3w + {{metric2}}_lag4w 
                            ),0) END {{kpi}}_Avg,
                    {% elif kind == 1 %}
                        CASE WHEN ({{metric2}}_lag1w + {{metric2}}_lag2w + {{metric2}}_lag3w + {{metric2}}_lag4w  = 0 
                        and {{metric1}}_lag1w + {{metric1}}_lag2w + {{metric1}}_lag3w + {{metric1}}_lag4w > 0 ) Then 100
                        Else
                            ifnull(safe_divide(
                                ({{metric1}}_lag1w + {{metric1}}_lag2w + {{metric1}}_lag3w + {{metric1}}_lag4w) *100 ,
                                {{metric2}}_lag1w + {{metric2}}_lag2w + {{metric2}}_lag3w + {{metric2}}_lag4w 
                            ),0) End {{kpi}}_Avg,
                    {% endif %}
            {% endfor %}

            from pre_final
    ),

    
    Avg_final2 as (
        select
            date,
            client_name,
            countryname,
            asin,
            product_name,
            link,
            adtype,
            {% for kpi in ns.array1 + ns.array2 + ns.array4 + ns.array5 %}
                {{kpi}},
            {% endfor %}
            'SDLW' Comparison_Type, 
            {% for kpi in ns.array1 %}
                {{kpi}}_lag1w  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                    {% if kind == 0 %}
                        CASE WHEN ({{metric2}}_lag1w = 0 and {{metric1}}_lag1w > 0) Then 1
                        ELSE
                            ifnull(safe_divide(
                                {{metric1}}_lag1w ,
                                {{metric2}}_lag1w 
                            ),0) END {{kpi}}_Avg,
                    {% elif kind == 1 %}
                        CASE WHEN ({{metric2}}_lag1w = 0 and {{metric1}}_lag1w > 0) Then 100
                            ELSE
                                ifnull(safe_divide(
                                        {{metric1}}_lag1w  *100 ,
                                        {{metric2}}_lag1w
                                ),0) END {{kpi}}_Avg,
                    {% endif %}
            {% endfor %}

            from pre_final
    ),

     
    Avg_final3 as (
        select
            date,
            client_name,
            countryname,
            asin,
            product_name,
            link,
            adtype,
            {% for kpi in ns.array1 + ns.array2 + ns.array4 + ns.array5 %}
                {{kpi}},
            {% endfor %}
            'SDLY' Comparison_Type, 
            {% for kpi in ns.array1 %}
                {{kpi}}_lag52w  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                    {% if kind == 0 %}
                        CASE WHEN ({{metric2}}_lag52w = 0 and {{metric1}}_lag52w > 0) Then 1
                            ELSE
                                ifnull(safe_divide(
                                    {{metric1}}_lag52w ,
                                    {{metric2}}_lag52w 
                                ),0) END {{kpi}}_Avg,
                    {% elif kind == 1 %}
                        CASE WHEN ({{metric2}}_lag52w = 0 and {{metric1}}_lag52w > 0) Then 100
                            ELSE
                            ifnull(safe_divide(
                                    {{metric1}}_lag52w  *100 ,
                                    {{metric2}}_lag52w
                                ),0) END {{kpi}}_Avg,
                    {% endif %}
            {% endfor %}

            from pre_final
    ),
    Avg_final4 as (
        select
            date,
            client_name,
            countryname,
            asin,
            product_name,
            link,
            adtype,
            {% for kpi in ns.array1 + ns.array2 + ns.array4 + ns.array5 %}
                {{kpi}},
            {% endfor %}
            'Last 30d Avg' Comparison_Type, 
            {% for kpi in ns.array1 %}
                {{kpi}}_30Avg  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                    {% if kind == 0 %}
                        CASE WHEN ({{metric2}}_30Avg = 0 and {{metric1}}_30Avg > 0) Then 1
                            ELSE
                                ifnull(safe_divide(
                                    {{metric1}}_30Avg ,
                                    {{metric2}}_30Avg
                                ),0) END {{kpi}}_Avg,
                    {% elif kind == 1 %}
                        CASE WHEN ({{metric2}}_30Avg = 0 and {{metric1}}_30Avg > 0) Then 100
                            ELSE
                            ifnull(safe_divide(
                                    {{metric1}}_30Avg  *100 ,
                                    {{metric2}}_30Avg
                                ),0) END {{kpi}}_Avg,
                    {% endif %}
            {% endfor %}
            from pre_final
    ),

    Avg_final5 as (
        select
            date,
            client_name,
            countryname,
            asin,
            product_name,
            link,
            adtype,
            {% for kpi in ns.array1 + ns.array2 + ns.array4 + ns.array5 %}
                {{kpi}},
            {% endfor %}
            'Last 90d Avg' Comparison_Type, 
            {% for kpi in ns.array1 %}
                {{kpi}}_90Avg  as {{kpi}}_Avg,
            {% endfor %}

            {% for metric1, metric2, kpi, kind in ns.array3 %}
                    {% if kind == 0 %}
                        CASE WHEN ({{metric2}}_90Avg = 0 and {{metric1}}_90Avg > 0) Then 1
                            ELSE
                                safe_divide(
                                    ifnull({{metric1}}_90Avg ,
                                    {{metric2}}_90Avg
                                ),0) END {{kpi}}_Avg,
                    {% elif kind == 1 %}
                        CASE WHEN ({{metric2}}_90Avg = 0 and {{metric1}}_90Avg > 0) Then 100
                            ELSE
                            ifnull(safe_divide(
                                    {{metric1}}_90Avg  *100 ,
                                    {{metric2}}_90Avg
                                ),0) END {{kpi}}_Avg,
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
                        CASE WHEN {{kpi}} != 0 THEN
                            ifnull(
                                safe_divide(
                                    ({{kpi}} - {{kpi}}_Avg) * 1,
                                    {{kpi}}_Avg
                                ),
                                1    {# If kpi is not 0 then null because of kpi_avg so making it 100% instead 0 #}
                            ) ELSE
                            ifnull(
                                safe_divide(
                                    ({{kpi}} - {{kpi}}_Avg) * 1,
                                    {{kpi}}_Avg
                                ),
                                0
                            )
                        END {{kpi}}_var,
                    {% endfor %}   

                    {% for kpi in ns.array2 %}
                        CASE WHEN {{kpi}} != 0 THEN
                            ifnull(
                                safe_divide(
                                    ({{kpi}} - {{kpi}}_Avg) * 1,
                                    {{kpi}}_Avg
                                ),
                                1
                            )ELSE
                            ifnull(
                                safe_divide(
                                    ({{kpi}} - {{kpi}}_Avg) * 1,
                                    {{kpi}}_Avg
                                ),
                                0
                            )
                        END {{kpi}}_var,
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

    select 
    date,
    client_name,
    countryname,
    asin,
    product_name,
    link,
    adtype,
    Comparison_Type, 
    {% for kpi in ns.array1 + ns.array2 %}
        ifnull({{kpi}}, 0) {{kpi}},
        ifnull({{kpi}}_var, 0) {{kpi}}_var,
        {{kpi}}_status,
    {% endfor %}
    {% for kpi in ns.array4 + ns.array5 %}
        ifnull({{kpi}}, 0) {{kpi}},
    {% endfor %}
    thp_BuyBoxPercent/100 thp_BuyBoxPercent,
    thp_ReturnRate 
 from status_final
    where AdSales_Status != 0 or AdOrders_Status != 0 or AdCR_Status != 0 or AdClicks_Status != 0 
    or AdImpressions_Status != 0 or ACOS_status != 0 or  AdSpend_Status != 0 or BuyBoxPercent < thp_BuyBoxPercent/100
    or ReturnRate > thp_ReturnRate

--where  Comparison_Type = 'SDLW' and 
-- where adtype = 'SponsoredProduct' and
-- client_name = 'AlwaysPrepared' and date in ('2023-03-20', '2023-03-13', '2023-03-06', '2023-02-27', '2023-02-20')
-- and asin = 'B072MGHY1W' 

-- order by date desc