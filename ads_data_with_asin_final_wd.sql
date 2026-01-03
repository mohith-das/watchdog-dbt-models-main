{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "part_key",
            "data_type": "timestamp",
            "granularity": "day",
        },
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

{%- set this_max_loaded_query -%} SELECT MAX(DATE({{ns.part_key}})) FROM {{ this }} {% endset %}
{%- set this_max_loaded_results = run_query(this_max_loaded_query) -%}
{% set ns.this_max_loaded = this_max_loaded_results.rows[0].values()[0] %}
{% do log("this_models:", info=true) %} 
{% do log(ns.this_max_loaded, info=true) %} 

{% set ns.parent_models = ns.unique_refs %}

{% do log("parent_models:", info=true) %} 
{% do log(ns.parent_models, info=true) %} 

    {% for model_name in ns.parent_models %} 

        {% set ns.query %} SELECT MAX(DATE({{ns.part_key}})) - 1 FROM {{ ref(model_name) }} {% endset %} 
        {% set query_result = run_query(ns.query) %}
        {% set min_dbr = query_result.rows[0].values()[0] %}

        {% if not ns.min or ns.min > min_dbr %}
            {% set ns.min = min_dbr %}
        {%endif%}
    {% endfor %}

  {% if not ns.min or ns.min > ns.this_max_loaded %}
            {% set ns.min = ns.this_max_loaded %}
  {%endif%}

{% set ns.max_loaded = ns.min %}
{% do log("min_of_max_loaded:", info=true) %} 
{% do log(ns.max_loaded, info=true) %} 

{% else %}
{% set ns.max_loaded = 0 %}
{%- endif -%}
{% endif %}


with
    sp as (
        select
            primary_key,
            ad.reportdate,
            ad.client_name,
            -- ad.client_name as Business_name,
            ad.countryname,
            ad.asin,
            ad.campaignId,
            ad.campaignName,
            -- ac.currencycode as currency,
            'SponsoredProduct' as adtype,
            case
                when
                    lower(ad.client_name) like '%night%'
                    or lower(ad.client_name) like '%first%'
                then ad.attributedsales14d
                else ad.attributedsales7d
            end as adsales,
            cast(ad.cost as numeric) adspend,
            attributedconversions7d conversions,
            ad.clicks clicks,
            cast(ad.impressions as numeric) impressions,
            part_key,

        -- CAST(pt.name as STRING) PortfolioName,
        -- c.profileId,
        -- ad.campaignName,
        from
            -- pointstory.data_transformation.SponsoredProducts_ProductAdsReport_Consolidated ad
            -- pointstory.data_transformation.sp_productadsreport_con_wd ad
            (
                select
                    *,
                    row_number() over (
                        partition by
                            reportdate, client_name, campaignid, adgroupid, asin, adid
                        order by _daton_batch_runtime desc
                    ) rn
                from {{ ref("sp_productadsreport_con_wd") }} ad
                {% if is_incremental() %}
                where date(part_key) >= date('{{ns.max_loaded}}')
                {% endif %}
            ) ad
        where rn = 1
    ),

    sb as (
        select
            primary_key,
            cast(ad.reportdate as date) as reportdate,
            ad.client_name,
            -- ad.client_name as Business_name,
            ad.countryname,
            'NA' as asin,
            ad.campaignId,
            ad.campaignName,
            -- ac.currencyCode as currency,
            'SponsoredBrands' as adtype,
            cast(ad.attributedsales14d as int64) adsales,
            cast(ad.cost as numeric) adspend,
            ad.attributedconversions14d conversions,
            cast(ad.clicks as numeric) clicks,
            cast(ad.impressions as numeric) impressions,
            part_key
        -- CAST(pt.name AS STRING) AS portfolioName,
        -- ad.profileId,
        -- ad.campaignName,
        -- pointstory.data_transformation.SponsoredBrands_CampaignsReport_Consolidated
        -- ad
        -- pointstory.data_transformation.sb_cr_con_wd ad
        from
            (
                select
                    *,
                    row_number() over (
                        partition by campaignid, reportdate
                        order by _daton_batch_runtime desc
                    ) rn
                from {{ ref("sb_cr_con_wd") }}
                {% if is_incremental() %}
                where date(part_key) >= date('{{ns.max_loaded}}')
                {% endif %}
            ) ad
        where rn = 1

    ),

    sbv as (
        select
            primary_key,
            ad.reportdate,
            ad.client_name,
            ad.countryname,
            'NA' as asin,
            ad.campaignId,
            ad.campaignName,
            'SponsoredBrands Video' as adtype,
            ad.attributedsales14d adsales,
            cast(ad.cost as numeric) adspend,
            ad.attributedconversions14d conversions,
            cast(ad.clicks as numeric) clicks,
            cast(ad.impressions as numeric) impressions,
            part_key
        from
            (
                select
                    *,
                    row_number() over (
                        partition by campaignid, reportdate
                        order by _daton_batch_runtime desc
                    ) rn
                from {{ ref("sb_cvr_con_wd") }}
                {% if is_incremental() %}
                where date(part_key) >= date('{{ns.max_loaded}}')
                {% endif %}
            ) ad
        where rn = 1

    ),


    sd as (
        select
            primary_key,
            cast(ad.reportdate as date) as reportdate,
            ad.client_name,
            -- ad.client_name as Business_name,
            ad.countryname,
            ad.asin,
            ad.campaignId,
            ad.campaignName,
            -- ac.currencyCode as currency,
            'SponsoredDisplay' as adtype,
            ad.attributedsales14d adsales,
            cast(ad.cost as numeric) adspend,
            ad.attributedconversions14d conversions,
            ad.clicks clicks,
            cast(ad.impressions as numeric) impressions,
            part_key
        -- CAST(pt.name as STRING) as PortfolioName,
        -- CAST(sdc.profileId as string) as profileId,
        -- ad.campaignName,
        -- pointstory.data_transformation.SponsoredDisplay_ProductAdsReport_Consolidated ad
        -- pointstory.data_transformation.sd_productadsreport_con_wd ad
        from
            (
                select
                    *,
                    row_number() over (
                        partition by reportdate, campaignid, adgroupid, adid, asin, sku
                        order by _daton_batch_runtime desc
                    ) rn
                from {{ ref("sd_productadsreport_con_wd") }}
                {% if is_incremental() %}
                where date(part_key) >= date('{{ns.max_loaded}}')
                {% endif %}
            ) ad
        where rn = 1

    ),
    cte as (
        select *
        from sp
        union all
        select *
        from sb
        union all
        select *
        from sbv
        union all
        select *
        from sd
        where lower(client_name) not in ('krounds', 'matisse', 'champion')
    ),
    dedup as (
        -- reportdate, client_name, countryname, adtype, asin
        select *, dense_rank() over (partition by primary_key order by part_key desc) rn
        from cte

    ),
    final as (
        select
            {% if is_incremental() %}
                current_timestamp() as part_key,    
            {% else %}
                TIMESTAMP_SUB(current_timestamp(), INTERVAL 1 Day) as part_key,
            {% endif %}
            reportdate,
            client_name,
            countryname,
            asin,
            campaignId,
            campaignName,
            adtype,
            -- rn,
            sum(adsales) as adsales,
            sum(adspend) as adspend,
            sum(conversions) as conversions,
            sum(clicks) as clicks,
            sum(impressions) as impressions,
        from dedup
        -- client_name = "Else"
        -- and countryname = "United States"
        -- and adtype = "SponsoredProduct"
        -- and reportdate = '2023-02-28'
        -- and asin = 'B0B44S2X51'
        -- and
        -- {#{ dbt_utils.group_by(4) }#}
        where rn = 1 {{ dbt_utils.group_by(8) }}
    )
select
    *,
    {{
        dbt_utils.surrogate_key(
            ["reportDate", "client_name", "countryname", "Adtype", "asin"]
        )
    }} as primary_key
from final
