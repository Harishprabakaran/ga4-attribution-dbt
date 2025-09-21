
-- models/marts/mart_attribution_summary.sql
{{ config(
    materialized='table',
    partition_by={
        "field": "event_date",
        "data_type": "date"
    },
    cluster_by=["attributed_channel"],
    description='Daily aggregated attribution metrics comparing first vs last click models'
) }}

with first_click_daily as (
    select
        event_date,
        attributed_channel,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        conversion_event,
        conversion_type,
        
        count(*) as first_click_conversions,
        sum(attributed_revenue) as first_click_revenue,
        count(distinct user_pseudo_id) as first_click_unique_users,
        count(distinct transaction_id) as first_click_transactions,
        
        -- Journey insights for first-click
        avg(total_touchpoints) as avg_touchpoints_first_click,
        countif(journey_complexity = 'single_touch') as single_touch_conversions_fc,
        countif(attribution_comparison = 'same_channel') as same_channel_conversions_fc
        
    from {{ ref('mart_attribution_first_click') }}
    group by 1, 2, 3, 4, 5, 6, 7
),

last_click_daily as (
    select
        event_date,
        attributed_channel, 
        attributed_source,
        attributed_medium,
        attributed_campaign,
        conversion_event,
        conversion_type,
        
        count(*) as last_click_conversions,
        sum(attributed_revenue) as last_click_revenue,
        count(distinct user_pseudo_id) as last_click_unique_users,
        count(distinct transaction_id) as last_click_transactions,
        
        -- Journey insights for last-click
        avg(total_touchpoints) as avg_touchpoints_last_click,
        countif(journey_complexity = 'single_touch') as single_touch_conversions_lc,
        countif(attribution_comparison = 'same_channel') as same_channel_conversions_lc
        
    from {{ ref('mart_attribution_last_click') }}
    group by 1, 2, 3, 4, 5, 6, 7
),

attribution_comparison as (
    select
        coalesce(f.event_date, l.event_date) as event_date,
        coalesce(f.attributed_channel, l.attributed_channel) as attributed_channel,
        coalesce(f.attributed_source, l.attributed_source) as attributed_source,
        coalesce(f.attributed_medium, l.attributed_medium) as attributed_medium,
        coalesce(f.attributed_campaign, l.attributed_campaign) as attributed_campaign,
        coalesce(f.conversion_event, l.conversion_event) as conversion_event,
        coalesce(f.conversion_type, l.conversion_type) as conversion_type,
        
        -- First-click metrics
        coalesce(f.first_click_conversions, 0) as first_click_conversions,
        coalesce(f.first_click_revenue, 0) as first_click_revenue,
        coalesce(f.first_click_unique_users, 0) as first_click_unique_users,
        coalesce(f.first_click_transactions, 0) as first_click_transactions,
        
        -- Last-click metrics  
        coalesce(l.last_click_conversions, 0) as last_click_conversions,
        coalesce(l.last_click_revenue, 0) as last_click_revenue,
        coalesce(l.last_click_unique_users, 0) as last_click_unique_users,
        coalesce(l.last_click_transactions, 0) as last_click_transactions,
        
        -- Attribution model comparison
        case 
            when coalesce(f.first_click_revenue, 0) > coalesce(l.last_click_revenue, 0) 
            then 'first_click_higher'
            when coalesce(f.first_click_revenue, 0) < coalesce(l.last_click_revenue, 0)
            then 'last_click_higher'
            else 'equal_attribution'
        end as revenue_attribution_comparison,
        
        -- Revenue difference
        coalesce(l.last_click_revenue, 0) - coalesce(f.first_click_revenue, 0) as revenue_difference_lc_vs_fc,
        
        -- Percentage difference
        case 
            when coalesce(f.first_click_revenue, 0) > 0 
            then (coalesce(l.last_click_revenue, 0) - coalesce(f.first_click_revenue, 0)) / f.first_click_revenue * 100
            else null
        end as revenue_pct_difference_lc_vs_fc,
        
        -- Journey complexity comparison
        coalesce(f.avg_touchpoints_first_click, l.avg_touchpoints_last_click) as avg_touchpoints,
        coalesce(f.single_touch_conversions_fc, 0) + coalesce(l.single_touch_conversions_lc, 0) as total_single_touch_conversions,
        coalesce(f.same_channel_conversions_fc, 0) + coalesce(l.same_channel_conversions_lc, 0) as total_same_channel_conversions,
        
        current_timestamp() as _created_at
        
    from first_click_daily f
    full outer join last_click_daily l
        on f.event_date = l.event_date
        and f.attributed_channel = l.attributed_channel
        and f.attributed_source = l.attributed_source
        and f.attributed_medium = l.attributed_medium
        and f.attributed_campaign = l.attributed_campaign
        and f.conversion_event = l.conversion_event
        and f.conversion_type = l.conversion_type
)

select * from attribution_comparison
