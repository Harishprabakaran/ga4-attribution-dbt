-- models/staging/stg_ga4_sessions.sql
{{ config(
    materialized='view', 
    description='Session-level aggregation of GA4 events for attribution analysis'
) }}

with session_events as (
     select
        user_pseudo_id,
        user_id,
        session_id,
        event_date,
        
        -- Session timing
        min(event_timestamp) as session_start_timestamp,
        max(event_timestamp) as session_end_timestamp,
        datetime(timestamp_micros(min(event_timestamp))) as session_start_datetime,
        datetime(timestamp_micros(max(event_timestamp))) as session_end_datetime,
        
        -- Session duration in minutes
        (max(event_timestamp) - min(event_timestamp)) / 1000000 / 60 as session_duration_minutes,
        
        -- Traffic source (first event in session)
        array_agg(traffic_source order by event_timestamp limit 1)[offset(0)] as session_traffic_source,
        array_agg(traffic_medium order by event_timestamp limit 1)[offset(0)] as session_traffic_medium,
        array_agg(traffic_campaign order by event_timestamp limit 1)[offset(0)] as session_traffic_campaign,
        
        -- Page information
        array_agg(page_location ignore nulls order by event_timestamp limit 1)[offset(0)] as landing_page,
        array_agg(page_location ignore nulls order by event_timestamp desc limit 1)[offset(0)] as exit_page,
        count(distinct page_location) as unique_pages_viewed,
        
        -- Engagement metrics
        count(*) as total_events,
        countif(event_name = 'page_view') as page_views,
        sum(coalesce(engagement_time_msec, 0)) / 1000 as total_engagement_seconds,
        
        -- Conversion metrics
        countif(event_name = 'purchase') as purchases,
        sum(case when event_name = 'purchase' then coalesce(purchase_revenue_in_usd, 0) else 0 end) as session_revenue,
        
        -- Device and geo (first event)
        array_agg(device_category order by event_timestamp limit 1)[offset(0)] as device_category,
        array_agg(operating_system order by event_timestamp limit 1)[offset(0)] as operating_system,
        array_agg(device_browser order by event_timestamp limit 1)[offset(0)] as device_browser,
        array_agg(country order by event_timestamp limit 1)[offset(0)] as country,
        
        -- Data quality
        min(_loaded_at) as _loaded_at
        
    from {{ ref('stg_ga4_events') }}
    group by 1, 2, 3, 4
),

session_classification as (
    select
        *,
        -- Session classification
        case 
            when session_traffic_source = '(direct)' then 'Direct'
            when session_traffic_medium = 'organic' then 'Organic Search'
            when session_traffic_medium in ('cpc', 'ppc') then 'Paid Search'  
            when session_traffic_medium = 'social' then 'Social'
            when session_traffic_medium = 'email' then 'Email'
            when session_traffic_medium = 'referral' then 'Referral'
            when session_traffic_medium = 'display' then 'Display'
            else 'Other'
        end as channel_grouping,
        
        -- Session quality indicators
        case 
            when session_duration_minutes >= 3 and page_views >= 2 then 'engaged'
            when purchases > 0 then 'converting'
            else 'bounce_risk'
        end as session_quality
        
    from session_events
)

select * from session_classification
