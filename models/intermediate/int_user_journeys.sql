-- models/intermediate/int_user_journeys.sql
{{ config(
    materialized='ephemeral',
    description='Reconstructed user journeys with touchpoints leading to conversions'
) }}

with user_conversions as (
    -- Get all conversion events with their timestamps
    select
        user_pseudo_id,
        user_id,
        event_date as conversion_date,
        event_timestamp as conversion_timestamp,
        event_datetime as conversion_datetime,
        event_name as conversion_event,
        conversion_revenue,
        conversion_type,
        transaction_id,
        row_number() over (
            partition by user_pseudo_id 
            order by event_timestamp
        ) as conversion_sequence
    from {{ ref('stg_ga4_conversions') }}
),

user_touchpoints as (
    -- Get all sessions that could be touchpoints
    select
        s.user_pseudo_id,
        s.user_id,
        s.session_id,
        s.event_date,
        s.session_start_timestamp,
        s.session_start_datetime,
        s.session_traffic_source,
        s.session_traffic_medium,
        s.session_traffic_campaign,
        s.channel_grouping,
        s.landing_page,
        s.page_views,
        s.total_engagement_seconds,
        s.session_revenue,
        s.session_quality
    from {{ ref('stg_ga4_sessions') }} s
    where s.session_start_timestamp is not null
),

journey_mapping as (
    select
        c.user_pseudo_id,
        c.conversion_sequence,
        c.conversion_date,
        c.conversion_timestamp,
        c.conversion_datetime,
        c.conversion_event,
        c.conversion_revenue,
        c.conversion_type,
        c.transaction_id,
        
        -- Touchpoint information  
        t.session_id,
        t.event_date as touchpoint_date,
        t.session_start_timestamp as touchpoint_timestamp,
        t.session_start_datetime as touchpoint_datetime,
        t.session_traffic_source,
        t.session_traffic_medium, 
        t.session_traffic_campaign,
        t.channel_grouping,
        t.landing_page,
        t.page_views,
        t.total_engagement_seconds,
        t.session_quality,
        
        -- Journey context
        row_number() over (
            partition by c.user_pseudo_id, c.conversion_timestamp
            order by t.session_start_timestamp
        ) as touchpoint_sequence,
        
        count(*) over (
            partition by c.user_pseudo_id, c.conversion_timestamp
        ) as total_touchpoints,
        
        -- Time from touchpoint to conversion
        timestamp_diff(
            timestamp_micros(c.conversion_timestamp),
            timestamp_micros(t.session_start_timestamp),
            hour
        ) as hours_to_conversion,
        
        timestamp_diff(
            timestamp_micros(c.conversion_timestamp),
            timestamp_micros(t.session_start_timestamp), 
            day
        ) as days_to_conversion
        
    from user_conversions c
    inner join user_touchpoints t
        on c.user_pseudo_id = t.user_pseudo_id
        -- Only include touchpoints within lookback window
        and t.session_start_timestamp <= c.conversion_timestamp
        and timestamp_diff(
            timestamp_micros(c.conversion_timestamp),
            timestamp_micros(t.session_start_timestamp),
            day
        ) <= {{ var('attribution_lookback_days') }}
),

journey_enrichment as (
    select
        *,
        -- Position-based flags
        case when touchpoint_sequence = 1 then true else false end as is_first_touch,
        case when touchpoint_sequence = total_touchpoints then true else false end as is_last_touch,
        
        -- Channel transition analysis
        lag(channel_grouping) over (
            partition by user_pseudo_id, conversion_timestamp 
            order by touchpoint_sequence
        ) as previous_channel,
        
        lead(channel_grouping) over (
            partition by user_pseudo_id, conversion_timestamp
            order by touchpoint_sequence  
        ) as next_channel,
        
        -- Journey quality indicators
        case 
            when total_touchpoints = 1 then 'single_touch'
            when total_touchpoints between 2 and 4 then 'simple_journey'
            when total_touchpoints between 5 and 10 then 'complex_journey'
            else 'very_complex_journey'
        end as journey_complexity,
        
        case 
            when days_to_conversion = 0 then 'same_day'
            when days_to_conversion <= 1 then 'next_day'  
            when days_to_conversion <= 7 then 'within_week'
            when days_to_conversion <= 30 then 'within_month'
            else 'long_consideration'
        end as consideration_period
        
    from journey_mapping
)

select * from journey_enrichment
