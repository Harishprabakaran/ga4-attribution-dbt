-- models/marts/mart_attribution_first_click.sql
{{ config(
    materialized='table',
    partition_by={
        "field": "event_date", 
        "data_type": "date"
    },
    cluster_by=["attributed_channel", "user_pseudo_id"],
    description='First-click attribution model with revenue and conversion attribution'
) }} 

with attribution_base as (
    select * from {{ ref('int_attribution_base') }}
),

first_click_attribution as (
    select
        -- Conversion identifiers
        user_pseudo_id,
        conversion_timestamp,
        date(timestamp_micros(conversion_timestamp)) as event_date,
        conversion_event,
        conversion_type,
        transaction_id,
        
        -- Attribution details
        'first_click' as attribution_model,
        first_touch_timestamp as attributed_timestamp,
        date(timestamp_micros(first_touch_timestamp)) as attributed_date,
        first_touch_session_id as attributed_session_id,
        
        -- Channel attribution
        first_touch_channel as attributed_channel,
        first_touch_source as attributed_source,
        first_touch_medium as attributed_medium,
        first_touch_campaign as attributed_campaign,
        first_touch_landing_page as attributed_landing_page,
        
        -- Revenue attribution (100% to first touch)
        conversion_revenue as attributed_revenue,
        1.0 as attribution_weight,
        
        -- Journey context
        total_touchpoints,
        journey_complexity,
        consideration_period,
        attribution_comparison,
        
        -- Time to conversion
        timestamp_diff(
            timestamp_micros(conversion_timestamp),
            timestamp_micros(first_touch_timestamp),
            day
        ) as days_first_touch_to_conversion,
        
        timestamp_diff(
            timestamp_micros(conversion_timestamp), 
            timestamp_micros(first_touch_timestamp),
            hour
        ) as hours_first_touch_to_conversion,
        
        _processed_at
        
    from attribution_base
)

select * from first_click_attribution

