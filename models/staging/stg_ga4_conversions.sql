-- models/staging/stg_ga4_conversions.sql  
{{ config(
    materialized='view',
    description='Staging view for GA4 conversion events with revenue attribution'
) }}

with conversion_events as (
    select
        event_date,
        event_timestamp,
        event_datetime,
        user_pseudo_id,
        user_id,
        session_id,
        event_name,
        -- Traffic source at conversion
        traffic_source,
        traffic_medium,
        traffic_campaign,
        -- Revenue information
        case 
            when event_name = 'purchase' then coalesce(purchase_revenue_in_usd, 0)
            when event_name = 'add_to_cart' then 0  -- Micro-conversion, no direct revenue
            else 0
        end as conversion_revenue,
        -- Transaction details
        transaction_id,
        total_item_quantity,
        -- Conversion type classification
        case 
            when event_name = 'purchase' then 'macro_conversion'
            when event_name in ('add_to_cart', 'begin_checkout') then 'micro_conversion'
            else 'other_conversion'
        end as conversion_type,
        -- Items purchased (for product-level attribution)
        items,
        _loaded_at
        
    from {{ ref('stg_ga4_events') }}
    where event_name in (
        {% for event in var('conversion_events') %}
        '{{ event }}'{% if not loop.last %},{% endif %}
        {% endfor %}
    )
)

select * from conversion_events