-- models/staging/stg_ga4_events.sql
{{ config(
    materialized='view',
    description='Staging table for GA4 events with basic cleaning and standardization'
) }}

with raw_events as (
    select * 
    from `{{ var('ga4_project_id') }}.{{ var('ga4_dataset_id') }}.{{ var('ga4_table_pattern') }}`
    where _table_suffix between 
        format_date('%Y%m%d', date('{{ var('start_date') }}')) 
        and format_date('%Y%m%d', date('{{ var('end_date') }}'))
),

cleaned_events as (
    select
        -- Event identification
        event_date,
        event_timestamp,
        event_name,
        event_previous_timestamp,
        event_bundle_sequence_id,
        
        -- User identification  
        user_pseudo_id,
        user_id,
        
        -- Session information
        concat(user_pseudo_id, '.', 
               cast(event_timestamp / 1000000 as string)) as session_id,
        
        -- Device and geo information
        device.category as device_category,
        device.mobile_brand_name,
        device.mobile_model_name,
        device.operating_system,
        device.web_info.browser as device_browser,
        device.language as device_language,
        
        geo.continent,
        geo.country,
        geo.region,
        geo.city,
        
        -- Traffic source
        traffic_source.source as traffic_source,
        traffic_source.medium as traffic_medium, 
        traffic_source.name as traffic_campaign,
        
        -- Page information
        (select value.string_value from unnest(event_params) 
         where key = 'page_location') as page_location,
        (select value.string_value from unnest(event_params) 
         where key = 'page_title') as page_title,
        (select value.string_value from unnest(event_params) 
         where key = 'page_referrer') as page_referrer,
        
        -- E-commerce data
        ecommerce.total_item_quantity,
        ecommerce.purchase_revenue_in_usd,
        ecommerce.purchase_revenue,
        ecommerce.refund_value_in_usd,
        ecommerce.shipping_value_in_usd,
        ecommerce.tax_value_in_usd,
        ecommerce.transaction_id,
        
        -- Items array (top-level array, not inside ecommerce)
        items,
        
        -- Custom event parameters (commonly used)
        (select value.string_value from unnest(event_params) 
         where key = 'session_engaged') as session_engaged,
        (select value.int_value from unnest(event_params) 
         where key = 'engagement_time_msec') as engagement_time_msec,
        (select value.string_value from unnest(event_params) 
         where key = 'gclid') as gclid,
        
        -- Calculated fields
        datetime(timestamp_micros(event_timestamp)) as event_datetime,
        date(timestamp_micros(event_timestamp)) as event_date_parsed,
        
        -- Data quality flags
        case 
            when user_pseudo_id is null then 'missing_user_id'
            when event_name is null then 'missing_event_name'
            when event_timestamp is null then 'missing_timestamp'
            else 'valid'
        end as data_quality_flag,
        
        -- Record metadata
        current_timestamp() as _loaded_at
        
    from raw_events
    where 
        -- Basic data quality filters
        event_timestamp is not null
        and user_pseudo_id is not null
        and event_name is not null
        -- Filter out privacy-related events that shouldn't be used for attribution
        and event_name not in ('first_visit', 'session_start', 'gtag.config')
)

select * from cleaned_events
