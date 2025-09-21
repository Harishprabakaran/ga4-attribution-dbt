-- models/intermediate/int_attribution_base.sql
{{ config(
    materialized='ephemeral',
    description='Base attribution data with first/last touch identification using window functions and deterministic tie-breaking'
) }}

with journey_touchpoints as (
  select * from {{ ref('int_user_journeys') }}
  -- If your int_user_journeys can contain conversions with NULL timestamps, uncomment the line below
  -- where conversion_timestamp is not null
),

/* -------------------------
   FIRST TOUCH: pick the earliest touch per (user, conversion)
   tie-breaker: prioritize channel_grouping order (Paid Search, Display, Social, Email, Organic, Referral, Direct, Other)
   ------------------------- */
first_touch_ranked as (
  select
    user_pseudo_id,
    conversion_timestamp,
    conversion_event,
    conversion_revenue,
    conversion_type,
    transaction_id,
    touchpoint_timestamp,
    session_id,
    channel_grouping,
    session_traffic_source,
    session_traffic_medium,
    session_traffic_campaign,
    landing_page,
    total_touchpoints,
    journey_complexity,
    consideration_period,

    row_number() over (
      partition by user_pseudo_id, conversion_timestamp
      order by
        touchpoint_timestamp asc,
        -- tie-break ordering (lower = higher priority)
        case channel_grouping
          when 'Paid Search' then 1
          when 'Display' then 2
          when 'Social' then 3
          when 'Email' then 4
          when 'Organic Search' then 5
          when 'Referral' then 6
          when 'Direct' then 7
          else 8
        end asc
    ) as rn_first
  from journey_touchpoints
),

first_touch_attribution as (
  select
    user_pseudo_id,
    conversion_timestamp,
    conversion_event,
    conversion_revenue,
    conversion_type,
    transaction_id,
    touchpoint_timestamp     as first_touch_timestamp,
    session_id               as first_touch_session_id,
    channel_grouping         as first_touch_channel,
    session_traffic_source   as first_touch_source,
    session_traffic_medium   as first_touch_medium,
    session_traffic_campaign as first_touch_campaign,
    landing_page             as first_touch_landing_page,
    total_touchpoints,
    journey_complexity,
    consideration_period
  from first_touch_ranked
  where rn_first = 1
),

/* -------------------------
   LAST TOUCH: pick the latest touch per (user, conversion)
   tie-breaker: if timestamps equal use same priority ordering (asc)
   ------------------------- */
last_touch_ranked as (
  select
    user_pseudo_id,
    conversion_timestamp,
    touchpoint_timestamp,
    session_id,
    channel_grouping,
    session_traffic_source,
    session_traffic_medium,
    session_traffic_campaign,
    landing_page,

    row_number() over (
      partition by user_pseudo_id, conversion_timestamp
      order by
        touchpoint_timestamp desc,
        -- same tie-break but keep asc (so among identical timestamps, Paid Search wins)
        case channel_grouping
          when 'Paid Search' then 1
          when 'Display' then 2
          when 'Social' then 3
          when 'Email' then 4
          when 'Organic Search' then 5
          when 'Referral' then 6
          when 'Direct' then 7
          else 8
        end asc
    ) as rn_last
  from journey_touchpoints
),

last_touch_attribution as (
  select
    user_pseudo_id,
    conversion_timestamp,
    touchpoint_timestamp           as last_touch_timestamp,
    session_id                     as last_touch_session_id,
    channel_grouping               as last_touch_channel,
    session_traffic_source         as last_touch_source,
    session_traffic_medium         as last_touch_medium,
    session_traffic_campaign       as last_touch_campaign,
    landing_page                   as last_touch_landing_page
  from last_touch_ranked
  where rn_last = 1
),

/* -------------------------
   Combine first + last
   ------------------------- */
attribution_combined as (
  select
    f.user_pseudo_id,
    f.conversion_timestamp,
    f.conversion_event,
    f.conversion_revenue,
    f.conversion_type,
    f.transaction_id,
    f.total_touchpoints,
    f.journey_complexity,
    f.consideration_period,

    -- first touch fields
    f.first_touch_timestamp,
    f.first_touch_session_id,
    f.first_touch_channel,
    f.first_touch_source,
    f.first_touch_medium,
    f.first_touch_campaign,
    f.first_touch_landing_page,

    -- last touch fields
    l.last_touch_timestamp,
    l.last_touch_session_id,
    l.last_touch_channel,
    l.last_touch_source,
    l.last_touch_medium,
    l.last_touch_campaign,
    l.last_touch_landing_page,

    -- comparison
    case
      when f.first_touch_channel is null or l.last_touch_channel is null then 'unknown'
      when f.first_touch_channel = l.last_touch_channel then 'same_channel'
      else 'cross_channel'
    end as attribution_comparison,

    current_timestamp() as _processed_at

  from first_touch_attribution f
  left join last_touch_attribution l
    on f.user_pseudo_id = l.user_pseudo_id
   and f.conversion_timestamp = l.conversion_timestamp
)

select * from attribution_combined
