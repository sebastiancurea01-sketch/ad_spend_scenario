with orders as (
    select * from {{ ref('stg_orders') }}
),

stg_order_items as (
    select
        order_id,
        sum(price_usd)          as gross_revenue_usd,
        sum(cogs_usd)           as cogs_usd
    from {{ ref('stg_order_items') }}
    group by order_id
),

sessions as (
    -- this is your already-joined model
    select * from {{ ref('ad_spend_JOIN_website_session') }}
),

final as (
    select
        o.order_id,
        o.ordered_date,
        o.session_id,

        -- channel attribution comes from your already-joined session model
        s.utm_source,
        s.utm_campaign,

        -- order metrics
        oi.gross_revenue_usd,
        oi.cogs_usd,
        oi.gross_revenue_usd - oi.cogs_usd                          as margin_usd

    from stg_orders o
    left join stg_order_items    oi on o.order_id   = oi.order_id
    left join stg_website_session       s on o.session_id = s.session_id
)

select * from final