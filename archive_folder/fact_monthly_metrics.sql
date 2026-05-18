with orders as (
    select * from {{ ref('fact_orders') }}
),

sessions as (
    select
        date_trunc('month', log_date)   as month,
        utm_source,
        utm_campaign,
        count(distinct performance_key)          as total_sessions,
        sum(daily_spend)                      as spend_usd
    from {{ ref('ad_spend_JOIN_website_session') }}
    group by 1, 2, 3
),

calcualated_revenue as (
    select
        sum(margin_usd) as net_revenue_usd
    from orders

),

order_metrics as (
    select
        date_trunc('month', ordered_date)     as month,
        utm_source,
        utm_campaign,
        count(distinct order_id)            as orders,
        sum(margin_usd)     as net_revenue_usd,
        sum(case when utm_campaign = 'brand'
            then ( 
            SELECT 
                net_revenue_usd 
            FROM 
                calcualated_revenue )
            end)       as brand_revenue_usd
    from orders
    group by 1, 2, 3
),

final as (
    select
        o.month,
        o.utm_source,
        o.utm_campaign,
        o.orders,
        o.net_revenue_usd,
        o.brand_revenue_usd,
        s.total_sessions,
        s.spend_usd,

        -- your 7 dashboard KPIs
        round(o.net_revenue_usd
            / nullif(s.spend_usd, 0), 2)                            as roas,
        round(s.spend_usd
            / nullif(s.total_sessions, 0), 4)                       as conversion_rate,
        round(o.brand_revenue_usd
            / nullif(o.net_revenue_usd, 0), 4)                      as brand_revenue_pct,

        -- yoy growth: current month revenue vs same month prior year
        round((o.net_revenue_usd - lag(o.net_revenue_usd, 12)
            over (partition by o.utm_source, o.utm_campaign
                  order by o.month))
            / nullif(lag(o.net_revenue_usd, 12)
            over (partition by o.utm_source, o.utm_campaign
                  order by o.month), 0), 4)                         as yoy_revenue_growth

        -- churn and retention computed in scenario model on top of this

    from order_metrics o
    left join sessions s
        on  o.month        = s.month
        and o.utm_source   = s.utm_source
        and o.utm_campaign = s.utm_campaign
)

select * from final