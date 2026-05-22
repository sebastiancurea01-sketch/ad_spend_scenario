{{
  config(
    materialized='table',
    schema='prod_analytics'
  )
}}

with performance as (
    select * from {{ ref('int_daily_performance_summarized') }}
),

ad_spend as (
    select * from {{ ref('stg_ad_spend') }}
),

joined as (
    select
        p.date_day,
        p.utm_source,
        p.utm_campaign,
        p.total_revenue,
        p.total_sessions,
        p.total_conversions,
        p.new_customers,
        p.returning_customers,
        s.total_spend,
        s.total_clicks
    from performance p
    right join ad_spend s
        on  p.date_day      = s.date_day
        and p.utm_source    = s.utm_source
        and p.utm_campaign  = s.utm_campaign
),

monthly as (
    select
        date_trunc('month', date_day)   as month,
        utm_source,
        utm_campaign,
        sum(total_revenue)              as total_revenue,
        sum(total_sessions)             as total_sessions,
        sum(total_conversions)          as total_conversions,
        sum(new_customers)              as new_customers,
        sum(returning_customers)        as returning_customers,
        sum(total_spend)                as total_spend,
        sum(total_clicks)               as total_clicks
    from joined
    group by 1, 2, 3
),

final as (
    select
        month,
        utm_source,
        utm_campaign,
        total_revenue,
        total_sessions,
        total_conversions,
        new_customers,
        returning_customers,
        total_spend,
        total_clicks,

        -- KPIs
        round(total_revenue
            / nullif(total_spend, 0), 2)                        as roas,
        round(total_spend
            / nullif(total_conversions, 0), 2)                  as cpa,
        round(total_conversions
            / nullif(total_sessions, 0), 4)                     as conversion_rate,
        round(total_spend
            / nullif(new_customers, 0), 2)                      as cac_usd,
        round(total_revenue
            / nullif(total_conversions, 0), 2)                  as avg_ltv_usd,

        -- churn rate: % of last month's customers who did not return
        round(
            1 - (
                returning_customers
                / nullif(
                    lag(total_conversions, 1) over (
                        partition by utm_source, utm_campaign
                        order by month
                    ), 0)
            ), 4)                                               as churn_rate,

        -- yoy growth: revenue vs same month prior year
        round(
            (total_revenue - lag(total_revenue, 12) over (
                partition by utm_source, utm_campaign
                order by month
            ))
            / nullif(lag(total_revenue, 12) over (
                partition by utm_source, utm_campaign
                order by month
            ), 0), 4)                                          as yoy_revenue_growth

    from monthly
)

select * from final