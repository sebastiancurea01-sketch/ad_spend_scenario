WITH spend AS (
    SELECT * FROM {{ ref('stg_ad_spend') }}
),

performance AS (
    SELECT * FROM {{ ref('fct_daily_performance_summarized') }}
)

SELECT
    s.date_day,
    s.utm_source,
    s.utm_campaign,
    s.total_spend,
    s.total_clicks,
    COALESCE(p.total_revenue, 0) AS total_revenue,
    COALESCE(p.total_conversions, 0) AS total_conversions,
    COALESCE(p.total_sessions, 0) AS total_sessions,

    -- Calculation: Return on Ad Spend (Revenue / Spend)
    CASE 
        WHEN s.total_spend > 0 THEN ROUND(p.total_revenue / s.total_spend, 2)
        ELSE NULL 
    END AS ROAS,

    -- Calculation: Cost Per Acquisition (Spend / Conversions)
    CASE 
        WHEN p.total_conversions > 0 THEN ROUND(s.total_spend / p.total_conversions, 2) 
        ELSE NULL 
    END AS CPA,

    -- Calculation: revenue coming from brand channel
    CASE
        WHEN s.utm_campaign = 'brand'
        THEN COALESCE(p.total_revenue, 0)
        ELSE 0
    END AS brand_revenue_usd

FROM spend AS s
LEFT JOIN performance AS p
    ON s.date_day = p.date_day
    AND s.utm_source = p.utm_source
    AND s.utm_campaign = p.utm_campaign

ORDER BY date_day DESC