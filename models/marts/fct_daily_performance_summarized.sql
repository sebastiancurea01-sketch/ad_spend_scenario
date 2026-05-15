WITH joined_data AS (
    SELECT * FROM {{ ref('int_sessions_joined_orders') }}
)

SELECT
    CAST(session_at AS DATE) AS date_day,
    utm_source,
    utm_campaign,
    SUM(price_usd) AS total_revenue,
    SUM(is_conversion) AS total_conversions,
    COUNT(session_id) AS total_sessions,
    COUNT(DISTINCT user_id) AS unique_customer


FROM joined_data
GROUP BY 
    1, 2, 3
