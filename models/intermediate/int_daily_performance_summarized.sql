WITH joined_data AS (
    SELECT * FROM {{ ref('int_sessions_joined_orders') }}
)

SELECT
    CAST(session_date AS DATE) AS date_day,
    utm_source,
    utm_campaign,
    SUM(price_usd) AS total_revenue,
    SUM(is_conversion) AS total_conversions,
    COUNT(session_id) AS total_sessions,
        -- new vs returning — COUNT the flag, don't carry it raw
    count(distinct case
        when is_new_customer = true
        then user_id
    end)                                       as new_customers,

    count(distinct case
        when is_new_customer = false
        then user_id
    end)                                       as returning_customers

FROM joined_data
GROUP BY 
    1, 2, 3
