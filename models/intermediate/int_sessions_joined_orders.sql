WITH sessions AS (
    SELECT * FROM {{ ref('stg_website_session') }}
),

orders AS (
    SELECT * FROM {{ ref('fct_order') }}
)

SELECT
    s.session_id,
    s.session_at,
    s.user_id,
    s.utm_source,
    s.utm_campaign,
    o.order_id,
    o.price_usd,
    o.items_purchased,
    o.is_new_customer,
    CASE 
        WHEN o.order_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS is_conversion
    
FROM sessions AS s
LEFT JOIN orders AS o
    ON s.session_id = o.session_id