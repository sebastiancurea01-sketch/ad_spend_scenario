WITH sessions AS (
    SELECT * FROM {{ ref('stg_website_session') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    s.session_id,
    s.session_date,
    s.user_id,
    s.utm_source,
    s.utm_campaign,
    o.order_id,
    o.price_usd,
    o.items_purchased,

    CASE 
        WHEN o.order_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS is_conversion,

        -- first order date per user, derived entirely from orders
    min(ordered_date) over (partition by o.user_id)     as first_order_date,

    -- new customer flag
    case
        when o.order_id is null then null  -- session did not convert, no customer
        when o.ordered_date = min(o.ordered_date) over (partition by o.user_id)
        then true
        else false
    end as is_new_customer
FROM sessions AS s
LEFT JOIN orders AS o
    ON s.session_id = o.session_id