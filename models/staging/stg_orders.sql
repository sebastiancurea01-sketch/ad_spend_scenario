WITH SOURCE AS (
    SELECT * FROM {{ source('internal_data', 'order') }}
)

SELECT
    CAST(order_id AS STRING) AS order_id,
    CAST(created_at AS TIMESTAMP) AS ordered_at,
    CAST(created_at AS DATE) AS ordered_date, -- New column for easy daily reporting
    CAST(website_session_id AS STRING) AS session_id,
    CAST(user_id AS STRING) AS user_id,
    CAST(primary_product_id AS STRING) AS primary_product_id,
    CAST(items_purchased AS INT) AS items_purchased,
    CAST(price_usd AS DECIMAL(10,2)) AS price_usd,
    CAST(cogs_usd AS DECIMAL(10,2)) AS cogs_usd
FROM SOURCE