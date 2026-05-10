WITH SOURCE AS (
    SELECT * FROM {{ source('internal_data', 'refunds') }}
)

SELECT
    CAST(order_item_refund_id AS STRING) AS order_item_refund_id,
    CAST(created_at AS TIMESTAMP) AS session_at,
    CAST(created_at AS DATE) AS session_date, -- New column for easy daily reporting
    CAST(order_item_id AS STRING) AS order_item_id,
    CAST(order_id AS STRING) AS order_id,
    CAST(refund_amount_usd AS DECIMAL(10,2)) AS refund_amount_usd

FROM SOURCE