WITH SOURCE AS (
    SELECT * FROM {{ source('internal_data', 'products') }}
)

SELECT
    CAST(product_id AS STRING) AS product_id,
    CAST(created_at AS TIMESTAMP) AS session_at,
    CAST(created_at AS DATE) AS session_date, -- New column for easy daily reporting
    product_name
FROM SOURCE