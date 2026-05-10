WITH SOURCE AS (
    SELECT * FROM {{ source('internal_data', 'order_items') }}
),

renamed_and_cast AS (SELECT
    CAST(order_item_id AS STRING) AS order_item_id,
    CAST(created_at AS TIMESTAMP) AS ordered_at,
    CAST(created_at AS DATE) AS ordered_date, -- New column for easy daily reporting
    CAST(order_id AS STRING) AS order_id,
    CAST(product_id AS STRING) AS product_id,
    CAST(is_primary_item AS STRING) AS is_primary_item,
    CAST(price_usd AS DECIMAL(10,2)) AS price_usd,
    CAST(cogs_usd AS DECIMAL(10,2)) AS cogs_usd
FROM SOURCE
)
SELECT
    *,
    -- Metadata column
    {{ dbt.current_timestamp() }} AS _loaded_at
FROM renamed_and_cast