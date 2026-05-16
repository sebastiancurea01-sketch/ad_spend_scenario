WITH SOURCE AS (
    SELECT * FROM {{ source('internal_data', 'products') }}
),

renamed_and_cast AS (SELECT
    CAST(product_id AS STRING) AS product_id,
    CAST(created_at AS TIMESTAMP) AS session_at,
    CAST(created_at AS DATE) AS session_date, -- New column for easy daily reporting
    product_name
FROM SOURCE
)
SELECT
    *,
    -- Metadata column
    {{ dbt.current_timestamp() }} AS _loaded_at
FROM renamed_and_cast