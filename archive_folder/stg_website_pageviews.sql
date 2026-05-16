WITH SOURCE AS (
    SELECT * FROM {{ source('internal_data', 'website_pageviews') }}
),

renamed_and_cast AS (SELECT
    CAST(website_pageview_id AS STRING) AS website_pageview_id,
    CAST(created_at AS TIMESTAMP) AS session_at,
    CAST(created_at AS DATE) AS session_date, -- New column for easy daily reporting
    CAST(website_session_id AS STRING) AS website_session_id,
    pageview_url
FROM SOURCE
)
SELECT
    *,
    -- Metadata column
    {{ dbt.current_timestamp() }} AS _loaded_at
FROM renamed_and_cast