WITH SOURCE AS (
    SELECT * FROM {{ source('internal_data', 'website_session') }}
)

SELECT
    CAST(website_session_id AS STRING) AS session_id,
    CAST(created_at AS TIMESTAMP) AS session_at,
    CAST(created_at AS DATE) AS ordered_date, -- New column for easy daily reporting
    CAST(user_id AS STRING) AS user_id,
    CAST(utm_source AS STRING) AS utm_source,
    CAST(utm_campaign AS STRING) AS utm_campaign,
    CAST(utm_content AS STRING) AS utm_content,
    CAST(device_type AS STRING) AS device_type,
    CAST(http_referer AS STRING) AS http_referer
FROM SOURCE