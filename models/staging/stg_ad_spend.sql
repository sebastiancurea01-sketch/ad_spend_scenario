WITH SOURCE AS (
    SELECT * FROM {{ source('external_marketing', 'ad_spend') }}
),

renamed_and_cast AS (SELECT
    CAST(reporting_date AS DATE) AS date_day,
    LOWER(utm_source) AS utm_source,
    LOWER(utm_campaign) AS utm_campaign,
    CAST(total_spend AS DECIMAL(10,2)) AS total_spend,
    CAST(total_clicks AS INT) AS total_clicks
FROM SOURCE
)
SELECT
    *,
    -- Metadata column
    {{ dbt.current_timestamp() }} AS _loaded_at
FROM renamed_and_cast

ORDER BY date_day DESC