/*
    Model: int_marketing__performance_joined
    Grain: Date, Source, Campaign
    Description: Aggregates sessions to the daily grain and joins with ad spend 
                 to calculate Cost Per Session (CPS).
*/

WITH daily_sessions AS (
    -- Step 1: Collapse 1.7M sessions into daily totals per source/campaign
    SELECT
        CAST(session_at AS DATE) AS session_date,
        utm_source,
        utm_campaign,
        -- This must match the components of your ad_spend_id
        {{ dbt_utils.generate_surrogate_key(['CAST(session_at AS DATE)', 'utm_source', 'utm_campaign']) }} AS ad_join_key,
        COUNT(session_id) AS total_sessions
    FROM {{ ref('stg_website_session') }}
    GROUP BY 1, 2, 3, 4
),

daily_spend AS (
    -- Step 2: Reference your existing hashed spend model
    SELECT
        ad_spend_id, -- This is your surrogate key
        date_day AS spend_date,
        utm_source,
        utm_campaign,
        spend_amount
    FROM {{ ref('int_ad_spend') }}
),

final_joined AS (
    -- Step 3: Join on the keys
    SELECT
        COALESCE(s.ad_join_key, d.ad_spend_id) AS performance_key,
        COALESCE(s.session_date, d.spend_date) AS log_date,
        COALESCE(s.utm_source, d.utm_source) AS utm_source,
        COALESCE(s.utm_campaign, d.utm_campaign) AS utm_campaign,
        
        -- Metrics
        COALESCE(d.spend_amount, 0) AS daily_spend,
        COALESCE(s.total_sessions, 0) AS session_count
        
        -- Business Logic: Handle division by zero


    FROM daily_sessions s
    FULL OUTER JOIN daily_spend d 
        ON s.ad_join_key = d.ad_spend_id
)

SELECT * FROM final_joined

ORDER BY log_date DESC