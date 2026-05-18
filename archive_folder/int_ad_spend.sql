/*
    Model: int_marketing__ad_spend_hashed
    Grain: date, utm_source, utm_campaign
    Description: Standardizing marketing spend and creating a persistent surrogate key 
                 to enable robust ROAS joins in the Gold layer.
*/

WITH filtered_source AS (
    SELECT * FROM {{ ref('stg_ad_spend') }}
    -- We filter here to ensure we don't hash rows with entirely missing keys
    WHERE date_day IS NOT NULL 
      AND utm_source IS NOT NULL
),

surrogate_key_generation AS (
    SELECT
        -- The Senior Move: Define the unique grain of the table
        {{ dbt_utils.generate_surrogate_key([
            'date_day', 
            'utm_source', 
            'utm_campaign'
        ]) }} AS ad_spend_id,
        
        -- Bring in the rest of the cleaned data
        date_day,
        utm_source,
        utm_campaign,
        CAST(total_spend AS DECIMAL(18, 2)) AS spend_amount,
        
        -- Metadata for auditability (as we discussed before)
        {{ dbt.current_timestamp() }} AS updated_at
    FROM filtered_source
)

SELECT * FROM surrogate_key_generation

ORDER BY date_day