WITH customer_first_orders AS (
    -- Pre-calculate the first date for every customer
    SELECT 
        user_id, 
        MIN(ordered_date) AS first_order_date
    FROM {{ ref('stg_orders') }}
    GROUP BY 1
),

final AS (
    SELECT 
        o.*,
        -- Comparison logic: if current order date matches the first one, they are new
        (o.ordered_date = cfo.first_order_date) AS is_new_customer
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN customer_first_orders cfo 
        ON o.user_id = cfo.user_id
)

SELECT * FROM final