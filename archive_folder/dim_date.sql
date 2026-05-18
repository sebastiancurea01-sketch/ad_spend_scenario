WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2012-01-01' as date)",
        end_date="date_add(current_date(), 1)"
    ) }}
),

calculated_dates AS (
    SELECT
        CAST(date_day AS DATE) AS date_key,
        date_day AS full_date,
        YEAR(date_day) AS year_id,
        MONTH(date_day) AS month_id,
        DATE_FORMAT(date_day, 'MMMM') AS month_name,
        QUARTER(date_day) AS quarter_id

    FROM date_spine
)

SELECT * FROM calculated_dates