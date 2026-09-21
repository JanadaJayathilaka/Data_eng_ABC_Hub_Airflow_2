TRUNCATE TABLE gold.dim_month CASCADE;
INSERT INTO gold.dim_month (
    month_key,
    month_number,
    month_name,
    quarter_number,
    year_number
)
SELECT
    TO_CHAR(datum, 'YYYYMM')::INTEGER AS month_key,
    EXTRACT(MONTH FROM datum)::INTEGER AS month_number,
    TRIM(TO_CHAR(datum, 'Month')) AS month_name,
    EXTRACT(QUARTER FROM datum)::INTEGER AS quarter_number,
    EXTRACT(YEAR FROM datum)::INTEGER AS year_number
FROM GENERATE_SERIES(
    '2020-01-01'::DATE,
    '2035-12-01'::DATE,
    '1 month'::INTERVAL
) AS datum;
