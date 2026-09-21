TRUNCATE TABLE gold.dim_date;

INSERT INTO gold.dim_date (
    date_key,
    full_date,
    day_number,
    day_name,
    week_number,
    month_number,
    month_name,
    quarter_number,
    year_number
)
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INTEGER AS date_key,
    datum AS full_date,
    EXTRACT(DAY FROM datum)::INTEGER AS day_number,
    TRIM(TO_CHAR(datum, 'Day')) AS day_name,
    EXTRACT(WEEK FROM datum)::INTEGER AS week_number,
    EXTRACT(MONTH FROM datum)::INTEGER AS month_number,
    TRIM(TO_CHAR(datum, 'Month')) AS month_name,
    EXTRACT(QUARTER FROM datum)::INTEGER AS quarter_number,
    EXTRACT(YEAR FROM datum)::INTEGER AS year_number
FROM GENERATE_SERIES(
    '2020-01-01'::DATE,
    '2035-12-31'::DATE,
    '1 day'::INTERVAL
) AS datum;