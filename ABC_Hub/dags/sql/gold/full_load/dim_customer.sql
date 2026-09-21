TRUNCATE TABLE gold.dim_customer RESTART IDENTITY CASCADE;
WITH latest_address AS (
    -- Pick each customer's most recent address to get their city_id
    SELECT
        customer_id,
        city_id,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC NULLS LAST, address_id DESC
        ) AS rn
    FROM silver.customer_address
)
INSERT INTO gold.dim_customer (
    customer_id,
    customer_no,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    registration_date,
    status,
    city,
    country,
    effective_from,
    effective_to,
    created_at,
    updated_at,
    is_current
)
SELECT
    c.customer_id,
    c.customer_no,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.date_of_birth,
    c.gender,
    c.registration_date,
    c.status,
    ci.city_name AS city,
    co.country_name AS country,
    COALESCE(c.registration_date, c.created_at::DATE, '2020-01-01'::DATE) AS effective_from,
    '9999-12-31'::DATE AS effective_to,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
    TRUE AS is_current
FROM silver.customer c
LEFT JOIN latest_address la ON c.customer_id = la.customer_id AND la.rn = 1
LEFT JOIN silver.city ci ON la.city_id = ci.city_id
LEFT JOIN silver.country co ON ci.country_id = co.country_id;