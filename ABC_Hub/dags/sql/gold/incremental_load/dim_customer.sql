-- ============================================================
-- GOLD INCREMENTAL: DIM_CUSTOMER (SCD Type 2)
-- ============================================================

WITH new_or_updated AS (
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
        c.updated_at
    FROM silver.customer c
    LEFT JOIN (
        SELECT customer_id, city_id,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id
                   ORDER BY updated_at DESC NULLS LAST, address_id DESC
               ) AS rn
        FROM silver.customer_address
    ) la ON c.customer_id = la.customer_id AND la.rn = 1
    LEFT JOIN silver.city ci ON la.city_id = ci.city_id
    LEFT JOIN silver.country co ON ci.country_id = co.country_id
    WHERE c.created_at > '{{ last_watermark }}'
       OR c.updated_at > '{{ last_watermark }}'
),

-- Expire previous active version if any tracked attribute changed
close_existing AS (
    UPDATE gold.dim_customer d
    SET
        effective_to = (n.updated_at::DATE - INTERVAL '1 day')::DATE,
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP
    FROM new_or_updated n
    WHERE d.customer_id = n.customer_id
      AND d.is_current = TRUE
      AND (
          d.first_name IS DISTINCT FROM n.first_name OR
          d.last_name IS DISTINCT FROM n.last_name OR
          d.email IS DISTINCT FROM n.email OR
          d.phone IS DISTINCT FROM n.phone OR
          d.status IS DISTINCT FROM n.status OR
          d.city IS DISTINCT FROM n.city OR
          d.country IS DISTINCT FROM n.country
      )
)

-- Insert brand new customers or new versions for changed customers
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
    n.customer_id,
    n.customer_no,
    n.first_name,
    n.last_name,
    n.email,
    n.phone,
    n.date_of_birth,
    n.gender,
    n.registration_date,
    n.status,
    n.city,
    n.country,
    n.updated_at::DATE AS effective_from,
    '9999-12-31'::DATE AS effective_to,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
    TRUE AS is_current
FROM new_or_updated n
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_customer d
    WHERE d.customer_id = n.customer_id
      AND d.is_current = TRUE
      AND d.first_name IS NOT DISTINCT FROM n.first_name
      AND d.last_name IS NOT DISTINCT FROM n.last_name
      AND d.email IS NOT DISTINCT FROM n.email
      AND d.phone IS NOT DISTINCT FROM n.phone
      AND d.status IS NOT DISTINCT FROM n.status
      AND d.city IS NOT DISTINCT FROM n.city
      AND d.country IS NOT DISTINCT FROM n.country
);
