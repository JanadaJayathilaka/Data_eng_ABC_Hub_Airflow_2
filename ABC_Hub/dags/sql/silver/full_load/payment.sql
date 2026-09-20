WITH cleaned AS (
    SELECT
        payment_id,
        customer_id,
        rental_id,
        subscription_id,
        payment_method_id,

        CASE
            WHEN amount >= 0 THEN amount
            ELSE NULL
        END AS amount,

        payment_date,
        NULLIF(UPPER(TRIM(payment_type)), '') AS payment_type,
        NULLIF(UPPER(TRIM(status)), '') AS status,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.payment
    WHERE payment_id IS NOT NULL
)

INSERT INTO silver.payment (
    payment_id,
    customer_id,
    rental_id,
    subscription_id,
    payment_method_id,
    amount,
    payment_date,
    payment_type,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    payment_id,
    customer_id,
    rental_id,
    subscription_id,
    payment_method_id,
    amount,
    payment_date,
    payment_type,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;