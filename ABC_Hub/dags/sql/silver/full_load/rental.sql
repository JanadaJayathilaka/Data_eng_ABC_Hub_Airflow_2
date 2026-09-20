WITH cleaned AS (
    SELECT
        rental_id,
        customer_id,
        inventory_id,
        rental_date,
        due_date,
        return_date,

        CASE
            WHEN rental_fee >= 0 THEN rental_fee
            ELSE NULL
        END AS rental_fee,

        CASE
            WHEN late_fee >= 0 THEN late_fee
            ELSE NULL
        END AS late_fee,

        NULLIF(UPPER(TRIM(status)), '') AS status,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY rental_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.rental
    WHERE rental_id IS NOT NULL
)

INSERT INTO silver.rental (
    rental_id,
    customer_id,
    inventory_id,
    rental_date,
    due_date,
    return_date,
    rental_fee,
    late_fee,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    rental_id,
    customer_id,
    inventory_id,
    rental_date,
    due_date,
    return_date,
    rental_fee,
    late_fee,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;