WITH cleaned AS (
    SELECT
        payment_method_id,
        NULLIF(INITCAP(TRIM(method_name)), '') AS method_name,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY payment_method_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.payment_method
    WHERE payment_method_id IS NOT NULL
)

INSERT INTO silver.payment_method (
    payment_method_id,
    method_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    payment_method_id,
    method_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;