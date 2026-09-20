WITH cleaned AS (
    SELECT
        address_id,
        customer_id,
        city_id,
        NULLIF(TRIM(address_line), '') AS address_line,
        NULLIF(TRIM(postal_code), '') AS postal_code,
        NULLIF(INITCAP(TRIM(address_type)), '') AS address_type,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY address_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.customer_address
    WHERE address_id IS NOT NULL
)

INSERT INTO silver.customer_address (
    address_id,
    customer_id,
    city_id,
    address_line,
    postal_code,
    address_type,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    address_id,
    customer_id,
    city_id,
    address_line,
    postal_code,
    address_type,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;