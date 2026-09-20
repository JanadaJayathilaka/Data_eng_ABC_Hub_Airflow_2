-- ============================================================
-- SILVER: CITY
-- ============================================================

WITH cleaned AS (
    SELECT
        city_id,
        country_id,
        NULLIF(TRIM(city_name), '') AS city_name,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY city_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.city
    WHERE city_id IS NOT NULL
)

INSERT INTO silver.city (
    city_id,
    country_id,
    city_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    city_id,
    country_id,
    city_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;