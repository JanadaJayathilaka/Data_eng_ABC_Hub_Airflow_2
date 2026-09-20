-- ============================================================
-- SILVER: COUNTRY
-- ============================================================

WITH cleaned AS (
    SELECT
        country_id,
        NULLIF(TRIM(country_name), '') AS country_name,
        UPPER(NULLIF(TRIM(country_code), '')) AS country_code,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY country_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.country
    WHERE country_id IS NOT NULL
)

INSERT INTO silver.country (
    country_id,
    country_name,
    country_code,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    country_id,
    country_name,
    country_code,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;