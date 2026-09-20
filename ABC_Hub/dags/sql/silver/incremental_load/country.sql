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
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.country
    WHERE country_id IN (SELECT country_id FROM deduped)
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
FROM deduped;
