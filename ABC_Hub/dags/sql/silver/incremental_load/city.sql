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
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.city
    WHERE city_id IN (SELECT city_id FROM deduped)
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
