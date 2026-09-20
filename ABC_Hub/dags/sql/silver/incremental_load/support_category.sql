WITH cleaned AS (
    SELECT
        category_id,
        NULLIF(INITCAP(TRIM(category_name)), '') AS category_name,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY category_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.support_category
    WHERE category_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.support_category
    WHERE category_id IN (SELECT category_id FROM deduped)
)
INSERT INTO silver.support_category (
    category_id,
    category_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    category_id,
    category_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
