WITH cleaned AS (
    SELECT
        content_type_id,
        NULLIF(TRIM(content_type), '') AS content_type,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY content_type_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.content_type
    WHERE content_type_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.content_type
    WHERE content_type_id IN (SELECT content_type_id FROM deduped)
)
INSERT INTO silver.content_type (
    content_type_id,
    content_type,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    content_type_id,
    content_type,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
