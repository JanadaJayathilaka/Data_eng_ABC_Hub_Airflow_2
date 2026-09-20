WITH cleaned AS (
    SELECT
        genre_id,
        NULLIF(INITCAP(TRIM(genre_name)), '') AS genre_name,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY genre_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.genre
    WHERE genre_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.genre
    WHERE genre_id IN (SELECT genre_id FROM deduped)
)
INSERT INTO silver.genre (
    genre_id,
    genre_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    genre_id,
    genre_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
