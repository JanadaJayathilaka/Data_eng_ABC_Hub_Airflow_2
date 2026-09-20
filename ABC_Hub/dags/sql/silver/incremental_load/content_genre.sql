WITH cleaned AS (
    SELECT
        content_id,
        genre_id,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY content_id, genre_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.content_genre
    WHERE content_id IS NOT NULL
      AND genre_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.content_genre s
    USING deduped d
    WHERE s.content_id = d.content_id
      AND s.genre_id = d.genre_id
)
INSERT INTO silver.content_genre (
    content_id,
    genre_id,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    content_id,
    genre_id,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
