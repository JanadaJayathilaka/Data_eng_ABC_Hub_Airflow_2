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
FROM cleaned
WHERE rn = 1;