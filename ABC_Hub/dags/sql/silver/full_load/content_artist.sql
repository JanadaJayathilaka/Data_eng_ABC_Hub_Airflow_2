WITH cleaned AS (
    SELECT
        content_id,
        artist_id,
        NULLIF(INITCAP(TRIM(role)), '') AS role,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY content_id, artist_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.content_artist
    WHERE content_id IS NOT NULL
      AND artist_id IS NOT NULL
)

INSERT INTO silver.content_artist (
    content_id,
    artist_id,
    role,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    content_id,
    artist_id,
    role,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;