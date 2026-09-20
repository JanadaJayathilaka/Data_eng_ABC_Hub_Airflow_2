WITH cleaned AS (
    SELECT
        artist_id,
        NULLIF(TRIM(artist_name), '') AS artist_name,
        NULLIF(INITCAP(TRIM(country)), '') AS country,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY artist_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.artist
    WHERE artist_id IS NOT NULL
)

INSERT INTO silver.artist (
    artist_id,
    artist_name,
    country,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    artist_id,
    artist_name,
    country,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;