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
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.artist
    WHERE artist_id IN (SELECT artist_id FROM deduped)
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
FROM deduped;
