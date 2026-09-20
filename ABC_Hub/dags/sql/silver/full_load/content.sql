WITH cleaned AS (
    SELECT
        content_id,
        content_type_id,
        NULLIF(TRIM(title), '') AS title,
        release_date,

        CASE
            WHEN duration_minutes >= 0 THEN duration_minutes
            ELSE NULL
        END AS duration_minutes,

        NULLIF(TRIM(language), '') AS language,
        NULLIF(UPPER(TRIM(age_rating)), '') AS age_rating,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY content_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.content
    WHERE content_id IS NOT NULL
)

INSERT INTO silver.content (
    content_id,
    content_type_id,
    title,
    release_date,
    duration_minutes,
    language,
    age_rating,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    content_id,
    content_type_id,
    title,
    release_date,
    duration_minutes,
    language,
    age_rating,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;