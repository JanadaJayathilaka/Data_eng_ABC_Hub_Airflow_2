WITH cleaned AS (
    SELECT
        recommendation_id,
        customer_id,
        content_id,
        recommendation_date,
        NULLIF(TRIM(algorithm_version), '') AS algorithm_version,
        clicked,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY recommendation_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.recommendation
    WHERE recommendation_id IS NOT NULL
)

INSERT INTO silver.recommendation (
    recommendation_id,
    customer_id,
    content_id,
    recommendation_date,
    algorithm_version,
    clicked,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    recommendation_id,
    customer_id,
    content_id,
    recommendation_date,
    algorithm_version,
    clicked,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;