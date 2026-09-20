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
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.recommendation
    WHERE recommendation_id IN (SELECT recommendation_id FROM deduped)
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
FROM deduped;
