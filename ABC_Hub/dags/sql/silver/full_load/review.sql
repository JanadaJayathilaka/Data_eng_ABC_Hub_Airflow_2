WITH cleaned AS (
    SELECT
        review_id,
        customer_id,
        content_id,

        CASE
            WHEN rating BETWEEN 1 AND 5 THEN rating
            ELSE NULL
        END AS rating,

        NULLIF(TRIM(review_text), '') AS review_text,
        review_date,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.review
    WHERE review_id IS NOT NULL
)

INSERT INTO silver.review (
    review_id,
    customer_id,
    content_id,
    rating,
    review_text,
    review_date,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    review_id,
    customer_id,
    content_id,
    rating,
    review_text,
    review_date,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;