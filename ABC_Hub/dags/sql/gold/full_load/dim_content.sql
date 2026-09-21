TRUNCATE TABLE gold.dim_content RESTART IDENTITY CASCADE;


INSERT INTO gold.dim_content (
    content_id,
    title,
    release_date,
    duration_minutes,
    language,
    age_rating,
    effective_from,
    effective_to,
    created_at,
    updated_at,
    is_current
)
SELECT
    content_id,
    title,
    release_date,
    duration_minutes,
    language,
    age_rating,
    COALESCE(release_date, created_at::DATE, '2020-01-01'::DATE) AS effective_from,
    '9999-12-31'::DATE AS effective_to,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
    TRUE AS is_current
FROM silver.content;
