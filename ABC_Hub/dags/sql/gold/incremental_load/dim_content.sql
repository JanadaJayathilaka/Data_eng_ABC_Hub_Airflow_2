-- ============================================================
-- GOLD INCREMENTAL: DIM_CONTENT (SCD Type 2)
-- ============================================================

WITH new_or_updated AS (
    SELECT
        content_id,
        title,
        release_date,
        duration_minutes,
        language,
        age_rating,
        updated_at
    FROM silver.content
    WHERE created_at > '{{ last_watermark }}'
       OR updated_at > '{{ last_watermark }}'
),

close_existing AS (
    UPDATE gold.dim_content d
    SET
        effective_to = (n.updated_at::DATE - INTERVAL '1 day')::DATE,
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP
    FROM new_or_updated n
    WHERE d.content_id = n.content_id
      AND d.is_current = TRUE
      AND (
          d.title IS DISTINCT FROM n.title OR
          d.duration_minutes IS DISTINCT FROM n.duration_minutes OR
          d.language IS DISTINCT FROM n.language OR
          d.age_rating IS DISTINCT FROM n.age_rating
      )
)

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
    n.content_id,
    n.title,
    n.release_date,
    n.duration_minutes,
    n.language,
    n.age_rating,
    n.updated_at::DATE AS effective_from,
    '9999-12-31'::DATE AS effective_to,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    TRUE
FROM new_or_updated n
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_content d
    WHERE d.content_id = n.content_id
      AND d.is_current = TRUE
      AND d.title IS NOT DISTINCT FROM n.title
      AND d.duration_minutes IS NOT DISTINCT FROM n.duration_minutes
      AND d.language IS NOT DISTINCT FROM n.language
      AND d.age_rating IS NOT DISTINCT FROM n.age_rating
);
