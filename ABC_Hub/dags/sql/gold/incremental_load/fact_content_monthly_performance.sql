-- ============================================================
-- GOLD INCREMENTAL: FACT_CONTENT_MONTHLY_PERFORMANCE
-- ============================================================

-- 1. Identify which (content_id, month_key) pairs had new/updated activity
WITH affected_content_months AS (
    SELECT content_id, TO_CHAR(start_time, 'YYYYMM')::INTEGER AS month_key
    FROM silver.streaming_session
    WHERE content_id IS NOT NULL AND start_time IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')

    UNION

    SELECT ii.content_id, TO_CHAR(r.rental_date, 'YYYYMM')::INTEGER AS month_key
    FROM silver.rental r
    JOIN silver.inventory_item ii ON r.inventory_id = ii.inventory_id
    WHERE ii.content_id IS NOT NULL AND r.rental_date IS NOT NULL
      AND (r.created_at > '{{ last_watermark }}' OR r.updated_at > '{{ last_watermark }}')

    UNION

    SELECT content_id, TO_CHAR(review_date, 'YYYYMM')::INTEGER AS month_key
    FROM silver.review
    WHERE content_id IS NOT NULL AND review_date IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')

    UNION

    SELECT content_id, TO_CHAR(added_date, 'YYYYMM')::INTEGER AS month_key
    FROM silver.wishlist
    WHERE content_id IS NOT NULL AND added_date IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')
),

-- 2. Recalculate metrics for affected content months
stream_monthly AS (
    SELECT
        s.content_id,
        TO_CHAR(s.start_time, 'YYYYMM')::INTEGER AS month_key,
        COUNT(*) AS total_stream_count
    FROM silver.streaming_session s
    JOIN affected_content_months acm
        ON s.content_id = acm.content_id AND TO_CHAR(s.start_time, 'YYYYMM')::INTEGER = acm.month_key
    GROUP BY s.content_id, TO_CHAR(s.start_time, 'YYYYMM')::INTEGER
),

rental_monthly AS (
    SELECT
        ii.content_id,
        TO_CHAR(r.rental_date, 'YYYYMM')::INTEGER AS month_key,
        COUNT(*) AS total_rental_count,
        COALESCE(SUM(COALESCE(r.rental_fee, 0) + COALESCE(r.late_fee, 0)), 0)::NUMERIC(14,2) AS revenue_generated
    FROM silver.rental r
    JOIN silver.inventory_item ii ON r.inventory_id = ii.inventory_id
    JOIN affected_content_months acm
        ON ii.content_id = acm.content_id AND TO_CHAR(r.rental_date, 'YYYYMM')::INTEGER = acm.month_key
    GROUP BY ii.content_id, TO_CHAR(r.rental_date, 'YYYYMM')::INTEGER
),

review_monthly AS (
    SELECT
        rev.content_id,
        TO_CHAR(rev.review_date, 'YYYYMM')::INTEGER AS month_key,
        ROUND(AVG(rev.rating), 2)::NUMERIC(5,2) AS average_customer_rating
    FROM silver.review rev
    JOIN affected_content_months acm
        ON rev.content_id = acm.content_id AND TO_CHAR(rev.review_date, 'YYYYMM')::INTEGER = acm.month_key
    WHERE rev.rating IS NOT NULL
    GROUP BY rev.content_id, TO_CHAR(rev.review_date, 'YYYYMM')::INTEGER
),

wishlist_monthly AS (
    SELECT
        w.content_id,
        TO_CHAR(w.added_date, 'YYYYMM')::INTEGER AS month_key,
        COUNT(*) AS wishlist_addition_count
    FROM silver.wishlist w
    JOIN affected_content_months acm
        ON w.content_id = acm.content_id AND TO_CHAR(w.added_date, 'YYYYMM')::INTEGER = acm.month_key
    GROUP BY w.content_id, TO_CHAR(w.added_date, 'YYYYMM')::INTEGER
),

prepared_facts AS (
    SELECT
        dc.content_key,
        acm.month_key,
        COALESCE(sm.total_stream_count, 0) AS total_stream_count,
        COALESCE(rm.total_rental_count, 0) AS total_rental_count,
        COALESCE(rm.revenue_generated, 0.00) AS revenue_generated,
        rev.average_customer_rating,
        COALESCE(wm.wishlist_addition_count, 0) AS wishlist_addition_count
    FROM affected_content_months acm
    JOIN gold.dim_month dm
        ON acm.month_key = dm.month_key
    JOIN LATERAL (
        SELECT content_key
        FROM gold.dim_content c
        WHERE c.content_id = acm.content_id
        ORDER BY
            CASE WHEN TO_DATE(acm.month_key::TEXT || '01', 'YYYYMMDD') BETWEEN c.effective_from AND c.effective_to THEN 0 ELSE 1 END,
            c.is_current DESC
        LIMIT 1
    ) dc ON TRUE
    LEFT JOIN stream_monthly sm
        ON acm.content_id = sm.content_id AND acm.month_key = sm.month_key
    LEFT JOIN rental_monthly rm
        ON acm.content_id = rm.content_id AND acm.month_key = rm.month_key
    LEFT JOIN review_monthly rev
        ON acm.content_id = rev.content_id AND acm.month_key = rev.month_key
    LEFT JOIN wishlist_monthly wm
        ON acm.content_id = wm.content_id AND acm.month_key = wm.month_key
),

-- Delete existing fact rows for affected (content_key, month_key)
deleted AS (
    DELETE FROM gold.fact_content_monthly_performance f
    USING prepared_facts pf
    WHERE f.content_key = pf.content_key
      AND f.month_key = pf.month_key
)

-- Insert updated monthly performance
INSERT INTO gold.fact_content_monthly_performance (
    content_key,
    month_key,
    total_stream_count,
    total_rental_count,
    revenue_generated,
    average_customer_rating,
    wishlist_addition_count
)
SELECT
    content_key,
    month_key,
    total_stream_count,
    total_rental_count,
    revenue_generated,
    average_customer_rating,
    wishlist_addition_count
FROM prepared_facts;
