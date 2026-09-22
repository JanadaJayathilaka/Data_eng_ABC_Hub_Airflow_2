TRUNCATE TABLE gold.fact_content_monthly_performance RESTART IDENTITY;

-- 1. Stream count per content per month
WITH stream_monthly AS (
    SELECT
        content_id,
        TO_CHAR(start_time, 'YYYYMM')::INTEGER AS month_key,
        COUNT(*) AS total_stream_count
    FROM silver.streaming_session
    WHERE start_time IS NOT NULL AND content_id IS NOT NULL
    GROUP BY content_id, TO_CHAR(start_time, 'YYYYMM')::INTEGER
),

-- 2. Rental count & revenue per content per month
rental_monthly AS (
    SELECT
        ii.content_id,
        TO_CHAR(r.rental_date, 'YYYYMM')::INTEGER AS month_key,
        COUNT(*) AS total_rental_count,
        COALESCE(SUM(COALESCE(r.rental_fee, 0) + COALESCE(r.late_fee, 0)), 0)::NUMERIC(14,2) AS revenue_generated
    FROM silver.rental r
    JOIN silver.inventory_item ii
        ON r.inventory_id = ii.inventory_id
    WHERE r.rental_date IS NOT NULL AND ii.content_id IS NOT NULL
    GROUP BY ii.content_id, TO_CHAR(r.rental_date, 'YYYYMM')::INTEGER
),

-- 3. Review average rating per content per month
review_monthly AS (
    SELECT
        content_id,
        TO_CHAR(review_date, 'YYYYMM')::INTEGER AS month_key,
        ROUND(AVG(rating), 2)::NUMERIC(5,2) AS average_customer_rating
    FROM silver.review
    WHERE review_date IS NOT NULL AND content_id IS NOT NULL AND rating IS NOT NULL
    GROUP BY content_id, TO_CHAR(review_date, 'YYYYMM')::INTEGER
),

-- 4. Wishlist additions per content per month
wishlist_monthly AS (
    SELECT
        content_id,
        TO_CHAR(added_date, 'YYYYMM')::INTEGER AS month_key,
        COUNT(*) AS wishlist_addition_count
    FROM silver.wishlist
    WHERE added_date IS NOT NULL AND content_id IS NOT NULL
    GROUP BY content_id, TO_CHAR(added_date, 'YYYYMM')::INTEGER
),

-- 5. Combine distinct (content_id, month_key) pairs
distinct_content_months AS (
    SELECT content_id, month_key FROM stream_monthly
    UNION
    SELECT content_id, month_key FROM rental_monthly
    UNION
    SELECT content_id, month_key FROM review_monthly
    UNION
    SELECT content_id, month_key FROM wishlist_monthly
)

-- 6. Insert into gold fact table
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
    dc.content_key,
    dcm.month_key,
    COALESCE(sm.total_stream_count, 0) AS total_stream_count,
    COALESCE(rm.total_rental_count, 0) AS total_rental_count,
    COALESCE(rm.revenue_generated, 0.00) AS revenue_generated,
    rev.average_customer_rating,
    COALESCE(wm.wishlist_addition_count, 0) AS wishlist_addition_count
FROM distinct_content_months dcm
-- Month dimension lookup
JOIN gold.dim_month dm
    ON dcm.month_key = dm.month_key
-- SCD Type 2 Content dimension lookup (using 1st day of the month as reference date)
JOIN LATERAL (
    SELECT content_key
    FROM gold.dim_content c
    WHERE c.content_id = dcm.content_id
    ORDER BY
        CASE WHEN TO_DATE(dcm.month_key::TEXT || '01', 'YYYYMMDD') BETWEEN c.effective_from AND c.effective_to THEN 0 ELSE 1 END,
        c.is_current DESC,
        c.content_key DESC
    LIMIT 1
) dc ON TRUE
-- Metric joins
LEFT JOIN stream_monthly sm
    ON dcm.content_id = sm.content_id AND dcm.month_key = sm.month_key
LEFT JOIN rental_monthly rm
    ON dcm.content_id = rm.content_id AND dcm.month_key = rm.month_key
LEFT JOIN review_monthly rev
    ON dcm.content_id = rev.content_id AND dcm.month_key = rev.month_key
LEFT JOIN wishlist_monthly wm
    ON dcm.content_id = wm.content_id AND dcm.month_key = wm.month_key;
