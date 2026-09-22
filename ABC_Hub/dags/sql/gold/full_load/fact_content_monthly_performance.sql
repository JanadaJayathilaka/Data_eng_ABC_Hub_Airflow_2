TRUNCATE TABLE gold.fact_content_monthly_performance RESTART IDENTITY CASCADE;

WITH
-- 1. Monthly Streaming Count per Content
streaming_monthly AS (
    SELECT
        content_id,
        TO_CHAR(start_time, 'YYYYMM')::INTEGER AS month_key,
        COUNT(stream_id) AS total_stream_count
    FROM silver.streaming_session
    WHERE content_id IS NOT NULL AND start_time IS NOT NULL
    GROUP BY content_id, TO_CHAR(start_time, 'YYYYMM')::INTEGER
),

-- 2. Monthly Physical Rentals & Revenue per Content
rentals_monthly AS (
    SELECT
        ii.content_id,
        TO_CHAR(r.rental_date, 'YYYYMM')::INTEGER AS month_key,
        COUNT(r.rental_id) AS total_rental_count,
        COALESCE(SUM(COALESCE(r.rental_fee, 0) + COALESCE(r.late_fee, 0)), 0.00) AS rental_revenue
    FROM silver.rental r
    JOIN silver.inventory_item ii
      ON r.inventory_id = ii.inventory_id
    WHERE ii.content_id IS NOT NULL AND r.rental_date IS NOT NULL
    GROUP BY ii.content_id, TO_CHAR(r.rental_date, 'YYYYMM')::INTEGER
),

-- 3. Monthly Average Customer Rating per Content
reviews_monthly AS (
    SELECT
        content_id,
        TO_CHAR(review_date, 'YYYYMM')::INTEGER AS month_key,
        ROUND(AVG(rating)::NUMERIC, 2) AS average_customer_rating
    FROM silver.review
    WHERE content_id IS NOT NULL AND review_date IS NOT NULL AND rating IS NOT NULL
    GROUP BY content_id, TO_CHAR(review_date, 'YYYYMM')::INTEGER
),

-- 4. Monthly Wishlist Additions per Content
wishlist_monthly AS (
    SELECT
        content_id,
        TO_CHAR(added_date, 'YYYYMM')::INTEGER AS month_key,
        COUNT(wishlist_id) AS wishlist_addition_count
    FROM silver.wishlist
    WHERE content_id IS NOT NULL AND added_date IS NOT NULL
    GROUP BY content_id, TO_CHAR(added_date, 'YYYYMM')::INTEGER
),

-- 5. Combine all distinct (content_id, month_key) occurrences
content_months AS (
    SELECT content_id, month_key FROM streaming_monthly
    UNION
    SELECT content_id, month_key FROM rentals_monthly
    UNION
    SELECT content_id, month_key FROM reviews_monthly
    UNION
    SELECT content_id, month_key FROM wishlist_monthly
)

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
    cm.month_key,

    COALESCE(sm.total_stream_count, 0) AS total_stream_count,
    COALESCE(rm.total_rental_count, 0) AS total_rental_count,
    COALESCE(rm.rental_revenue, 0.00) AS revenue_generated,
    rev.average_customer_rating AS average_customer_rating,
    COALESCE(wm.wishlist_addition_count, 0) AS wishlist_addition_count

FROM content_months cm
JOIN gold.dim_month dm
  ON cm.month_key = dm.month_key
JOIN gold.dim_content dc
  ON cm.content_id = dc.content_id
 AND TO_DATE(cm.month_key::TEXT || '01', 'YYYYMMDD') BETWEEN dc.effective_from AND dc.effective_to
LEFT JOIN streaming_monthly sm
  ON cm.content_id = sm.content_id AND cm.month_key = sm.month_key
LEFT JOIN rentals_monthly rm
  ON cm.content_id = rm.content_id AND cm.month_key = rm.month_key
LEFT JOIN reviews_monthly rev
  ON cm.content_id = rev.content_id AND cm.month_key = rev.month_key
LEFT JOIN wishlist_monthly wm
  ON cm.content_id = wm.content_id AND cm.month_key = wm.month_key;
