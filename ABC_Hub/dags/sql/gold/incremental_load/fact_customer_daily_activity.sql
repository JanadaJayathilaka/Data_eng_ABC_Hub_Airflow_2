-- ============================================================
-- GOLD INCREMENTAL: FACT_CUSTOMER_DAILY_ACTIVITY
-- ============================================================

-- 1. Identify which (customer_id, activity_date) pairs had new/updated events
WITH affected_customer_dates AS (
    SELECT customer_id, start_time::DATE AS activity_date
    FROM silver.streaming_session
    WHERE customer_id IS NOT NULL AND start_time IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')

    UNION

    SELECT customer_id, rental_date::DATE AS activity_date
    FROM silver.rental
    WHERE customer_id IS NOT NULL AND rental_date IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')

    UNION

    SELECT customer_id, return_date::DATE AS activity_date
    FROM silver.rental
    WHERE customer_id IS NOT NULL AND return_date IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')

    UNION

    SELECT customer_id, payment_date::DATE AS activity_date
    FROM silver.payment
    WHERE customer_id IS NOT NULL AND payment_date IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')

    UNION

    SELECT customer_id, opened_date::DATE AS activity_date
    FROM silver.support_ticket
    WHERE customer_id IS NOT NULL AND opened_date IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')
),

-- 2. Recalculate metrics for affected (customer_id, activity_date)
stream_agg AS (
    SELECT
        s.customer_id,
        s.start_time::DATE AS activity_date,
        COUNT(*) AS stream_count,
        COALESCE(SUM(s.watch_duration), 0)::INTEGER AS total_streaming_duration
    FROM silver.streaming_session s
    JOIN affected_customer_dates acd
        ON s.customer_id = acd.customer_id AND s.start_time::DATE = acd.activity_date
    GROUP BY s.customer_id, s.start_time::DATE
),

rental_agg AS (
    SELECT
        r.customer_id,
        r.rental_date::DATE AS activity_date,
        COUNT(*) AS rental_count
    FROM silver.rental r
    JOIN affected_customer_dates acd
        ON r.customer_id = acd.customer_id AND r.rental_date::DATE = acd.activity_date
    GROUP BY r.customer_id, r.rental_date::DATE
),

return_agg AS (
    SELECT
        r.customer_id,
        r.return_date::DATE AS activity_date,
        COUNT(*) AS returned_item_count
    FROM silver.rental r
    JOIN affected_customer_dates acd
        ON r.customer_id = acd.customer_id AND r.return_date::DATE = acd.activity_date
    GROUP BY r.customer_id, r.return_date::DATE
),

payment_agg AS (
    SELECT
        p.customer_id,
        p.payment_date::DATE AS activity_date,
        COALESCE(SUM(p.amount), 0)::NUMERIC(14,2) AS total_amount_spent
    FROM silver.payment p
    JOIN affected_customer_dates acd
        ON p.customer_id = acd.customer_id AND p.payment_date::DATE = acd.activity_date
    WHERE (p.status IS NULL OR p.status = 'COMPLETED')
    GROUP BY p.customer_id, p.payment_date::DATE
),

ticket_agg AS (
    SELECT
        t.customer_id,
        t.opened_date::DATE AS activity_date,
        COUNT(*) AS support_ticket_count
    FROM silver.support_ticket t
    JOIN affected_customer_dates acd
        ON t.customer_id = acd.customer_id AND t.opened_date::DATE = acd.activity_date
    GROUP BY t.customer_id, t.opened_date::DATE
),

-- Active subscription plan lookup
customer_active_plan AS (
    SELECT
        cs.customer_id,
        acd.activity_date,
        dsp.plan_key,
        ROW_NUMBER() OVER (
            PARTITION BY cs.customer_id, acd.activity_date
            ORDER BY cs.start_date DESC, cs.subscription_id DESC
        ) AS rn
    FROM affected_customer_dates acd
    JOIN silver.customer_subscription cs
        ON acd.customer_id = cs.customer_id
        AND acd.activity_date >= cs.start_date::DATE
        AND (cs.end_date IS NULL OR acd.activity_date <= cs.end_date::DATE)
    LEFT JOIN LATERAL (
        SELECT plan_key
        FROM gold.dim_subscription_plan dsp
        WHERE dsp.plan_id = cs.plan_id
        ORDER BY
            CASE WHEN acd.activity_date BETWEEN dsp.effective_from AND dsp.effective_to THEN 0 ELSE 1 END,
            dsp.is_current DESC,
            dsp.plan_key DESC
        LIMIT 1
    ) dsp ON TRUE
),

prepared_facts AS (
    SELECT
        dc.customer_key,
        dd.date_key,
        cap.plan_key AS subscription_plan_key,
        COALESCE(st.stream_count, 0) AS stream_count,
        COALESCE(st.total_streaming_duration, 0) AS total_streaming_duration,
        COALESCE(rt.rental_count, 0) AS rental_count,
        COALESCE(ret.returned_item_count, 0) AS returned_item_count,
        COALESCE(py.total_amount_spent, 0.00) AS total_amount_spent,
        COALESCE(tk.support_ticket_count, 0) AS support_ticket_count
    FROM affected_customer_dates acd
    JOIN LATERAL (
        SELECT customer_key
        FROM gold.dim_customer c
        WHERE c.customer_id = acd.customer_id
        ORDER BY
            CASE WHEN acd.activity_date BETWEEN c.effective_from AND c.effective_to THEN 0 ELSE 1 END,
            c.is_current DESC
        LIMIT 1
    ) dc ON TRUE
    JOIN gold.dim_date dd
        ON acd.activity_date = dd.full_date
    LEFT JOIN customer_active_plan cap
        ON acd.customer_id = cap.customer_id
        AND acd.activity_date = cap.activity_date
        AND cap.rn = 1
    LEFT JOIN stream_agg st
        ON acd.customer_id = st.customer_id AND acd.activity_date = st.activity_date
    LEFT JOIN rental_agg rt
        ON acd.customer_id = rt.customer_id AND acd.activity_date = rt.activity_date
    LEFT JOIN return_agg ret
        ON acd.customer_id = ret.customer_id AND acd.activity_date = ret.activity_date
    LEFT JOIN payment_agg py
        ON acd.customer_id = py.customer_id AND acd.activity_date = py.activity_date
    LEFT JOIN ticket_agg tk
        ON acd.customer_id = tk.customer_id AND acd.activity_date = tk.activity_date
),

-- Delete existing fact rows for affected (customer_key, date_key) to prevent duplicates
deleted AS (
    DELETE FROM gold.fact_customer_daily_activity f
    USING prepared_facts pf
    WHERE f.customer_key = pf.customer_key
      AND f.date_key = pf.date_key
)

-- Insert updated daily metrics
INSERT INTO gold.fact_customer_daily_activity (
    customer_key,
    date_key,
    subscription_plan_key,
    stream_count,
    total_streaming_duration,
    rental_count,
    returned_item_count,
    total_amount_spent,
    support_ticket_count
)
SELECT
    customer_key,
    date_key,
    subscription_plan_key,
    stream_count,
    total_streaming_duration,
    rental_count,
    returned_item_count,
    total_amount_spent,
    support_ticket_count
FROM prepared_facts;
