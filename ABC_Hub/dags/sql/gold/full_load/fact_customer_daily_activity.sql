TRUNCATE TABLE gold.fact_customer_daily_activity RESTART IDENTITY;

-- 1. Pre-aggregate streaming activity per customer per day
WITH stream_agg AS (
    SELECT
        customer_id,
        start_time::DATE AS activity_date,
        COUNT(*) AS stream_count,
        COALESCE(SUM(watch_duration), 0)::INTEGER AS total_streaming_duration
    FROM silver.streaming_session
    WHERE start_time IS NOT NULL AND customer_id IS NOT NULL
    GROUP BY customer_id, start_time::DATE
),

-- 2. Pre-aggregate rental starts per customer per day
rental_agg AS (
    SELECT
        customer_id,
        rental_date::DATE AS activity_date,
        COUNT(*) AS rental_count
    FROM silver.rental
    WHERE rental_date IS NOT NULL AND customer_id IS NOT NULL
    GROUP BY customer_id, rental_date::DATE
),

-- 3. Pre-aggregate returned items per customer per day
return_agg AS (
    SELECT
        customer_id,
        return_date::DATE AS activity_date,
        COUNT(*) AS returned_item_count
    FROM silver.rental
    WHERE return_date IS NOT NULL AND customer_id IS NOT NULL
    GROUP BY customer_id, return_date::DATE
),

-- 4. Pre-aggregate payments per customer per day
payment_agg AS (
    SELECT
        customer_id,
        payment_date::DATE AS activity_date,
        COALESCE(SUM(amount), 0)::NUMERIC(14,2) AS total_amount_spent
    FROM silver.payment
    WHERE payment_date IS NOT NULL AND customer_id IS NOT NULL
    GROUP BY customer_id, payment_date::DATE
),

-- 5. Pre-aggregate support tickets opened per customer per day
ticket_agg AS (
    SELECT
        customer_id,
        opened_date::DATE AS activity_date,
        COUNT(*) AS support_ticket_count
    FROM silver.support_ticket
    WHERE opened_date IS NOT NULL AND customer_id IS NOT NULL
    GROUP BY customer_id, opened_date::DATE
),

-- 6. Combine all distinct (customer_id, activity_date) pairs
distinct_activities AS (
    SELECT customer_id, activity_date FROM stream_agg
    UNION
    SELECT customer_id, activity_date FROM rental_agg
    UNION
    SELECT customer_id, activity_date FROM return_agg
    UNION
    SELECT customer_id, activity_date FROM payment_agg
    UNION
    SELECT customer_id, activity_date FROM ticket_agg
),

-- 7. Find active subscription plan surrogate key for each customer on each activity date
customer_active_plan AS (
    SELECT
        cs.customer_id,
        act.activity_date,
        dsp.plan_key,
        ROW_NUMBER() OVER (
            PARTITION BY cs.customer_id, act.activity_date
            ORDER BY cs.start_date DESC, cs.subscription_id DESC
        ) AS rn
    FROM distinct_activities act
    JOIN silver.customer_subscription cs
        ON act.customer_id = cs.customer_id
        AND act.activity_date >= cs.start_date::DATE
        AND (cs.end_date IS NULL OR act.activity_date <= cs.end_date::DATE)
    LEFT JOIN LATERAL (
        SELECT plan_key
        FROM gold.dim_subscription_plan dsp
        WHERE dsp.plan_id = cs.plan_id
        ORDER BY
            CASE WHEN act.activity_date BETWEEN dsp.effective_from AND dsp.effective_to THEN 0 ELSE 1 END,
            dsp.is_current DESC,
            dsp.plan_key DESC
        LIMIT 1
    ) dsp ON TRUE
)

-- 8. Insert into gold fact table
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
    dc.customer_key,
    dd.date_key,
    cap.plan_key AS subscription_plan_key,
    COALESCE(st.stream_count, 0) AS stream_count,
    COALESCE(st.total_streaming_duration, 0) AS total_streaming_duration,
    COALESCE(rt.rental_count, 0) AS rental_count,
    COALESCE(ret.returned_item_count, 0) AS returned_item_count,
    COALESCE(py.total_amount_spent, 0.00) AS total_amount_spent,
    COALESCE(tk.support_ticket_count, 0) AS support_ticket_count
FROM distinct_activities da
-- SCD Type 2 point-in-time customer lookup
JOIN LATERAL (
    SELECT customer_key
    FROM gold.dim_customer c
    WHERE c.customer_id = da.customer_id
    ORDER BY
        CASE WHEN da.activity_date BETWEEN c.effective_from AND c.effective_to THEN 0 ELSE 1 END,
        c.is_current DESC,
        c.customer_key DESC
    LIMIT 1
) dc ON TRUE
-- Date dimension lookup
JOIN gold.dim_date dd
    ON da.activity_date = dd.full_date
-- Active subscription lookup
LEFT JOIN customer_active_plan cap
    ON da.customer_id = cap.customer_id
    AND da.activity_date = cap.activity_date
    AND cap.rn = 1
-- Metric joins
LEFT JOIN stream_agg st
    ON da.customer_id = st.customer_id AND da.activity_date = st.activity_date
LEFT JOIN rental_agg rt
    ON da.customer_id = rt.customer_id AND da.activity_date = rt.activity_date
LEFT JOIN return_agg ret
    ON da.customer_id = ret.customer_id AND da.activity_date = ret.activity_date
LEFT JOIN payment_agg py
    ON da.customer_id = py.customer_id AND da.activity_date = py.activity_date
LEFT JOIN ticket_agg tk
    ON da.customer_id = tk.customer_id AND da.activity_date = tk.activity_date;
