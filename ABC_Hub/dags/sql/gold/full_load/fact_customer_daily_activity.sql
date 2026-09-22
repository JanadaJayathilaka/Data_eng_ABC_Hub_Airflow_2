TRUNCATE TABLE gold.fact_customer_daily_activity RESTART IDENTITY;

WITH customer_dates AS (
    SELECT customer_id,
           start_time::DATE AS activity_date
    FROM silver.streaming_session
    WHERE customer_id IS NOT NULL AND start_time IS NOT NULL

    UNION

    SELECT customer_id, 
           rental_date::DATE AS activity_date 
    FROM silver.rental 
    WHERE customer_id IS NOT NULL AND rental_date IS NOT NULL

    UNION

    SELECT customer_id,
           return_date::DATE AS activity_date
    FROM silver.rental 
    WHERE customer_id IS NOT NULL AND return_date IS NOT NULL

    UNION

    SELECT customer_id,
           payment_date::DATE AS activity_date 
    FROM silver.payment 
    WHERE customer_id IS NOT NULL AND payment_date IS NOT NULL

    UNION
    
    SELECT customer_id,
           opened_date::DATE AS activity_date 
    FROM silver.support_ticket 
    WHERE customer_id IS NOT NULL AND opened_date IS NOT NULL
),

streaming_agg AS ( 
    SELECT customer_id, 
           start_time::DATE AS activity_date, 
           COUNT(stream_id) AS stream_count, 
           COALESCE(SUM(watch_duration), 0)::INTEGER AS total_streaming_duration
    FROM silver.streaming_session 
    WHERE start_time IS NOT NULL AND customer_id IS NOT NULL
    GROUP BY customer_id, start_time::DATE 
),

rental_agg AS ( 
    SELECT customer_id, 
           rental_date::DATE AS activity_date, 
           COUNT(rental_id) AS rental_count 
    FROM silver.rental 
    WHERE rental_date IS NOT NULL AND customer_id IS NOT NULL
    GROUP BY customer_id, rental_date::DATE
),

return_agg AS ( 
    SELECT customer_id, 
           return_date::DATE AS activity_date, 
           COUNT(rental_id) AS returned_item_count 
    FROM silver.rental 
    WHERE return_date IS NOT NULL AND customer_id IS NOT NULL
    GROUP BY customer_id, return_date::DATE 
),

payment_agg AS ( 
    SELECT customer_id, 
           payment_date::DATE AS activity_date, 
           COALESCE(SUM(amount), 0)::NUMERIC(14,2) AS total_amount_spent 
    FROM silver.payment 
    WHERE payment_date IS NOT NULL AND customer_id IS NOT NULL
      AND (status IS NULL OR status = 'COMPLETED') 
    GROUP BY customer_id, payment_date::DATE 
),

ticket_agg AS ( 
    SELECT customer_id, 
           opened_date::DATE AS activity_date, 
           COUNT(ticket_id) AS support_ticket_count 
    FROM silver.support_ticket 
    WHERE opened_date IS NOT NULL AND customer_id IS NOT NULL
    GROUP BY customer_id, opened_date::DATE 
)

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
    d.date_key, 
    dsp.plan_key AS subscription_plan_key, 
    COALESCE(sa.stream_count, 0) AS stream_count, 
    COALESCE(sa.total_streaming_duration, 0) AS total_streaming_duration, 
    COALESCE(ra.rental_count, 0) AS rental_count, 
    COALESCE(rta.returned_item_count, 0) AS returned_item_count, 
    COALESCE(pa.total_amount_spent, 0) AS total_amount_spent, 
    COALESCE(ta.support_ticket_count, 0) AS support_ticket_count 
FROM customer_dates cd 
JOIN gold.dim_date d 
    ON cd.activity_date = d.full_date 
-- SCD Type 2 point-in-time customer lookup
JOIN LATERAL (
    SELECT customer_key
    FROM gold.dim_customer c
    WHERE c.customer_id = cd.customer_id
    ORDER BY
        CASE WHEN cd.activity_date BETWEEN c.effective_from AND c.effective_to THEN 0 ELSE 1 END,
        c.is_current DESC
    LIMIT 1
) dc ON TRUE
-- Safely resolve active subscription plan without duplicate row risk
LEFT JOIN LATERAL (
    SELECT cs.plan_id
    FROM silver.customer_subscription cs
    WHERE cs.customer_id = cd.customer_id
      AND cd.activity_date >= cs.start_date::DATE
      AND cd.activity_date <= COALESCE(cs.end_date::DATE, '9999-12-31'::DATE)
    ORDER BY cs.start_date DESC, cs.updated_at DESC NULLS LAST
    LIMIT 1
) sub ON TRUE
-- Resolve subscription plan surrogate key
LEFT JOIN LATERAL (
    SELECT plan_key
    FROM gold.dim_subscription_plan p
    WHERE p.plan_id = sub.plan_id
    ORDER BY
        CASE WHEN cd.activity_date BETWEEN p.effective_from AND p.effective_to THEN 0 ELSE 1 END,
        p.is_current DESC
    LIMIT 1
) dsp ON TRUE
-- Pre-aggregated metric joins
LEFT JOIN streaming_agg sa 
    ON cd.customer_id = sa.customer_id AND cd.activity_date = sa.activity_date 
LEFT JOIN rental_agg ra 
    ON cd.customer_id = ra.customer_id AND cd.activity_date = ra.activity_date 
LEFT JOIN return_agg rta 
    ON cd.customer_id = rta.customer_id AND cd.activity_date = rta.activity_date 
LEFT JOIN payment_agg pa 
    ON cd.customer_id = pa.customer_id AND cd.activity_date = pa.activity_date 
LEFT JOIN ticket_agg ta 
    ON cd.customer_id = ta.customer_id AND cd.activity_date = ta.activity_date;
