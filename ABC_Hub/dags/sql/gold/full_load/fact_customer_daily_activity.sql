TRUNCATE TABLE gold.fact_customer_daily_activity RESTART IDENTITY;

with customer_dates as (
    select customer_id,
           start_time::DATE AS activity_date
    from silver.streaming_session
    where customer_id is not null and start_time is not null
    
    UNION

    SELECT customer_id, 
    rental_date AS activity_date 
    FROM silver.rental 
    WHERE customer_id IS NOT NULL AND rental_date IS NOT NULL

    UNION

    SELECT customer_id,
    return_date AS activity_date
    FROM silver.rental 
    WHERE customer_id IS NOT NULL AND return_date IS NOT NULL

    UNION

    SELECT customer_id,
    payment_date AS activity_date 
    FROM silver.payment 
    WHERE customer_id IS NOT NULL AND payment_date IS NOT NULL

    UNION
    
    SELECT customer_id,
    opened_date AS activity_date 
    FROM silver.support_ticket 
    WHERE customer_id IS NOT NULL AND opened_date IS NOT NULL
   
),
streaming_agg AS ( 
    SELECT customer_id, 
    start_time::DATE AS activity_date, 
    COUNT(stream_id) AS stream_count, 
    COALESCE(SUM(watch_duration), 0) AS total_streaming_duration
    FROM silver.streaming_session 
    GROUP BY customer_id, start_time::DATE 
),

rental_agg AS ( 
    SELECT customer_id, 
    rental_date AS activity_date, 
    COUNT(rental_id) AS rental_count 
    FROM silver.rental GROUP BY customer_id, rental_date
),

return_agg AS ( 
    SELECT customer_id, 
    return_date AS activity_date, 
    COUNT(rental_id) AS returned_item_count 
    FROM silver.rental 
    WHERE return_date IS NOT NULL 
    GROUP BY customer_id, return_date 
),

payment_agg AS ( 
    SELECT customer_id, 
    payment_date AS activity_date, 
    COALESCE(SUM(amount), 0) AS total_amount_spent 
    FROM silver.payment WHERE status = 'COMPLETED' 
    GROUP BY customer_id, payment_date 
),

ticket_agg AS ( 
    SELECT customer_id, 
    opened_date AS activity_date, 
    COUNT(ticket_id) AS support_ticket_count 
    FROM silver.support_ticket 
    GROUP BY customer_id, opened_date 
),

active_subscription AS ( 
    SELECT cs.customer_id, 
    cs.plan_id, 
    cs.start_date, 
    COALESCE(cs.end_date, '9999-12-31'::DATE) AS end_date, 
    ROW_NUMBER() OVER ( PARTITION BY cs.customer_id, cs.start_date ORDER BY cs.updated_at DESC NULLS LAST ) AS rn 
    FROM silver.customer_subscription cs 
),

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
JOIN gold.dim_date d ON cd.activity_date = d.full_date 
JOIN gold.dim_customer dc ON cd.customer_id = dc.customer_id AND cd.activity_date BETWEEN dc.effective_from AND dc.effective_to 
LEFT JOIN active_subscription sub ON cd.customer_id = sub.customer_id AND cd.activity_date BETWEEN sub.start_date AND sub.end_date AND sub.rn = 1 
LEFT JOIN gold.dim_subscription_plan dsp ON sub.plan_id = dsp.plan_id AND cd.activity_date BETWEEN dsp.effective_from AND dsp.effective_to 
LEFT JOIN streaming_agg sa ON cd.customer_id = sa.customer_id AND cd.activity_date = sa.activity_date 
LEFT JOIN rental_agg ra ON cd.customer_id = ra.customer_id AND cd.activity_date = ra.activity_date 
LEFT JOIN return_agg rta ON cd.customer_id = rta.customer_id AND cd.activity_date = rta.activity_date 
LEFT JOIN payment_agg pa ON cd.customer_id = pa.customer_id AND cd.activity_date = pa.activity_date 
LEFT JOIN ticket_agg ta ON cd.customer_id = ta.customer_id AND cd.activity_date = ta.activity_date;
