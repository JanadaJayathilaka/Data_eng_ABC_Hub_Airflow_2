-- ============================================================
-- GOLD INCREMENTAL: FACT_INVENTORY_DAILY_UTILISATION
-- ============================================================

-- 1. Identify which (inventory_id, activity_date) pairs had new/updated rentals or returns
WITH affected_inventory_dates AS (
    SELECT inventory_id, rental_date::DATE AS activity_date
    FROM silver.rental
    WHERE inventory_id IS NOT NULL AND rental_date IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')

    UNION

    SELECT inventory_id, return_date::DATE AS activity_date
    FROM silver.rental
    WHERE inventory_id IS NOT NULL AND return_date IS NOT NULL
      AND (created_at > '{{ last_watermark }}' OR updated_at > '{{ last_watermark }}')
),

-- 2. Recalculate daily events for affected inventory items
rental_events AS (
    SELECT
        r.inventory_id,
        r.rental_date::DATE AS activity_date,
        COUNT(*) AS rental_count
    FROM silver.rental r
    JOIN affected_inventory_dates aid
        ON r.inventory_id = aid.inventory_id AND r.rental_date::DATE = aid.activity_date
    GROUP BY r.inventory_id, r.rental_date::DATE
),

return_events AS (
    SELECT
        r.inventory_id,
        r.return_date::DATE AS activity_date,
        COUNT(*) AS return_count
    FROM silver.rental r
    JOIN affected_inventory_dates aid
        ON r.inventory_id = aid.inventory_id AND r.return_date::DATE = aid.activity_date
    GROUP BY r.inventory_id, r.return_date::DATE
),

prepared_facts AS (
    SELECT
        di.inventory_key,
        di.warehouse_key,
        dd.date_key,
        COALESCE(re.rental_count, 0) AS rental_count,
        COALESCE(ret.return_count, 0) AS return_count,
        CASE
            WHEN COALESCE(re.rental_count, 0) > 0 THEN 0
            ELSE 1
        END AS days_available,
        CASE
            WHEN COALESCE(re.rental_count, 0) > 0 THEN 100.00
            ELSE 0.00
        END AS inventory_utilisation_percentage
    FROM affected_inventory_dates aid
    JOIN LATERAL (
        SELECT inventory_key, warehouse_key
        FROM gold.dim_inventory inv
        WHERE inv.inventory_id = aid.inventory_id
        ORDER BY
            CASE WHEN aid.activity_date BETWEEN inv.effective_from AND inv.effective_to THEN 0 ELSE 1 END,
            inv.is_current DESC
        LIMIT 1
    ) di ON TRUE
    JOIN gold.dim_warehouse dw
        ON di.warehouse_key = dw.warehouse_key
    JOIN gold.dim_date dd
        ON aid.activity_date = dd.full_date
    LEFT JOIN rental_events re
        ON aid.inventory_id = re.inventory_id AND aid.activity_date = re.activity_date
    LEFT JOIN return_events ret
        ON aid.inventory_id = ret.inventory_id AND aid.activity_date = ret.activity_date
),

-- Delete existing fact rows for affected (inventory_key, warehouse_key, date_key)
deleted AS (
    DELETE FROM gold.fact_inventory_daily_utilisation f
    USING prepared_facts pf
    WHERE f.inventory_key = pf.inventory_key
      AND f.warehouse_key = pf.warehouse_key
      AND f.date_key = pf.date_key
)

-- Insert updated utilisation rows
INSERT INTO gold.fact_inventory_daily_utilisation (
    inventory_key,
    warehouse_key,
    date_key,
    rental_count,
    return_count,
    days_available,
    inventory_utilisation_percentage
)
SELECT
    inventory_key,
    warehouse_key,
    date_key,
    rental_count,
    return_count,
    days_available,
    inventory_utilisation_percentage
FROM prepared_facts;
