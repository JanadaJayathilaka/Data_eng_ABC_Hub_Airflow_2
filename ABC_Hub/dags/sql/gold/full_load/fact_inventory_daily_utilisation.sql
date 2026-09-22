TRUNCATE TABLE gold.fact_inventory_daily_utilisation RESTART IDENTITY;

-- 1. Daily rental events per inventory item
WITH rental_events AS (
    SELECT
        inventory_id,
        rental_date::DATE AS activity_date,
        COUNT(*) AS rental_count
    FROM silver.rental
    WHERE rental_date IS NOT NULL AND inventory_id IS NOT NULL
    GROUP BY inventory_id, rental_date::DATE
),

-- 2. Daily return events per inventory item
return_events AS (
    SELECT
        inventory_id,
        return_date::DATE AS activity_date,
        COUNT(*) AS return_count
    FROM silver.rental
    WHERE return_date IS NOT NULL AND inventory_id IS NOT NULL
    GROUP BY inventory_id, return_date::DATE
),

-- 3. Combine distinct inventory dates
distinct_inventory_dates AS (
    SELECT inventory_id, activity_date FROM rental_events
    UNION
    SELECT inventory_id, activity_date FROM return_events
)

-- 4. Insert into gold fact table
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
    di.inventory_key,
    di.warehouse_key,
    dd.date_key,
    COALESCE(re.rental_count, 0) AS rental_count,
    COALESCE(ret.return_count, 0) AS return_count,
    -- If rented on this day, available days = 0, otherwise 1
    CASE
        WHEN COALESCE(re.rental_count, 0) > 0 THEN 0
        ELSE 1
    END AS days_available,
    -- 100% utilisation if rented on that day, else 0%
    CASE
        WHEN COALESCE(re.rental_count, 0) > 0 THEN 100.00
        ELSE 0.00
    END AS inventory_utilisation_percentage
FROM distinct_inventory_dates did
-- Inventory dimension lookup
JOIN LATERAL (
    SELECT inventory_key, warehouse_key
    FROM gold.dim_inventory inv
    WHERE inv.inventory_id = did.inventory_id
    ORDER BY
        CASE WHEN did.activity_date BETWEEN inv.effective_from AND inv.effective_to THEN 0 ELSE 1 END,
        inv.is_current DESC,
        inv.inventory_key DESC
    LIMIT 1
) di ON TRUE
-- Warehouse dimension lookup (ensures FK validity)
JOIN gold.dim_warehouse dw
    ON di.warehouse_key = dw.warehouse_key
-- Date dimension lookup
JOIN gold.dim_date dd
    ON did.activity_date = dd.full_date
LEFT JOIN rental_events re
    ON did.inventory_id = re.inventory_id AND did.activity_date = re.activity_date
LEFT JOIN return_events ret
    ON did.inventory_id = ret.inventory_id AND did.activity_date = ret.activity_date;
