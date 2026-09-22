TRUNCATE TABLE gold.fact_inventory_daily_utilisation RESTART IDENTITY CASCADE;

WITH
-- 1. Active Date Range
date_range AS (
    SELECT
        COALESCE(MIN(rental_date), '2024-01-01'::DATE) AS min_date,
        COALESCE(MAX(GREATEST(rental_date, return_date, due_date)), CURRENT_DATE) AS max_date
    FROM silver.rental
),

-- 2. Daily Calendar
calendar_days AS (
    SELECT full_date, date_key
    FROM gold.dim_date, date_range
    WHERE full_date BETWEEN min_date AND max_date
),

-- 3. Daily Rental Activity
daily_rentals AS (
    SELECT
        inventory_id,
        rental_date AS event_date,
        COUNT(rental_id) AS rental_count
    FROM silver.rental
    WHERE inventory_id IS NOT NULL AND rental_date IS NOT NULL
    GROUP BY inventory_id, rental_date
),

-- 4. Daily Return Activity
daily_returns AS (
    SELECT
        inventory_id,
        return_date AS event_date,
        COUNT(rental_id) AS return_count
    FROM silver.rental
    WHERE inventory_id IS NOT NULL AND return_date IS NOT NULL
    GROUP BY inventory_id, return_date
),

-- 5. Daily Rented State
active_rental_days AS (
    SELECT DISTINCT
        r.inventory_id,
        cd.full_date,
        cd.date_key
    FROM silver.rental r
    JOIN calendar_days cd
      ON cd.full_date >= r.rental_date
     AND cd.full_date <= COALESCE(r.return_date, r.due_date, r.rental_date)
),

-- 6. Combine all inventory activity days
inventory_days AS (
    SELECT inventory_id, full_date, date_key, TRUE AS is_rented
    FROM active_rental_days
    UNION
    SELECT dr.inventory_id, cd.full_date, cd.date_key, FALSE AS is_rented
    FROM daily_returns dr
    JOIN calendar_days cd ON dr.event_date = cd.full_date
)

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
    dw.warehouse_key,
    id.date_key,

    COALESCE(rent.rental_count, 0) AS rental_count,
    COALESCE(ret.return_count, 0) AS return_count,
    CASE WHEN id.is_rented THEN 0 ELSE 1 END AS days_available,
    CASE WHEN id.is_rented THEN 100.00 ELSE 0.00 END AS inventory_utilisation_percentage

FROM inventory_days id
JOIN silver.inventory_item ii
  ON id.inventory_id = ii.inventory_id
JOIN gold.dim_inventory di
  ON ii.inventory_id = di.inventory_id
 AND id.full_date BETWEEN di.effective_from AND di.effective_to
JOIN gold.dim_warehouse dw
  ON ii.warehouse_id = dw.warehouse_id
LEFT JOIN daily_rentals rent
  ON id.inventory_id = rent.inventory_id AND id.full_date = rent.event_date
LEFT JOIN daily_returns ret
  ON id.inventory_id = ret.inventory_id AND id.full_date = ret.event_date;
