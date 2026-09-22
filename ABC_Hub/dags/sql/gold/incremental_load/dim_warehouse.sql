-- ============================================================
-- GOLD INCREMENTAL: DIM_WAREHOUSE (Type 1 Upsert)
-- ============================================================

WITH new_or_updated AS (
    SELECT
        w.warehouse_id,
        w.warehouse_name,
        ci.city_name AS city
    FROM silver.warehouse w
    LEFT JOIN silver.city ci ON w.city_id = ci.city_id
    WHERE w.created_at > '{{ last_watermark }}'
       OR w.updated_at > '{{ last_watermark }}'
),

deleted AS (
    DELETE FROM gold.dim_warehouse
    WHERE warehouse_id IN (SELECT warehouse_id FROM new_or_updated)
)

INSERT INTO gold.dim_warehouse (
    warehouse_id,
    warehouse_name,
    city
)
SELECT
    warehouse_id,
    warehouse_name,
    city
FROM new_or_updated;
