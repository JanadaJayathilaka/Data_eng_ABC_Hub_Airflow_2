TRUNCATE TABLE gold.dim_warehouse RESTART IDENTITY CASCADE;
INSERT INTO gold.dim_warehouse (
    warehouse_id,
    warehouse_name,
    city
)
SELECT
    w.warehouse_id,
    w.warehouse_name,
    ci.city_name AS city
FROM silver.warehouse w
LEFT JOIN silver.city ci ON w.city_id = ci.city_id;
