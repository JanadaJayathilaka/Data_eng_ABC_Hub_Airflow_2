TRUNCATE TABLE gold.dim_inventory RESTART IDENTITY CASCADE;


INSERT INTO gold.dim_inventory (
    inventory_id,
    content_key,
    warehouse_key,
    barcode,
    purchase_date,
    item_condition,
    status,
    effective_from,
    effective_to,
    created_at,
    updated_at,
    is_current
)
SELECT
    ii.inventory_id,
    dc.content_key,
    dw.warehouse_key,
    ii.barcode,
    ii.purchase_date,
    ii.item_condition,
    ii.status,
    COALESCE(ii.purchase_date, ii.created_at::DATE, '2020-01-01'::DATE) AS effective_from,
    '9999-12-31'::DATE AS effective_to,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
    TRUE AS is_current
FROM silver.inventory_item ii
LEFT JOIN gold.dim_content dc
    ON ii.content_id = dc.content_id AND dc.is_current = TRUE
LEFT JOIN gold.dim_warehouse dw
    ON ii.warehouse_id = dw.warehouse_id;