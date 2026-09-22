-- ============================================================
-- GOLD INCREMENTAL: DIM_INVENTORY (SCD Type 2)
-- ============================================================

WITH new_or_updated AS (
    SELECT
        ii.inventory_id,
        dc.content_key,
        dw.warehouse_key,
        ii.barcode,
        ii.purchase_date,
        ii.item_condition,
        ii.status,
        ii.updated_at
    FROM silver.inventory_item ii
    LEFT JOIN gold.dim_content dc ON ii.content_id = dc.content_id AND dc.is_current = TRUE
    LEFT JOIN gold.dim_warehouse dw ON ii.warehouse_id = dw.warehouse_id
    WHERE ii.created_at > '{{ last_watermark }}'
       OR ii.updated_at > '{{ last_watermark }}'
),

close_existing AS (
    UPDATE gold.dim_inventory d
    SET
        effective_to = (n.updated_at::DATE - INTERVAL '1 day')::DATE,
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP
    FROM new_or_updated n
    WHERE d.inventory_id = n.inventory_id
      AND d.is_current = TRUE
      AND (
          d.item_condition IS DISTINCT FROM n.item_condition OR
          d.status IS DISTINCT FROM n.status OR
          d.warehouse_key IS DISTINCT FROM n.warehouse_key
      )
)

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
    n.inventory_id,
    n.content_key,
    n.warehouse_key,
    n.barcode,
    n.purchase_date,
    n.item_condition,
    n.status,
    n.updated_at::DATE AS effective_from,
    '9999-12-31'::DATE AS effective_to,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    TRUE
FROM new_or_updated n
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_inventory d
    WHERE d.inventory_id = n.inventory_id
      AND d.is_current = TRUE
      AND d.item_condition IS NOT DISTINCT FROM n.item_condition
      AND d.status IS NOT DISTINCT FROM n.status
      AND d.warehouse_key IS NOT DISTINCT FROM n.warehouse_key
);
