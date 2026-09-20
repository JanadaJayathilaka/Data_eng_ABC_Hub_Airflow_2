WITH cleaned AS (
    SELECT
        inventory_id,
        content_id,
        warehouse_id,
        NULLIF(TRIM(barcode), '') AS barcode,
        purchase_date,
        NULLIF(INITCAP(TRIM(item_condition)), '') AS item_condition,
        NULLIF(UPPER(TRIM(status)), '') AS status,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY inventory_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.inventory_item
    WHERE inventory_id IS NOT NULL
)

INSERT INTO silver.inventory_item (
    inventory_id,
    content_id,
    warehouse_id,
    barcode,
    purchase_date,
    item_condition,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    inventory_id,
    content_id,
    warehouse_id,
    barcode,
    purchase_date,
    item_condition,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1;