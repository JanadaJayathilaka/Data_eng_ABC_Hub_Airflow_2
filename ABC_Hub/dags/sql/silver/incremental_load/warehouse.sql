WITH cleaned AS (
    SELECT
        warehouse_id,
        city_id,
        NULLIF(TRIM(warehouse_name), '') AS warehouse_name,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.warehouse
    WHERE warehouse_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.warehouse
    WHERE warehouse_id IN (SELECT warehouse_id FROM deduped)
)
INSERT INTO silver.warehouse (
    warehouse_id,
    city_id,
    warehouse_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    warehouse_id,
    city_id,
    warehouse_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
