WITH cleaned AS (
    SELECT
        device_id,
        NULLIF(INITCAP(TRIM(device_name)), '') AS device_name,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY device_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.device
    WHERE device_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.device
    WHERE device_id IN (SELECT device_id FROM deduped)
)
INSERT INTO silver.device (
    device_id,
    device_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    device_id,
    device_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
