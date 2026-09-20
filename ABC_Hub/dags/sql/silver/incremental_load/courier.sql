WITH cleaned AS (
    SELECT
        courier_id,
        NULLIF(INITCAP(TRIM(courier_name)), '') AS courier_name,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY courier_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn
    FROM bronze.courier
    WHERE courier_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.courier
    WHERE courier_id IN (SELECT courier_id FROM deduped)
)
INSERT INTO silver.courier (
    courier_id,
    courier_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    courier_id,
    courier_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
