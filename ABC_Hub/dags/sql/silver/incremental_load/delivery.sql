WITH cleaned AS (
    SELECT
        delivery_id,
        rental_id,
        courier_id,
        dispatch_date,
        delivered_date,
        returned_date,
        NULLIF(UPPER(TRIM(delivery_status)), '') AS delivery_status,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY delivery_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.delivery
    WHERE delivery_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.delivery
    WHERE delivery_id IN (SELECT delivery_id FROM deduped)
)
INSERT INTO silver.delivery (
    delivery_id,
    rental_id,
    courier_id,
    dispatch_date,
    delivered_date,
    returned_date,
    delivery_status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    delivery_id,
    rental_id,
    courier_id,
    dispatch_date,
    delivered_date,
    returned_date,
    delivery_status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
