WITH cleaned AS (
    SELECT
        wishlist_id,
        customer_id,
        content_id,
        added_date,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY wishlist_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.wishlist
    WHERE wishlist_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.wishlist
    WHERE wishlist_id IN (SELECT wishlist_id FROM deduped)
)
INSERT INTO silver.wishlist (
    wishlist_id,
    customer_id,
    content_id,
    added_date,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    wishlist_id,
    customer_id,
    content_id,
    added_date,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
