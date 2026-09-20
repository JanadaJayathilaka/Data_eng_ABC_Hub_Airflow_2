-- ============================================================
-- SILVER INCREMENTAL: CUSTOMER
-- Pattern: Watermark Filter -> Clean & Deduplicate -> Atomic Upsert
-- ============================================================

WITH cleaned AS (
    SELECT
        customer_id,
        NULLIF(TRIM(customer_no), '') AS customer_no,
        NULLIF(INITCAP(TRIM(first_name)), '') AS first_name,
        NULLIF(INITCAP(TRIM(last_name)), '') AS last_name,
        NULLIF(LOWER(TRIM(email)), '') AS email,
        NULLIF(TRIM(phone), '') AS phone,
        date_of_birth,

        CASE
            WHEN gender IS NULL OR TRIM(gender) = '' THEN NULL
            ELSE INITCAP(TRIM(gender))
        END AS gender,

        registration_date,

        CASE
            WHEN status IS NULL OR TRIM(status) = '' THEN NULL
            ELSE UPPER(TRIM(status))
        END AS status,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.customer
    WHERE customer_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),

deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),

-- Remove existing records that have an update in this batch
deleted AS (
    DELETE FROM silver.customer
    WHERE customer_id IN (SELECT customer_id FROM deduped)
)

-- Insert clean, updated records
INSERT INTO silver.customer (
    customer_id,
    customer_no,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    registration_date,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    customer_id,
    customer_no,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    registration_date,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
