WITH cleaned AS (
    SELECT
        ticket_id,
        customer_id,
        category_id,
        opened_date,
        closed_date,
        NULLIF(UPPER(TRIM(priority)), '') AS priority,
        NULLIF(UPPER(TRIM(status)), '') AS status,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY ticket_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.support_ticket
    WHERE ticket_id IS NOT NULL
)

INSERT INTO silver.support_ticket (
    ticket_id,
    customer_id,
    category_id,
    opened_date,
    closed_date,
    priority,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    ticket_id,
    customer_id,
    category_id,
    opened_date,
    closed_date,
    priority,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1
  AND (
      closed_date IS NULL
      OR opened_date IS NULL
      OR opened_date <= closed_date
  );