WITH cleaned AS (
    SELECT
        subscription_id,
        customer_id,
        plan_id,
        start_date,
        end_date,

        CASE
            WHEN status IS NULL OR TRIM(status) = '' THEN NULL
            ELSE UPPER(TRIM(status))
        END AS status,

        auto_renew,
        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY subscription_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.customer_subscription
    WHERE subscription_id IS NOT NULL
)

INSERT INTO silver.customer_subscription (
    subscription_id,
    customer_id,
    plan_id,
    start_date,
    end_date,
    status,
    auto_renew,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    subscription_id,
    customer_id,
    plan_id,
    start_date,
    end_date,
    status,
    auto_renew,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM cleaned
WHERE rn = 1
  AND (
      end_date IS NULL
      OR start_date IS NULL
      OR start_date <= end_date
  );