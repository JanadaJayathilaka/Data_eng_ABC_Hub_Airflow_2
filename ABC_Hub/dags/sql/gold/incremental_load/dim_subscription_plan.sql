-- ============================================================
-- GOLD INCREMENTAL: DIM_SUBSCRIPTION_PLAN (SCD Type 2)
-- ============================================================

WITH new_or_updated AS (
    SELECT
        plan_id,
        plan_name,
        monthly_fee,
        video_quality,
        max_devices,
        updated_at
    FROM silver.subscription_plan
    WHERE created_at > '{{ last_watermark }}'
       OR updated_at > '{{ last_watermark }}'
),

close_existing AS (
    UPDATE gold.dim_subscription_plan d
    SET
        effective_to = (n.updated_at::DATE - INTERVAL '1 day')::DATE,
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP
    FROM new_or_updated n
    WHERE d.plan_id = n.plan_id
      AND d.is_current = TRUE
      AND (
          d.plan_name IS DISTINCT FROM n.plan_name OR
          d.monthly_fee IS DISTINCT FROM n.monthly_fee OR
          d.video_quality IS DISTINCT FROM n.video_quality OR
          d.max_devices IS DISTINCT FROM n.max_devices
      )
)

INSERT INTO gold.dim_subscription_plan (
    plan_id,
    plan_name,
    monthly_fee,
    video_quality,
    max_devices,
    effective_from,
    effective_to,
    created_at,
    updated_at,
    is_current
)
SELECT
    n.plan_id,
    n.plan_name,
    n.monthly_fee,
    n.video_quality,
    n.max_devices,
    n.updated_at::DATE AS effective_from,
    '9999-12-31'::DATE AS effective_to,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    TRUE
FROM new_or_updated n
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_subscription_plan d
    WHERE d.plan_id = n.plan_id
      AND d.is_current = TRUE
      AND d.plan_name IS NOT DISTINCT FROM n.plan_name
      AND d.monthly_fee IS NOT DISTINCT FROM n.monthly_fee
      AND d.video_quality IS NOT DISTINCT FROM n.video_quality
      AND d.max_devices IS NOT DISTINCT FROM n.max_devices
);
