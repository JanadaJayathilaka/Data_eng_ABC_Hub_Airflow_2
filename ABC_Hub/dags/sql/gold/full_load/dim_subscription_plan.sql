TRUNCATE TABLE gold.dim_subscription_plan RESTART IDENTITY CASCADE;


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
)SELECT
    plan_id,
    plan_name,
    monthly_fee,
    video_quality,
    max_devices,
    COALESCE(created_at::DATE, '2020-01-01'::DATE) AS effective_from,
    '9999-12-31'::DATE AS effective_to,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
    TRUE AS is_current
FROM silver.subscription_plan;
