WITH cleaned AS (
    SELECT
        plan_id,
        NULLIF(TRIM(plan_name), '') AS plan_name,

        CASE
            WHEN monthly_fee >= 0 THEN monthly_fee
            ELSE NULL
        END AS monthly_fee,

        NULLIF(INITCAP(TRIM(video_quality)), '') AS video_quality,

        CASE
            WHEN max_devices >= 0 THEN max_devices
            ELSE NULL
        END AS max_devices,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY plan_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.subscription_plan
    WHERE plan_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned WHERE rn = 1
),
deleted AS (
    DELETE FROM silver.subscription_plan
    WHERE plan_id IN (SELECT plan_id FROM deduped)
)
INSERT INTO silver.subscription_plan (
    plan_id,
    plan_name,
    monthly_fee,
    video_quality,
    max_devices,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    plan_id,
    plan_name,
    monthly_fee,
    video_quality,
    max_devices,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
