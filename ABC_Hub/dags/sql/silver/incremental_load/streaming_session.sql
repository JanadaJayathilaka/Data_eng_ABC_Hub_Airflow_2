WITH cleaned AS (
    SELECT
        stream_id,
        customer_id,
        content_id,
        device_id,
        start_time,
        end_time,

        CASE
            WHEN watch_duration >= 0 THEN watch_duration
            ELSE NULL
        END AS watch_duration,

        CASE
            WHEN completion_percentage BETWEEN 0 AND 100
            THEN completion_percentage
            ELSE NULL
        END AS completion_percentage,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER (
            PARTITION BY stream_id
            ORDER BY updated_at DESC NULLS LAST,
                     load_timestamp DESC
        ) AS rn

    FROM bronze.streaming_session
    WHERE stream_id IS NOT NULL
      AND (
          created_at > '{{ last_watermark }}'
          OR updated_at > '{{ last_watermark }}'
      )
),
deduped AS (
    SELECT * FROM cleaned
    WHERE rn = 1
      AND (
          end_time IS NULL
          OR start_time IS NULL
          OR start_time <= end_time
      )
),
deleted AS (
    DELETE FROM silver.streaming_session
    WHERE stream_id IN (SELECT stream_id FROM deduped)
)
INSERT INTO silver.streaming_session (
    stream_id,
    customer_id,
    content_id,
    device_id,
    start_time,
    end_time,
    watch_duration,
    completion_percentage,
    created_at,
    updated_at,
    load_timestamp,
    source_system
)
SELECT
    stream_id,
    customer_id,
    content_id,
    device_id,
    start_time,
    end_time,
    watch_duration,
    completion_percentage,
    created_at,
    updated_at,
    load_timestamp,
    source_system
FROM deduped;
