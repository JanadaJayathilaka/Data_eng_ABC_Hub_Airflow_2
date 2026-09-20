SELECT
    *,
    CURRENT_TIMESTAMP AS load_timestamp,
    'ABC_Hub' AS source_system
FROM public.artist
WHERE created_at > '{{ last_watermark }}'
   OR updated_at > '{{ last_watermark }}';
