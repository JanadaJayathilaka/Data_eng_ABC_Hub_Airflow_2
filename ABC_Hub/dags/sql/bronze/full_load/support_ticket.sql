SELECT
    *,
    CURRENT_TIMESTAMP AS load_timestamp,
    'ABC_Hub' AS source_system
FROM public.support_ticket;
