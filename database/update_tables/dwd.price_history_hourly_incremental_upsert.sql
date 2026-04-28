BEGIN;

WITH refresh_params AS (
    SELECT
        NULL::TIMESTAMPTZ AS start_time,
        NULL::TIMESTAMPTZ AS end_time,
        'America/New_York'::TEXT AS bucket_timezone
),
refresh_window AS (
    SELECT
        COALESCE(
            start_time,
            (
                SELECT MAX(bucket_start) - INTERVAL '1 hour'
                FROM dwd.price_history_hourly
            ),
            (
                SELECT MIN(ph.candle_time)
                FROM ods.price_history ph
                JOIN ods.price_history_frequency_type pft
                    ON pft.id = ph.frequency_type
                WHERE pft.code = 'minute'
                  AND ph.frequency = 1
            )
        ) AS start_time,
        COALESCE(end_time, NOW()) AS end_time,
        bucket_timezone
    FROM refresh_params
),
source_rows AS (
    SELECT
        ph.instrument_id,
        date_trunc('hour', ph.candle_time AT TIME ZONE rw.bucket_timezone)
            AT TIME ZONE rw.bucket_timezone AS bucket_start,
        ph.candle_time,
        ph.open,
        ph.high,
        ph.low,
        ph.close,
        ph.volume,
        ph.previous_close,
        ph.previous_close_time
    FROM ods.price_history ph
    JOIN ods.price_history_frequency_type pft
        ON pft.id = ph.frequency_type
    CROSS JOIN refresh_window rw
    WHERE pft.code = 'minute'
      AND ph.frequency = 1
      AND ph.candle_time >= date_trunc('hour', rw.start_time AT TIME ZONE rw.bucket_timezone)
          AT TIME ZONE rw.bucket_timezone
      AND ph.candle_time < rw.end_time
),
aggregated_rows AS (
    SELECT
        instrument_id,
        bucket_start,
        (ARRAY_AGG(open ORDER BY candle_time ASC))[1] AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        (ARRAY_AGG(close ORDER BY candle_time DESC))[1] AS close,
        AVG(close) AS avg_price,
        SUM(volume)::BIGINT AS volume,
        (ARRAY_AGG(previous_close ORDER BY candle_time DESC))[1] AS previous_close,
        (ARRAY_AGG(previous_close_time ORDER BY candle_time DESC))[1] AS previous_close_time,
        COUNT(*)::INTEGER AS source_candle_count,
        MIN(candle_time) AS source_min_candle_time,
        MAX(candle_time) AS source_max_candle_time,
        NOW() AS refreshed_at
    FROM source_rows
    GROUP BY instrument_id, bucket_start
)
INSERT INTO dwd.price_history_hourly (
    instrument_id,
    bucket_start,
    open,
    high,
    low,
    close,
    avg_price,
    volume,
    previous_close,
    previous_close_time,
    source_candle_count,
    source_min_candle_time,
    source_max_candle_time,
    refreshed_at
)
SELECT
    instrument_id,
    bucket_start,
    open,
    high,
    low,
    close,
    avg_price,
    volume,
    previous_close,
    previous_close_time,
    source_candle_count,
    source_min_candle_time,
    source_max_candle_time,
    refreshed_at
FROM aggregated_rows
ON CONFLICT (instrument_id, bucket_start) DO UPDATE
SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    avg_price = EXCLUDED.avg_price,
    volume = EXCLUDED.volume,
    previous_close = EXCLUDED.previous_close,
    previous_close_time = EXCLUDED.previous_close_time,
    source_candle_count = EXCLUDED.source_candle_count,
    source_min_candle_time = EXCLUDED.source_min_candle_time,
    source_max_candle_time = EXCLUDED.source_max_candle_time,
    refreshed_at = EXCLUDED.refreshed_at;

COMMIT;
