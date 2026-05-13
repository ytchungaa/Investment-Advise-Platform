\if :{?refresh_start_time}
\else
\set refresh_start_time ''
\endif
\if :{?refresh_end_time}
\else
\set refresh_end_time ''
\endif
\if :{?refresh_lookback_days}
\else
\set refresh_lookback_days '1'
\endif
\if :{?bucket_timezone}
\else
\set bucket_timezone 'America/New_York'
\endif

BEGIN;

WITH refresh_params AS (
    SELECT
        NULLIF(:'refresh_start_time', '')::TIMESTAMPTZ AS requested_start_time,
        NULLIF(:'refresh_end_time', '')::TIMESTAMPTZ AS requested_end_time,
        GREATEST(COALESCE(NULLIF(:'refresh_lookback_days', '')::INTEGER, 1), 1)
            AS refresh_lookback_days,
        COALESCE(NULLIF(:'bucket_timezone', ''), 'America/New_York')::TEXT AS bucket_timezone
),
source_bounds AS (
    SELECT
        (
            SELECT ph.candle_time
            FROM ods.price_history ph
            WHERE ph.frequency_type = 1
              AND ph.frequency IN (1, 5, 10, 15, 30)
            ORDER BY ph.candle_time ASC
            LIMIT 1
        ) AS source_min_time,
        (
            SELECT ph.candle_time
            FROM ods.price_history ph
            WHERE ph.frequency_type = 1
              AND ph.frequency IN (1, 5, 10, 15, 30)
            ORDER BY ph.candle_time DESC
            LIMIT 1
        ) AS source_max_time
),
refresh_bounds AS (
    SELECT
        (SELECT MIN(bucket_start) FROM dwd.price_history_hourly) AS dwd_min_time,
        source_min_time,
        source_max_time
    FROM source_bounds
),
refresh_window AS (
    SELECT
        COALESCE(
            rp.requested_start_time,
            CASE
                WHEN rb.dwd_min_time IS NULL THEN rb.source_min_time
                ELSE GREATEST(
                    rb.source_min_time,
                    COALESCE(
                        rp.requested_end_time,
                        rb.source_max_time + INTERVAL '1 microsecond'
                    ) - (rp.refresh_lookback_days * INTERVAL '1 day')
                )
            END
        ) AS start_time,
        COALESCE(
            rp.requested_end_time,
            rb.source_max_time + INTERVAL '1 microsecond'
        ) AS end_time,
        rp.bucket_timezone
    FROM refresh_params rp
    CROSS JOIN refresh_bounds rb
),
bucket_limits AS (
    SELECT
        date_trunc('hour', start_time AT TIME ZONE bucket_timezone) AS start_bucket_local,
        date_trunc('hour', end_time AT TIME ZONE bucket_timezone) AS end_bucket_local,
        bucket_timezone
    FROM refresh_window
    WHERE start_time IS NOT NULL
      AND end_time IS NOT NULL
      AND start_time <= end_time
),
deleted_rows AS (
    DELETE FROM dwd.price_history_hourly d
    USING bucket_limits bl
    WHERE d.bucket_start >= bl.start_bucket_local AT TIME ZONE bl.bucket_timezone
      AND d.bucket_start <= bl.end_bucket_local AT TIME ZONE bl.bucket_timezone
    RETURNING d.instrument_id, d.bucket_start
),
source_rows AS (
    SELECT
        ph.instrument_id,
        ph.frequency,
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
    CROSS JOIN refresh_window rw
    WHERE ph.frequency_type = 1
      AND ph.frequency IN (1, 5, 10, 15, 30)
      AND ph.candle_time >= date_trunc('hour', rw.start_time AT TIME ZONE rw.bucket_timezone)
          AT TIME ZONE rw.bucket_timezone
      AND ph.candle_time < rw.end_time
),
frequency_bucket_rows AS (
    SELECT
        instrument_id,
        bucket_start,
        frequency,
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
        MAX(candle_time) AS source_max_candle_time
    FROM source_rows
    GROUP BY instrument_id, bucket_start, frequency
),
selected_bucket_rows AS (
    SELECT DISTINCT ON (instrument_id, bucket_start)
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
        source_max_candle_time
    FROM frequency_bucket_rows
    WHERE close IS NOT NULL
    ORDER BY instrument_id, bucket_start, frequency ASC
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
    COALESCE(avg_price, close) AS avg_price,
    COALESCE(volume, 0) AS volume,
    previous_close,
    previous_close_time,
    source_candle_count,
    source_min_candle_time,
    source_max_candle_time,
    NOW() AS refreshed_at
FROM selected_bucket_rows
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
