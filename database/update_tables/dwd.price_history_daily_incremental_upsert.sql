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
\set refresh_lookback_days '10'
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
        GREATEST(COALESCE(NULLIF(:'refresh_lookback_days', '')::INTEGER, 10), 1)
            AS refresh_lookback_days,
        COALESCE(NULLIF(:'bucket_timezone', ''), 'America/New_York')::TEXT AS bucket_timezone
),
source_bounds AS (
    SELECT
        MIN(ph.candle_time) AS source_min_time,
        MAX(ph.candle_time) AS source_max_time
    FROM ods.price_history ph
    JOIN ods.price_history_frequency_type pft
        ON pft.id = ph.frequency_type
    WHERE (
            pft.code = 'daily'
            AND ph.frequency = 1
        )
       OR (
            pft.code = 'minute'
            AND ph.frequency IN (1, 5, 10, 15, 30)
        )
),
refresh_bounds AS (
    SELECT
        (SELECT MIN(bucket_start) FROM dwd.price_history_daily) AS dwd_min_time,
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
        date_trunc('day', start_time AT TIME ZONE bucket_timezone) AS start_bucket_local,
        date_trunc('day', end_time AT TIME ZONE bucket_timezone) AS end_bucket_local,
        bucket_timezone
    FROM refresh_window
    WHERE start_time IS NOT NULL
      AND end_time IS NOT NULL
      AND start_time <= end_time
),
deleted_rows AS (
    DELETE FROM dwd.price_history_daily d
    USING bucket_limits bl
    WHERE d.bucket_start >= bl.start_bucket_local AT TIME ZONE bl.bucket_timezone
      AND d.bucket_start <= bl.end_bucket_local AT TIME ZONE bl.bucket_timezone
    RETURNING d.instrument_id, d.bucket_start
),
minute_source_rows AS (
    SELECT
        ph.instrument_id,
        ph.frequency,
        date_trunc('day', ph.candle_time AT TIME ZONE rw.bucket_timezone)
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
      AND ph.frequency IN (1, 5, 10, 15, 30)
      AND ph.candle_time >= date_trunc('day', rw.start_time AT TIME ZONE rw.bucket_timezone)
          AT TIME ZONE rw.bucket_timezone
      AND ph.candle_time < rw.end_time
),
minute_frequency_bucket_rows AS (
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
    FROM minute_source_rows
    GROUP BY instrument_id, bucket_start, frequency
),
minute_bucket_rows AS (
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
        source_max_candle_time,
        1 AS priority
    FROM minute_frequency_bucket_rows
    WHERE close IS NOT NULL
    ORDER BY instrument_id, bucket_start, frequency ASC
),
daily_bucket_rows AS (
    SELECT
        ph.instrument_id,
        date_trunc('day', ph.candle_time AT TIME ZONE rw.bucket_timezone)
            AT TIME ZONE rw.bucket_timezone AS bucket_start,
        ph.open,
        ph.high,
        ph.low,
        ph.close,
        ph.close AS avg_price,
        ph.volume,
        ph.previous_close,
        ph.previous_close_time,
        1::INTEGER AS source_candle_count,
        ph.candle_time AS source_min_candle_time,
        ph.candle_time AS source_max_candle_time,
        2 AS priority
    FROM ods.price_history ph
    JOIN ods.price_history_frequency_type pft
        ON pft.id = ph.frequency_type
    CROSS JOIN refresh_window rw
    WHERE pft.code = 'daily'
      AND ph.frequency = 1
      AND ph.close IS NOT NULL
      AND ph.candle_time >= date_trunc('day', rw.start_time AT TIME ZONE rw.bucket_timezone)
          AT TIME ZONE rw.bucket_timezone
      AND ph.candle_time < rw.end_time
),
bucket_values AS (
    SELECT * FROM minute_bucket_rows
    UNION ALL
    SELECT * FROM daily_bucket_rows
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
    FROM bucket_values
    ORDER BY instrument_id, bucket_start, priority
)
INSERT INTO dwd.price_history_daily (
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
