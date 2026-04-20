BEGIN;

-- 1) Inspect duplicate natural keys in ods.price_history.
SELECT
    instrument_id,
    frequency_type,
    frequency,
    candle_time,
    COUNT(*) AS duplicate_count
FROM ods.price_history
GROUP BY instrument_id, frequency_type, frequency, candle_time
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, instrument_id, candle_time;

-- 2) Delete duplicate rows while keeping one row per natural key.
WITH ranked_rows AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY instrument_id, frequency_type, frequency, candle_time
            ORDER BY ctid
        ) AS row_num
    FROM ods.price_history
),
deleted_rows AS (
    DELETE FROM ods.price_history AS ph
    USING ranked_rows AS rr
    WHERE ph.ctid = rr.ctid
      AND rr.row_num > 1
    RETURNING ph.instrument_id, ph.frequency_type, ph.frequency, ph.candle_time
)
SELECT COUNT(*) AS deleted_duplicate_rows
FROM deleted_rows;

-- 3) Verify no duplicates remain.
SELECT
    instrument_id,
    frequency_type,
    frequency,
    candle_time,
    COUNT(*) AS duplicate_count
FROM ods.price_history
GROUP BY instrument_id, frequency_type, frequency, candle_time
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, instrument_id, candle_time;

COMMIT;
