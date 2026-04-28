CREATE TABLE IF NOT EXISTS dwd.price_history_hourly (
    instrument_id BIGINT NOT NULL REFERENCES ods.instrument(id) ON DELETE CASCADE,
    bucket_start TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    avg_price DOUBLE PRECISION,
    volume BIGINT,
    previous_close DOUBLE PRECISION,
    previous_close_time TIMESTAMPTZ,
    source_candle_count INTEGER NOT NULL DEFAULT 0,
    source_min_candle_time TIMESTAMPTZ,
    source_max_candle_time TIMESTAMPTZ,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (instrument_id, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_price_history_hourly_bucket_start
ON dwd.price_history_hourly (bucket_start DESC);
