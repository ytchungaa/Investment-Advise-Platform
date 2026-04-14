CREATE TABLE IF NOT EXISTS ods.price_history (
    instrument_id BIGINT NOT NULL REFERENCES ods.instrument(id) ON DELETE CASCADE,
    frequency_type TEXT NOT NULL CHECK (frequency_type IN ('minute', 'daily', 'weekly', 'monthly')),
    frequency INTEGER NOT NULL,
    candle_time TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    previous_close DOUBLE PRECISION,
    previous_close_time TIMESTAMPTZ,
    need_extended_hours_data BOOLEAN NOT NULL DEFAULT TRUE,
    source_payload JSONB,
    PRIMARY KEY (instrument_id, frequency_type, frequency, candle_time)
);

CREATE INDEX IF NOT EXISTS idx_price_history_instrument_time
ON ods.price_history (instrument_id, candle_time DESC);
