CREATE TABLE IF NOT EXISTS dwd.price_history_hourly (
    instrument_id BIGINT NOT NULL REFERENCES ods.instrument(id) ON DELETE CASCADE,
    date_time TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    avg_price DOUBLE PRECISION,
    volume BIGINT,
    previous_close DOUBLE PRECISION,
    previous_close_time TIMESTAMPTZ,
    PRIMARY KEY (instrument_id, date_time)
);

CREATE INDEX IF NOT EXISTS idx_price_history_instrument_time
ON dwd.price_history_hourly (instrument_id, date_time DESC);
