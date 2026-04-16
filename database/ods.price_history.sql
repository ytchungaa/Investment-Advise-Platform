CREATE TABLE IF NOT EXISTS ods.price_history_frequency_type (
    id SMALLINT PRIMARY KEY,
    code TEXT NOT NULL UNIQUE
);

INSERT INTO ods.price_history_frequency_type (id, code)
VALUES
    (1, 'minute'),
    (2, 'daily'),
    (3, 'weekly'),
    (4, 'monthly')
ON CONFLICT (id) DO UPDATE
SET code = EXCLUDED.code;

CREATE TABLE IF NOT EXISTS ods.price_history (
    instrument_id BIGINT NOT NULL REFERENCES ods.instrument(id) ON DELETE CASCADE,
    frequency_type SMALLINT NOT NULL REFERENCES ods.price_history_frequency_type(id),
    frequency SMALLINT NOT NULL,
    candle_time TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    previous_close DOUBLE PRECISION,
    previous_close_time TIMESTAMPTZ,
    need_extended_hours_data BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (instrument_id, frequency_type, frequency, candle_time)
);

CREATE INDEX IF NOT EXISTS idx_price_history_instrument_time
ON ods.price_history (instrument_id, candle_time DESC);
