CREATE INDEX IF NOT EXISTS idx_price_history_frequency_time
ON ods.price_history (frequency_type, frequency, candle_time DESC, instrument_id);
