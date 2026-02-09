
CREATE TABLE ods.price_history (
    symbol TEXT NOT NULL,
    datetime BIGINT NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT
);

CREATE INDEX idx_price_history_symbol_datetime
ON ods.price_history (symbol, datetime);

DROP TABLE IF EXISTS ods.price_history CASCADE;
