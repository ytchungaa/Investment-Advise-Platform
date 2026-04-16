BEGIN;

DROP VIEW IF EXISTS dwd.fact_price_daily;

DROP INDEX IF EXISTS ods.idx_instrument_symbol;

ALTER TABLE IF EXISTS ods.instrument
DROP COLUMN IF EXISTS source_payload;

ALTER TABLE IF EXISTS ods.instrument_fundamental_history
DROP COLUMN IF EXISTS source_payload;

ALTER TABLE IF EXISTS ods.quote_history
DROP COLUMN IF EXISTS source_payload;

ALTER TABLE IF EXISTS ods.price_history
DROP COLUMN IF EXISTS source_payload;

ALTER TABLE IF EXISTS ods.price_history
ALTER COLUMN frequency TYPE SMALLINT USING frequency::SMALLINT;

ALTER TABLE IF EXISTS ods.orders
DROP COLUMN IF EXISTS account_number;

COMMIT;
