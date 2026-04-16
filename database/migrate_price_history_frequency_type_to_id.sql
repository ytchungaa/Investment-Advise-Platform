BEGIN;

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

DROP VIEW IF EXISTS dwd.fact_price_daily;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'ods'
          AND table_name = 'price_history'
          AND column_name = 'frequency_type'
          AND data_type IN ('text', 'character varying')
    ) THEN
        ALTER TABLE ods.price_history
        ADD COLUMN IF NOT EXISTS frequency_type_id SMALLINT;

        UPDATE ods.price_history
        SET frequency_type_id = CASE frequency_type
            WHEN 'minute' THEN 1
            WHEN 'daily' THEN 2
            WHEN 'weekly' THEN 3
            WHEN 'monthly' THEN 4
            ELSE NULL
        END
        WHERE frequency_type_id IS NULL;

        IF EXISTS (
            SELECT 1
            FROM ods.price_history
            WHERE frequency_type_id IS NULL
        ) THEN
            RAISE EXCEPTION 'price_history contains unsupported frequency_type values';
        END IF;

        ALTER TABLE ods.price_history
        ALTER COLUMN frequency_type_id SET NOT NULL;

        ALTER TABLE ods.price_history
        DROP CONSTRAINT IF EXISTS price_history_pkey;

        ALTER TABLE ods.price_history
        DROP CONSTRAINT IF EXISTS price_history_frequency_type_check;

        ALTER TABLE ods.price_history
        DROP COLUMN frequency_type;

        ALTER TABLE ods.price_history
        RENAME COLUMN frequency_type_id TO frequency_type;
    END IF;
END $$;

ALTER TABLE ods.price_history
DROP CONSTRAINT IF EXISTS price_history_frequency_type_fkey;

ALTER TABLE ods.price_history
ADD CONSTRAINT price_history_frequency_type_fkey
FOREIGN KEY (frequency_type)
REFERENCES ods.price_history_frequency_type(id);

ALTER TABLE ods.price_history
DROP CONSTRAINT IF EXISTS price_history_pkey;

ALTER TABLE ods.price_history
ADD CONSTRAINT price_history_pkey
PRIMARY KEY (instrument_id, frequency_type, frequency, candle_time);

COMMIT;
