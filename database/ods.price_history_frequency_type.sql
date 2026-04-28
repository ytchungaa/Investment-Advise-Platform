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