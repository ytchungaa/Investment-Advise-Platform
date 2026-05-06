DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'account_type_enum') THEN
        CREATE TYPE account_type_enum AS ENUM ('CASH', 'MARGIN');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ods.load_audit (
    id BIGSERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    request_params JSONB,
    status TEXT NOT NULL DEFAULT 'started',
    notes TEXT
);

CREATE TABLE IF NOT EXISTS ods.securities_account (
    id BIGSERIAL PRIMARY KEY,
    account_number TEXT UNIQUE NOT NULL,
    hash_value TEXT NOT NULL,
    account_type account_type_enum NOT NULL,
    round_trips INT,
    is_day_trader BOOLEAN NOT NULL DEFAULT FALSE,
    is_closing_only_restricted BOOLEAN NOT NULL DEFAULT FALSE,
    pfcb_flag BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS ods.securities_account
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS ods.position (
    as_of_time TIMESTAMPTZ NOT NULL,
    account_id BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
    instrument_id BIGINT NOT NULL REFERENCES ods.instrument(id),
    long_quantity DOUBLE PRECISION,
    short_quantity DOUBLE PRECISION,
    average_price DOUBLE PRECISION,
    average_long_price DOUBLE PRECISION,
    taxlot_average_long_price DOUBLE PRECISION,
    current_day_profit_loss DOUBLE PRECISION,
    current_day_profit_loss_percentage DOUBLE PRECISION,
    market_value DOUBLE PRECISION,
    maintenance_requirement DOUBLE PRECISION,
    long_open_profit_loss DOUBLE PRECISION,
    previous_session_long_quantity DOUBLE PRECISION,
    current_day_cost DOUBLE PRECISION,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (as_of_time, account_id, instrument_id)
);

ALTER TABLE IF EXISTS ods.position
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_position_account_as_of_time
ON ods.position (account_id, as_of_time DESC);

CREATE TABLE IF NOT EXISTS ods.account_initial_balances (
    as_of_time TIMESTAMPTZ NOT NULL,
    account_id BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
    accrued_interest NUMERIC,
    cash_available_for_trading NUMERIC,
    cash_available_for_withdrawal NUMERIC,
    cash_balance NUMERIC,
    bond_value NUMERIC,
    cash_receipts NUMERIC,
    liquidation_value NUMERIC,
    long_option_market_value NUMERIC,
    long_stock_value NUMERIC,
    money_market_fund NUMERIC,
    mutual_fund_value NUMERIC,
    short_option_market_value NUMERIC,
    short_stock_value NUMERIC,
    is_in_call BOOLEAN,
    unsettled_cash NUMERIC,
    cash_debit_call_value NUMERIC,
    pending_deposits NUMERIC,
    account_value NUMERIC,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (as_of_time, account_id)
);

ALTER TABLE IF EXISTS ods.account_initial_balances
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS ods.account_current_balances (
    as_of_time TIMESTAMPTZ NOT NULL,
    account_id BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
    accrued_interest NUMERIC,
    cash_balance NUMERIC,
    cash_receipts NUMERIC,
    long_option_market_value NUMERIC,
    liquidation_value NUMERIC,
    long_market_value NUMERIC,
    money_market_fund NUMERIC,
    savings NUMERIC,
    short_market_value NUMERIC,
    pending_deposits NUMERIC,
    mutual_fund_value NUMERIC,
    bond_value NUMERIC,
    short_option_market_value NUMERIC,
    cash_available_for_trading NUMERIC,
    cash_available_for_withdrawal NUMERIC,
    cash_call NUMERIC,
    long_non_marginable_market_value NUMERIC,
    total_cash NUMERIC,
    cash_debit_call_value NUMERIC,
    unsettled_cash NUMERIC,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (as_of_time, account_id)
);

ALTER TABLE IF EXISTS ods.account_current_balances
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS ods.account_projected_balances (
    as_of_time TIMESTAMPTZ NOT NULL,
    account_id BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
    cash_available_for_trading NUMERIC,
    cash_available_for_withdrawal NUMERIC,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (as_of_time, account_id)
);

ALTER TABLE IF EXISTS ods.account_projected_balances
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS ods.aggregated_balance (
    as_of_time TIMESTAMPTZ NOT NULL,
    account_id BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
    current_liquidation_value NUMERIC,
    liquidation_value NUMERIC,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS ods.aggregated_balance
    ADD COLUMN IF NOT EXISTS account_id BIGINT REFERENCES ods.securities_account(id) ON DELETE CASCADE,
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE IF EXISTS ods.aggregated_balance
    DROP CONSTRAINT IF EXISTS aggregated_balance_pkey;

CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregated_balance_account_time
ON ods.aggregated_balance (as_of_time, account_id)
WHERE account_id IS NOT NULL;

CREATE OR REPLACE VIEW ods.v_position_latest AS
SELECT DISTINCT ON (account_id, instrument_id)
    *
FROM ods.position
ORDER BY account_id, instrument_id, as_of_time DESC;

CREATE OR REPLACE VIEW ods.v_account_initial_balances_latest AS
SELECT DISTINCT ON (account_id)
    *
FROM ods.account_initial_balances
ORDER BY account_id, as_of_time DESC;

CREATE OR REPLACE VIEW ods.v_account_current_balances_latest AS
SELECT DISTINCT ON (account_id)
    *
FROM ods.account_current_balances
ORDER BY account_id, as_of_time DESC;

CREATE OR REPLACE VIEW ods.v_account_projected_balances_latest AS
SELECT DISTINCT ON (account_id)
    *
FROM ods.account_projected_balances
ORDER BY account_id, as_of_time DESC;
