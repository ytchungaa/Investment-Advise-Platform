DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'account_type_enum') THEN
        CREATE TYPE account_type_enum AS ENUM ('CASH', 'MARGIN');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ods.securities_account (
    id BIGSERIAL PRIMARY KEY,
    account_number TEXT UNIQUE NOT NULL,
    hash_value TEXT NOT NULL,
    account_type account_type_enum NOT NULL,
    round_trips INT,
    is_day_trader BOOLEAN NOT NULL DEFAULT FALSE,
    is_closing_only_restricted BOOLEAN NOT NULL DEFAULT FALSE,
    pfcb_flag BOOLEAN NOT NULL DEFAULT FALSE
);

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
    PRIMARY KEY (as_of_time, account_id, instrument_id)
);

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
    PRIMARY KEY (as_of_time, account_id)
);

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
    PRIMARY KEY (as_of_time, account_id)
);

CREATE TABLE IF NOT EXISTS ods.account_projected_balances (
    as_of_time TIMESTAMPTZ NOT NULL,
    account_id BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
    cash_available_for_trading NUMERIC,
    cash_available_for_withdrawal NUMERIC,
    PRIMARY KEY (as_of_time, account_id)
);

CREATE TABLE IF NOT EXISTS ods.aggregated_balance (
    as_of_time TIMESTAMPTZ PRIMARY KEY,
    current_liquidation_value NUMERIC,
    liquidation_value NUMERIC
);
