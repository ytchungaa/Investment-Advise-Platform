-- ===== Enums
CREATE TYPE account_type_enum AS ENUM ('CASH','MARGIN');
CREATE TYPE asset_type_enum AS ENUM (
  'EQUITY','MUTUAL_FUND','OPTION','FUTURE','FOREX','INDEX',
  'CASH_EQUIVALENT','FIXED_INCOME','PRODUCT','CURRENCY','COLLECTIVE_INVESTMENT'
);

-- ===== Snapshots (track when you pulled the data)
CREATE TABLE ods.snapshot (
  id BIGSERIAL PRIMARY KEY,
  date_time TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ===== Accounts
CREATE TABLE ods.securities_account (
  id BIGSERIAL PRIMARY KEY,
  account_number TEXT UNIQUE NOT NULL,
  hash_value TEXT NOT NULL,
  account_type account_type_enum NOT NULL,
  round_trips INT,
  is_day_trader BOOLEAN NOT NULL DEFAULT FALSE,
  is_closing_only_restricted BOOLEAN NOT NULL DEFAULT FALSE,
  pfcb_flag BOOLEAN NOT NULL DEFAULT FALSE
);

-- ===== Instruments (covers equity/ETF/fixed-income from sample)
CREATE TABLE ods.instrument (
  id BIGSERIAL PRIMARY KEY,
  asset_type asset_type_enum NOT NULL,
  symbol TEXT,           -- e.g. AAPL, VOO, 912797QP5
  cusip TEXT,            -- when present
  description TEXT,      -- ETF/fixed income often has this
  etf_type TEXT,         -- e.g. 'EXCHANGE_TRADED_FUND' when asset_type='COLLECTIVE_INVESTMENT'
  maturity_date TIMESTAMPTZ, -- for FIXED_INCOME
  variable_rate DOUBLE PRECISION, -- for FIXED_INCOME
  UNIQUE (symbol, asset_type) -- symbol+asset_type is unique
);

-- ===== Positions (one table; per account x instrument x snapshot)
CREATE TABLE ods.position (
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  account_id  BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
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
  PRIMARY KEY (created_at, account_id, instrument_id)
);

-- ===== Balances
-- Initial (has extra fields in sample)
CREATE TABLE ods.account_initial_balances (
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  account_id  BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
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
  PRIMARY KEY (created_at, account_id)
);

-- Current (matches your sample keys)
CREATE TABLE ods.account_current_balances (
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  account_id  BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
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
  PRIMARY KEY (created_at, account_id)
);

-- Projected (sample only shows the two fields)
CREATE TABLE ods.account_projected_balances (
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  account_id  BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
  cash_available_for_trading NUMERIC,
  cash_available_for_withdrawal NUMERIC,
  PRIMARY KEY (created_at, account_id)
);

-- Aggregated balance block in sample
CREATE TABLE ods.aggregated_balance (
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  current_liquidation_value NUMERIC,
  liquidation_value NUMERIC
);
