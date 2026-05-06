DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_session_enum') THEN
        CREATE TYPE order_session_enum AS ENUM ('NORMAL','AM','PM','SEAMLESS');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_duration_enum') THEN
        CREATE TYPE order_duration_enum AS ENUM ('DAY','GOOD_TILL_CANCEL','FILL_OR_KILL','IMMEDIATE_OR_CANCEL','END_OF_WEEK','END_OF_MONTH','NEXT_END_OF_MONTH','UNKNOWN');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_type_enum') THEN
        CREATE TYPE order_type_enum AS ENUM ('MARKET','LIMIT','STOP','STOP_LIMIT','TRAILING_STOP','CABINET','NON_MARKETABLE','MARKET_ON_CLOSE','EXERCISE','TRAILING_STOP_LIMIT','NET_DEBIT','NET_CREDIT','NET_ZERO','LIMIT_ON_CLOSE','UNKNOWN');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_complex_strategy_enum') THEN
        CREATE TYPE order_complex_strategy_enum AS ENUM ('NONE','COVERED','VERTICAL','BACK_RATIO','CALENDAR','DIAGONAL','STRADDLE','STRANGLE','COLLAR_SYNTHETIC','BUTTERFLY','CONDOR','IRON_CONDOR','VERTICAL_ROLL','COLLAR_WITH_STOCK','DOUBLE_DIAGONAL','UNBALANCED_BUTTERFLY','UNBALANCED_CONDOR','UNBALANCED_IRON_CONDOR','UNBALANCED_VERTICAL_ROLL','MUTUAL_FUND_SWAP','CUSTOM');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_dest_enum') THEN
        CREATE TYPE order_dest_enum AS ENUM ('INET','ECN_ARCA','CBOE','AMEX','PHLX','ISE','BOX','NYSE','NASDAQ','BATS','C2','AUTO');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_strategy_enum') THEN
        CREATE TYPE order_strategy_enum AS ENUM ('SINGLE','CANCEL','RECALL','PAIR','FLATTEN','TWO_DAY_SWAP','BLAST_ALL','OCO','TRIGGER');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_status_enum') THEN
        CREATE TYPE order_status_enum AS ENUM ('AWAITING_PARENT_ORDER','AWAITING_CONDITION','AWAITING_STOP_CONDITION','AWAITING_MANUAL_REVIEW','ACCEPTED','AWAITING_UR_OUT','PENDING_ACTIVATION','QUEUED','WORKING','REJECTED','PENDING_CANCEL','CANCELED','PENDING_REPLACE','REPLACED','FILLED','EXPIRED','NEW','AWAITING_RELEASE_TIME','PENDING_ACKNOWLEDGEMENT','PENDING_RECALL','UNKNOWN');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_leg_type_enum') THEN
        CREATE TYPE order_leg_type_enum AS ENUM ('EQUITY','OPTION','INDEX','MUTUAL_FUND','CASH_EQUIVALENT','FIXED_INCOME','CURRENCY','COLLECTIVE_INVESTMENT');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_instruction_enum') THEN
        CREATE TYPE order_instruction_enum AS ENUM ('BUY','SELL','BUY_TO_COVER','SELL_SHORT','BUY_TO_OPEN','BUY_TO_CLOSE','SELL_TO_OPEN','SELL_TO_CLOSE','EXCHANGE','SELL_SHORT_EXEMPT');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_position_effect_enum') THEN
        CREATE TYPE order_position_effect_enum AS ENUM ('OPENING','CLOSING','AUTOMATIC');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_quantity_type_enum') THEN
        CREATE TYPE order_quantity_type_enum AS ENUM ('ALL_SHARES','DOLLARS','SHARES');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_div_cap_gains_enum') THEN
        CREATE TYPE order_div_cap_gains_enum AS ENUM ('REINVEST','PAYOUT');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_special_instruction_enum') THEN
        CREATE TYPE order_special_instruction_enum AS ENUM ('ALL_OR_NONE','DO_NOT_REDUCE','ALL_OR_NONE_DO_NOT_REDUCE');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_activity_type_enum') THEN
        CREATE TYPE order_activity_type_enum AS ENUM ('EXECUTION','ORDER_ACTION');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_execution_type_enum') THEN
        CREATE TYPE order_execution_type_enum AS ENUM ('FILL');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ods.orders (
    order_id BIGINT PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
    session order_session_enum,
    duration order_duration_enum,
    order_type order_type_enum,
    complex_strategy order_complex_strategy_enum,
    quantity DOUBLE PRECISION,
    filled_quantity DOUBLE PRECISION,
    remaining_quantity DOUBLE PRECISION,
    requested_destination order_dest_enum,
    destination_link_name TEXT,
    price DOUBLE PRECISION,
    tax_lot_method TEXT,
    order_strategy order_strategy_enum,
    cancelable BOOLEAN,
    editable BOOLEAN,
    status order_status_enum,
    entered_time TIMESTAMPTZ,
    close_time TIMESTAMPTZ,
    tag TEXT,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS ods.orders
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS ods.order_leg (
    order_id BIGINT NOT NULL REFERENCES ods.orders(order_id) ON DELETE CASCADE,
    leg_id BIGINT NOT NULL,
    order_leg_type order_leg_type_enum,
    instrument_id BIGINT REFERENCES ods.instrument(id),
    schwab_instrument_id BIGINT,
    instruction order_instruction_enum,
    position_effect order_position_effect_enum,
    quantity DOUBLE PRECISION,
    quantity_type order_quantity_type_enum,
    div_cap_gains order_div_cap_gains_enum,
    to_symbol TEXT,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, leg_id)
);

ALTER TABLE IF EXISTS ods.order_leg
    ADD COLUMN IF NOT EXISTS schwab_instrument_id BIGINT,
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS ods.order_activity (
    order_id BIGINT NOT NULL REFERENCES ods.orders(order_id) ON DELETE CASCADE,
    activity_id BIGINT PRIMARY KEY,
    activity_type order_activity_type_enum,
    execution_type order_execution_type_enum,
    quantity DOUBLE PRECISION,
    order_remaining_quantity DOUBLE PRECISION,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS ods.order_activity
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS ods.execution_leg (
    activity_id BIGINT NOT NULL REFERENCES ods.order_activity(activity_id) ON DELETE CASCADE,
    leg_id BIGINT NOT NULL,
    price DOUBLE PRECISION,
    quantity DOUBLE PRECISION,
    mismarked_quantity DOUBLE PRECISION,
    schwab_instrument_id BIGINT,
    exec_time TIMESTAMPTZ,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_id, leg_id)
);

ALTER TABLE IF EXISTS ods.execution_leg
    ADD COLUMN IF NOT EXISTS schwab_instrument_id BIGINT,
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS ods.account_transaction (
    transaction_id TEXT PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
    activity_id TEXT,
    account_number TEXT,
    transaction_time TIMESTAMPTZ,
    transaction_type TEXT,
    status TEXT,
    sub_account TEXT,
    trade_date DATE,
    settlement_date DATE,
    position_id TEXT,
    order_id BIGINT,
    net_amount NUMERIC,
    description TEXT,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_account_transaction_account_time
ON ods.account_transaction (account_id, transaction_time DESC);

CREATE TABLE IF NOT EXISTS ods.transaction_transfer_item (
    transaction_id TEXT NOT NULL REFERENCES ods.account_transaction(transaction_id) ON DELETE CASCADE,
    item_sequence INT NOT NULL,
    instrument_id BIGINT REFERENCES ods.instrument(id),
    symbol TEXT,
    cusip TEXT,
    asset_type TEXT,
    quantity DOUBLE PRECISION,
    price NUMERIC,
    cost NUMERIC,
    amount NUMERIC,
    fee_type TEXT,
    position_effect TEXT,
    instruction TEXT,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, item_sequence)
);

CREATE INDEX IF NOT EXISTS idx_transaction_transfer_item_instrument
ON ods.transaction_transfer_item (instrument_id);
