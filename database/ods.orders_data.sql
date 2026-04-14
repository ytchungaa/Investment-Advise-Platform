-- ====== ENUMS (orders)
CREATE TYPE order_session_enum AS ENUM ('NORMAL','AM','PM','SEAMLESS');                                            -- :contentReference[oaicite:0]{index=0}
CREATE TYPE order_duration_enum AS ENUM ('DAY','GOOD_TILL_CANCEL','FILL_OR_KILL','IMMEDIATE_OR_CANCEL','END_OF_WEEK','END_OF_MONTH','NEXT_END_OF_MONTH','UNKNOWN'); -- :contentReference[oaicite:1]{index=1}
CREATE TYPE order_type_enum AS ENUM ('MARKET','LIMIT','STOP','STOP_LIMIT','TRAILING_STOP','CABINET','NON_MARKETABLE','MARKET_ON_CLOSE','EXERCISE','TRAILING_STOP_LIMIT','NET_DEBIT','NET_CREDIT','NET_ZERO','LIMIT_ON_CLOSE','UNKNOWN'); -- :contentReference[oaicite:2]{index=2}
CREATE TYPE order_complex_strategy_enum AS ENUM ('NONE','COVERED','VERTICAL','BACK_RATIO','CALENDAR','DIAGONAL','STRADDLE','STRANGLE','COLLAR_SYNTHETIC','BUTTERFLY','CONDOR','IRON_CONDOR','VERTICAL_ROLL','COLLAR_WITH_STOCK','DOUBLE_DIAGONAL','UNBALANCED_BUTTERFLY','UNBALANCED_CONDOR','UNBALANCED_IRON_CONDOR','UNBALANCED_VERTICAL_ROLL','MUTUAL_FUND_SWAP','CUSTOM'); -- :contentReference[oaicite:3]{index=3}
CREATE TYPE order_dest_enum AS ENUM ('INET','ECN_ARCA','CBOE','AMEX','PHLX','ISE','BOX','NYSE','NASDAQ','BATS','C2','AUTO');  -- :contentReference[oaicite:4]{index=4}
CREATE TYPE order_strategy_enum AS ENUM ('SINGLE','CANCEL','RECALL','PAIR','FLATTEN','TWO_DAY_SWAP','BLAST_ALL','OCO','TRIGGER'); -- :contentReference[oaicite:5]{index=5}
CREATE TYPE order_status_enum AS ENUM ('AWAITING_PARENT_ORDER','AWAITING_CONDITION','AWAITING_STOP_CONDITION','AWAITING_MANUAL_REVIEW','ACCEPTED','AWAITING_UR_OUT','PENDING_ACTIVATION','QUEUED','WORKING','REJECTED','PENDING_CANCEL','CANCELED','PENDING_REPLACE','REPLACED','FILLED','EXPIRED','NEW','AWAITING_RELEASE_TIME','PENDING_ACKNOWLEDGEMENT','PENDING_RECALL','UNKNOWN'); -- :contentReference[oaicite:6]{index=6}
CREATE TYPE order_leg_type_enum AS ENUM ('EQUITY','OPTION','INDEX','MUTUAL_FUND','CASH_EQUIVALENT','FIXED_INCOME','CURRENCY','COLLECTIVE_INVESTMENT'); -- :contentReference[oaicite:7]{index=7}
CREATE TYPE order_instruction_enum AS ENUM ('BUY','SELL','BUY_TO_COVER','SELL_SHORT','BUY_TO_OPEN','BUY_TO_CLOSE','SELL_TO_OPEN','SELL_TO_CLOSE','EXCHANGE','SELL_SHORT_EXEMPT'); -- :contentReference[oaicite:8]{index=8}
CREATE TYPE order_position_effect_enum AS ENUM ('OPENING','CLOSING','AUTOMATIC'); -- :contentReference[oaicite:9]{index=9}
CREATE TYPE order_quantity_type_enum AS ENUM ('ALL_SHARES','DOLLARS','SHARES'); -- :contentReference[oaicite:10]{index=10}
CREATE TYPE order_div_cap_gains_enum AS ENUM ('REINVEST','PAYOUT'); -- :contentReference[oaicite:11]{index=11}
CREATE TYPE order_special_instruction_enum AS ENUM ('ALL_OR_NONE','DO_NOT_REDUCE','ALL_OR_NONE_DO_NOT_REDUCE'); -- :contentReference[oaicite:12]{index=12}

-- ====== ORDERS (one row per Schwab order)
CREATE TABLE IF NOT EXISTS ods.orders (
  order_id           BIGINT PRIMARY KEY,                        -- e.g. 1003060390456, unique per Schwab sample  
  account_id         BIGINT NOT NULL REFERENCES ods.securities_account(id) ON DELETE CASCADE,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when you pulled it (fits your snapshot model) :contentReference[oaicite:14]{index=14}
  session            order_session_enum,
  duration           order_duration_enum,
  order_type         order_type_enum,
  complex_strategy   order_complex_strategy_enum,
  quantity           DOUBLE PRECISION,
  filled_quantity    DOUBLE PRECISION,
  remaining_quantity DOUBLE PRECISION,
  requested_destination order_dest_enum,
  destination_link_name TEXT,
  price              DOUBLE PRECISION,
  tax_lot_method     TEXT,         -- keep TEXT; enum available if needed later  :contentReference[oaicite:15]{index=15}
  order_strategy     order_strategy_enum,
  cancelable         BOOLEAN, 
  editable           BOOLEAN,
  status             order_status_enum,
  entered_time       TIMESTAMPTZ,
  close_time         TIMESTAMPTZ,
  tag                TEXT,         -- e.g. API_TOS:FUNDAMENTALS / POS_STMT  
  account_number     TEXT NOT NULL -- denormalized for quick joins / validation  
);

-- ====== ORDER LEGS (EQUITY / FIXED_INCOME etc., per legId)
CREATE TABLE IF NOT EXISTS ods.order_leg (
  order_id        BIGINT NOT NULL REFERENCES ods.orders(order_id) ON DELETE CASCADE,
  leg_id          BIGINT NOT NULL,
  order_leg_type  order_leg_type_enum,
  instrument_id   BIGINT REFERENCES ods.instrument(id),  -- resolve via (symbol, asset_type) or instrumentId during ETL 
  instruction     order_instruction_enum,
  position_effect order_position_effect_enum,
  quantity        DOUBLE PRECISION,
  quantity_type   order_quantity_type_enum,
  div_cap_gains   order_div_cap_gains_enum,
  to_symbol       TEXT,
  PRIMARY KEY (order_id, leg_id)
);

-- ====== ORDER ACTIVITIES (fills / cancellations, etc.)
CREATE TYPE order_activity_type_enum AS ENUM ('EXECUTION','ORDER_ACTION');  -- :contentReference[oaicite:19]{index=19}
CREATE TYPE order_execution_type_enum AS ENUM ('FILL');                      -- :contentReference[oaicite:20]{index=20}

CREATE TABLE IF NOT EXISTS ods.order_activity (
  order_id                 BIGINT NOT NULL REFERENCES ods.orders(order_id) ON DELETE CASCADE,
  activity_id              BIGINT PRIMARY KEY,  -- from sample: activityId  
  activity_type            order_activity_type_enum,
  execution_type           order_execution_type_enum,
  quantity                 DOUBLE PRECISION,
  order_remaining_quantity DOUBLE PRECISION
);

-- ====== EXECUTION LEGS (per activity x leg)
CREATE TABLE IF NOT EXISTS ods.execution_leg (
  activity_id        BIGINT NOT NULL REFERENCES ods.order_activity(activity_id) ON DELETE CASCADE,
  leg_id             BIGINT NOT NULL,
  price              DOUBLE PRECISION,
  quantity           DOUBLE PRECISION,
  mismarked_quantity DOUBLE PRECISION,
  instrument_id      BIGINT,              -- raw Schwab instrumentId for traceability  :contentReference[oaicite:22]{index=22}
  exec_time          TIMESTAMPTZ,
  PRIMARY KEY (activity_id, leg_id)
);

-- ====== OPTIONAL: link to child/replacing orders if you capture those later
-- CREATE TYPE order_link_enum AS ENUM ('CHILD','REPLACING');
-- CREATE TABLE order_link (order_id BIGINT, linked_order_id BIGINT, link_type order_link_enum, PRIMARY KEY(order_id, linked_order_id, link_type));
