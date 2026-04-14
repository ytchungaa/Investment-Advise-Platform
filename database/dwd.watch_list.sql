CREATE TABLE IF NOT EXISTS dwd.watch_list (
    symbol TEXT PRIMARY KEY,
    symbol_name TEXT,
    asset_type TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE dwd.watch_list ADD COLUMN IF NOT EXISTS symbol_name TEXT;
ALTER TABLE dwd.watch_list ADD COLUMN IF NOT EXISTS asset_type TEXT;
ALTER TABLE dwd.watch_list ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE dwd.watch_list ADD COLUMN IF NOT EXISTS added_at TIMESTAMPTZ NOT NULL DEFAULT now();

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'dwd'
          AND table_name = 'stock_list'
    ) THEN
        INSERT INTO dwd.watch_list (symbol, symbol_name, asset_type)
        SELECT symbol, symbol_name, type
        FROM dwd.stock_list
        ON CONFLICT (symbol) DO UPDATE
        SET symbol_name = EXCLUDED.symbol_name,
            asset_type = COALESCE(EXCLUDED.asset_type, dwd.watch_list.asset_type);
    END IF;
END $$;

CREATE OR REPLACE VIEW dwd.dim_instrument AS
SELECT
    id AS instrument_id,
    symbol,
    asset_type,
    cusip,
    description,
    exchange,
    asset_main_type,
    asset_sub_type,
    quote_type,
    ssid,
    realtime,
    first_seen_at,
    last_seen_at
FROM ods.instrument;

CREATE OR REPLACE VIEW dwd.fact_quote_latest AS
SELECT DISTINCT ON (instrument_id)
    instrument_id,
    as_of_time,
    week_52_high,
    week_52_low,
    ask_mic_id,
    ask_price,
    ask_size,
    ask_time,
    bid_mic_id,
    bid_price,
    bid_size,
    bid_time,
    close_price,
    high_price,
    last_mic_id,
    last_price,
    last_size,
    low_price,
    mark,
    mark_change,
    mark_percent_change,
    net_change,
    net_percent_change,
    open_price,
    post_market_change,
    post_market_percent_change,
    quote_time,
    trade_time,
    security_status,
    total_volume
FROM ods.quote_history
ORDER BY instrument_id, as_of_time DESC;

CREATE OR REPLACE VIEW dwd.fact_fundamental_latest AS
SELECT DISTINCT ON (instrument_id)
    instrument_id,
    as_of_time,
    week_52_high,
    week_52_low,
    dividend_amount,
    dividend_yield,
    dividend_date,
    pe_ratio,
    peg_ratio,
    pb_ratio,
    pr_ratio,
    pcf_ratio,
    gross_margin_ttm,
    gross_margin_mrq,
    net_profit_margin_ttm,
    net_profit_margin_mrq,
    operating_margin_ttm,
    operating_margin_mrq,
    return_on_equity,
    return_on_assets,
    return_on_investment,
    quick_ratio,
    current_ratio,
    interest_coverage,
    total_debt_to_capital,
    lt_debt_to_equity,
    total_debt_to_equity,
    eps_ttm,
    eps_change_percent_ttm,
    eps_change_year,
    eps_change,
    rev_change_year,
    rev_change_ttm,
    rev_change_in,
    shares_outstanding,
    market_cap_float,
    market_cap,
    book_value_per_share,
    short_int_to_float,
    short_int_day_to_cover,
    div_growth_rate_3_year,
    dividend_pay_amount,
    dividend_pay_date,
    beta,
    vol_1_day_avg,
    vol_10_day_avg,
    vol_3_month_avg,
    avg_10_days_volume,
    avg_1_day_volume,
    avg_3_month_volume,
    declaration_date,
    dividend_freq,
    eps,
    dtn_volume,
    next_dividend_pay_date,
    next_dividend_date,
    fund_leverage_factor
FROM ods.instrument_fundamental_history
ORDER BY instrument_id, as_of_time DESC;

CREATE OR REPLACE VIEW dwd.fact_price_daily AS
SELECT
    ph.instrument_id,
    i.symbol,
    i.asset_type,
    ph.candle_time,
    ph.open,
    ph.high,
    ph.low,
    ph.close,
    ph.volume,
    ph.previous_close,
    ph.previous_close_time,
    ph.need_extended_hours_data
FROM ods.price_history ph
JOIN ods.instrument i ON i.id = ph.instrument_id
WHERE ph.frequency_type = 'daily'
  AND ph.frequency = 1;
