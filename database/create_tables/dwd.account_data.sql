CREATE OR REPLACE VIEW dwd.dim_account AS
SELECT
    id AS account_id,
    CASE
        WHEN account_number IS NULL THEN NULL
        WHEN length(account_number) <= 4 THEN repeat('*', length(account_number))
        ELSE repeat('*', greatest(length(account_number) - 4, 0)) || right(account_number, 4)
    END AS masked_account_number,
    account_type,
    round_trips,
    is_day_trader,
    is_closing_only_restricted,
    pfcb_flag,
    created_at,
    last_seen_at
FROM ods.securities_account;

CREATE OR REPLACE VIEW dwd.fact_position_latest AS
SELECT
    p.as_of_time,
    p.account_id,
    da.masked_account_number,
    da.account_type,
    p.instrument_id,
    i.symbol,
    i.description,
    i.asset_type,
    i.exchange,
    i.asset_main_type,
    i.asset_sub_type,
    p.long_quantity,
    p.short_quantity,
    COALESCE(p.long_quantity, 0) - COALESCE(p.short_quantity, 0) AS net_quantity,
    p.average_price,
    p.average_long_price,
    p.taxlot_average_long_price,
    p.current_day_profit_loss,
    p.current_day_profit_loss_percentage,
    p.market_value,
    p.maintenance_requirement,
    p.long_open_profit_loss,
    p.previous_session_long_quantity,
    p.current_day_cost
FROM ods.v_position_latest p
JOIN dwd.dim_account da
    ON da.account_id = p.account_id
JOIN ods.instrument i
    ON i.id = p.instrument_id;

CREATE OR REPLACE VIEW dwd.fact_position_history AS
SELECT
    p.as_of_time,
    p.account_id,
    da.masked_account_number,
    da.account_type,
    p.instrument_id,
    i.symbol,
    i.description,
    i.asset_type,
    i.exchange,
    i.asset_main_type,
    i.asset_sub_type,
    p.long_quantity,
    p.short_quantity,
    COALESCE(p.long_quantity, 0) - COALESCE(p.short_quantity, 0) AS net_quantity,
    p.average_price,
    p.average_long_price,
    p.taxlot_average_long_price,
    p.current_day_profit_loss,
    p.current_day_profit_loss_percentage,
    p.market_value,
    p.maintenance_requirement,
    p.long_open_profit_loss,
    p.previous_session_long_quantity,
    p.current_day_cost
FROM ods.position p
JOIN dwd.dim_account da
    ON da.account_id = p.account_id
JOIN ods.instrument i
    ON i.id = p.instrument_id;

CREATE OR REPLACE VIEW dwd.fact_portfolio_composition_latest AS
WITH positive_positions AS (
    SELECT
        account_id,
        instrument_id,
        symbol,
        description,
        COALESCE(asset_type, 'UNKNOWN') AS asset_type,
        as_of_time,
        market_value::NUMERIC AS market_value
    FROM dwd.fact_position_latest
    WHERE market_value > 0
),
symbol_composition AS (
    SELECT
        'symbol'::TEXT AS composition_type,
        symbol AS label,
        MIN(description) AS description,
        NULL::TEXT AS asset_type,
        SUM(market_value) AS market_value,
        MAX(as_of_time) AS as_of_time
    FROM positive_positions
    GROUP BY symbol
),
asset_type_composition AS (
    SELECT
        'asset_type'::TEXT AS composition_type,
        asset_type AS label,
        NULL::TEXT AS description,
        asset_type,
        SUM(market_value) AS market_value,
        MAX(as_of_time) AS as_of_time
    FROM positive_positions
    GROUP BY asset_type
),
combined_composition AS (
    SELECT * FROM symbol_composition
    UNION ALL
    SELECT * FROM asset_type_composition
),
composition_totals AS (
    SELECT
        composition_type,
        SUM(market_value) AS total_market_value
    FROM combined_composition
    GROUP BY composition_type
)
SELECT
    c.composition_type,
    c.label,
    c.description,
    c.asset_type,
    c.market_value,
    CASE
        WHEN t.total_market_value > 0
            THEN c.market_value / t.total_market_value
        ELSE NULL
    END AS weight_pct,
    c.as_of_time
FROM combined_composition c
JOIN composition_totals t
    ON t.composition_type = c.composition_type;

CREATE OR REPLACE VIEW dwd.fact_account_balance_latest AS
SELECT
    b.as_of_time,
    b.account_id,
    da.masked_account_number,
    da.account_type,
    b.accrued_interest,
    b.cash_balance,
    b.cash_receipts,
    b.long_option_market_value,
    b.liquidation_value,
    b.long_market_value,
    b.money_market_fund,
    b.savings,
    b.short_market_value,
    b.pending_deposits,
    b.mutual_fund_value,
    b.bond_value,
    b.short_option_market_value,
    b.cash_available_for_trading,
    b.cash_available_for_withdrawal,
    b.cash_call,
    b.long_non_marginable_market_value,
    b.total_cash,
    b.cash_debit_call_value,
    b.unsettled_cash
FROM ods.v_account_current_balances_latest b
JOIN dwd.dim_account da
    ON da.account_id = b.account_id;

CREATE OR REPLACE VIEW dwd.fact_account_balance_history AS
SELECT
    b.as_of_time,
    b.account_id,
    da.masked_account_number,
    da.account_type,
    b.accrued_interest,
    b.cash_balance,
    b.cash_receipts,
    b.long_option_market_value,
    b.liquidation_value,
    b.long_market_value,
    b.money_market_fund,
    b.savings,
    b.short_market_value,
    b.pending_deposits,
    b.mutual_fund_value,
    b.bond_value,
    b.short_option_market_value,
    b.cash_available_for_trading,
    b.cash_available_for_withdrawal,
    b.cash_call,
    b.long_non_marginable_market_value,
    b.total_cash,
    b.cash_debit_call_value,
    b.unsettled_cash
FROM ods.account_current_balances b
JOIN dwd.dim_account da
    ON da.account_id = b.account_id;

CREATE OR REPLACE VIEW dwd.fact_portfolio_balance_latest AS
SELECT
    MAX(as_of_time) AS as_of_time,
    COUNT(DISTINCT account_id) AS account_count,
    SUM(COALESCE(liquidation_value, 0)) AS total_liquidation_value,
    SUM(COALESCE(cash_balance, 0)) AS total_cash_balance,
    SUM(COALESCE(total_cash, 0)) AS total_cash,
    SUM(COALESCE(long_market_value, 0)) AS total_long_market_value,
    SUM(COALESCE(short_market_value, 0)) AS total_short_market_value,
    SUM(COALESCE(money_market_fund, 0)) AS total_money_market_fund,
    SUM(COALESCE(mutual_fund_value, 0)) AS total_mutual_fund_value,
    SUM(COALESCE(bond_value, 0)) AS total_bond_value,
    SUM(COALESCE(cash_available_for_trading, 0)) AS total_cash_available_for_trading,
    SUM(COALESCE(cash_available_for_withdrawal, 0)) AS total_cash_available_for_withdrawal
FROM dwd.fact_account_balance_latest;

CREATE OR REPLACE VIEW dwd.fact_portfolio_balance_history AS
SELECT
    as_of_time,
    COUNT(DISTINCT account_id) AS account_count,
    SUM(COALESCE(liquidation_value, 0)) AS total_liquidation_value,
    SUM(COALESCE(cash_balance, 0)) AS total_cash_balance,
    SUM(COALESCE(total_cash, 0)) AS total_cash,
    SUM(COALESCE(long_market_value, 0)) AS total_long_market_value,
    SUM(COALESCE(short_market_value, 0)) AS total_short_market_value,
    SUM(COALESCE(money_market_fund, 0)) AS total_money_market_fund,
    SUM(COALESCE(mutual_fund_value, 0)) AS total_mutual_fund_value,
    SUM(COALESCE(bond_value, 0)) AS total_bond_value,
    SUM(COALESCE(cash_available_for_trading, 0)) AS total_cash_available_for_trading,
    SUM(COALESCE(cash_available_for_withdrawal, 0)) AS total_cash_available_for_withdrawal
FROM dwd.fact_account_balance_history
GROUP BY as_of_time;
