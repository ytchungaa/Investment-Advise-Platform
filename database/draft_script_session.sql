SELECT dwd.price_history_hourly.* FROM dwd.price_history_hourly 
LEFT JOIN ods.instrument ON dwd.price_history_hourly.instrument_id = ods.instrument.id
WHERE ods.instrument.symbol = 'AAPL'
ORDER BY bucket_start DESC
LIMIT 1000;

SELECT dwd.price_history_daily.* FROM dwd.price_history_daily 
LEFT JOIN ods.instrument ON dwd.price_history_daily.instrument_id = ods.instrument.id
WHERE ods.instrument.symbol = 'AAPL'
ORDER BY bucket_start DESC
LIMIT 1000;