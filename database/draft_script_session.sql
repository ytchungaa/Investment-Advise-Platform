SELECT candle_time, COUNT(*) FROM ods.price_history 
GROUP BY candle_time 
ORDER BY candle_time DESC
LIMIT 1000;