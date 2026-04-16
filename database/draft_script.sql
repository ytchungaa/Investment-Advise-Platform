SELECT instrument_id, symbol, COUNT(*) FROM ods.price_history 
LEFT JOIN ods.instrument ON ods.price_history.instrument_id = ods.instrument.id
GROUP BY instrument_id, symbol;


SELECT * FROM ods.instrument LIMIT 1000;

SELECT symbol, * FROM ods.price_history 
LEFT JOIN ods.instrument ON ods.price_history.instrument_id = ods.instrument.id
WHERE symbol = 'AAPL'
LIMIT 1000;