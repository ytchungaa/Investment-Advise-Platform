SELECT instrument_id, MAX(candle_time) AS latest_candle_time
        FROM price_history
        WHERE frequency_type = :frequency_type
          AND frequency = :frequency
        GROUP BY instrument_id