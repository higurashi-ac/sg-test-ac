CREATE TABLE IF NOT EXISTS payment_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    payment_date DATE,
    game VARCHAR(100),
    currency VARCHAR(10),
    price FLOAT,
    status VARCHAR(20)
);

CREATE OR REPLACE VIEW daily_game_aggregates AS
SELECT
    payment_date,
    game,
    COUNT(*) as transaction_count,
    SUM(CASE WHEN status = 'successfull' THEN 1 ELSE 0 END) as successful_transactions,
    SUM(CASE 
        WHEN status = 'successfull' THEN 
            CASE currency
                WHEN 'USD' THEN price * 0.93  -- USD to EUR
                WHEN 'GBP' THEN price * 1.17  -- GBP to EUR
                WHEN 'JPY' THEN price * 0.006 -- JPY to EUR
                WHEN 'CAD' THEN price * 0.68  -- CAD to EUR
                WHEN 'AUD' THEN price * 0.61  -- AUD to EUR
                ELSE price                   -- Already EUR or unknown (treated as EUR)
            END
        ELSE 0 
    END) as revenue_eur
FROM payment_transactions
GROUP BY payment_date, game;