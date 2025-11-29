SELECT 
    -- Convert DateTime to Date for daily aggregation
    toDate(transaction_time) AS transaction_date,
    
    user_id,
    
    -- Summing the Decimal128 column for precise calculation
    SUM(amount) AS total_daily_spent,
    
    -- Optional: Count how many transactions they made that day
    COUNT() AS transaction_count
FROM 
    fact_transactions
WHERE 
    status = 'SUCCESS' -- Only count successful transactions
GROUP BY 
    transaction_date, 
    user_id
ORDER BY 
    transaction_date DESC, 
    total_daily_spent DESC;