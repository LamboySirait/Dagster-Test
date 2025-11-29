SELECT 
    m.merchant_name,
    m.category,
    
    -- Count distinct transaction IDs (from MongoDB source)
    COUNT(t.transaction_mongo_id) AS total_transactions,
    
    -- Calculate total Gross Transaction Value (GTV)
    SUM(t.amount) AS total_revenue,
    
    -- Calculate Average Ticket Size
    ROUND(AVG(t.amount), 2) AS avg_ticket_size
FROM 
    fact_transactions AS t
-- Join with Dimension table to get readable Merchant names
INNER JOIN 
    dim_merchants AS m ON t.merchant_id = m.merchant_id
WHERE 
    t.status = 'SUCCESS'
GROUP BY 
    m.merchant_id, 
    m.merchant_name, 
    m.category
ORDER BY 
    total_revenue DESC;