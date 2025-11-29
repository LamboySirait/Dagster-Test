-- 1. Dimension: Users
CREATE TABLE dim_users (
    user_id UInt32,
    full_name String,
    email String,
    phone_number String,
    created_at DateTime,
    updated_at DateTime
) 
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

-- 2. Dimension: Merchants
CREATE TABLE dim_merchants (
    merchant_id UInt32,
    merchant_name String,
    category LowCardinality(String), 
    status LowCardinality(String),
    updated_at DateTime
) 
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY merchant_id;

-- 3. Fact: Transactions
CREATE TABLE IF NOT EXISTS fact_transactions (
    -- ID & Keys
    transaction_mongo_id String,
    user_id UInt32,
    merchant_id UInt32,
    
    -- Metrics
    amount Decimal128(4),
    currency LowCardinality(String), -- Ada di Mock Data
    status LowCardinality(String),
    
    -- Transformed Columns
    device_type String,              -- Hasil Flatten metadata
    promo_code String,               -- Hasil Flatten metadata
    amount_category String,          -- Hasil Logic Python (New!)
    
    -- Timestamps
    transaction_time DateTime,
    ingestion_time DateTime DEFAULT now()
) 
ENGINE = MergeTree()
ORDER BY (transaction_time, merchant_id, user_id);