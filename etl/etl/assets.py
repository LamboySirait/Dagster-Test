
from dagster import asset
from .db import get_mssql_engine
from dagster import  asset, Output, MetadataValue
from .ch import get_clickhouse_client
from .mock_data import MYSQL_USERS, MYSQL_MERCHANTS, MONGO_TRANSACTIONS
import pandas as pd
from datetime import datetime
import os




# -- test dagster dari db sendiri :v

# @asset
# def load_customers(extract_customers):
#     os.makedirs("output", exist_ok=True)
#     extract_customers.to_csv("output/customers.csv", index=False)


# -- test ke ch 


@asset
def test_clickhouse_connection():
    client = get_clickhouse_client()
    df = client.query_df("SELECT 1")
    return df

# ==========================================
# 1. EXTRACT (Ingestion Layer)
# ==========================================
# --- HELPER FUNCTION: CHECKPOINTING || KEPERLUAN INCREMENTAL LOAD, MENGAMBIL MAX TIME STAMP DARI TABLE TARGER---

def get_high_watermark():
    """
    Mengecek waktu transaksi terakhir di ClickHouse.
    Return datetime 1970-01-01 jika tabel belum ada atau kosong.
    """
    client = get_clickhouse_client()
    
    # 1. Cek apakah tabel exists
    # Note: Syntax check table bisa beda tergantung driver, 
    # cara paling aman adalah try-except query
    try:
        # Mengambil nilai maksimum transaction_time
        # result query biasanya list of tuples/rows
        query = "SELECT max(transaction_time) FROM fact_transactions"
        result = client.query(query) 
        
        # Logika parsing result tergantung library (clickhouse-connect / clickhouse-driver)
        # Asumsi result.result_rows (clickhouse_connect)
        if result.result_rows and result.result_rows[0][0]:
            last_time = result.result_rows[0][0]
            return pd.to_datetime(last_time) # Pastikan format datetime pandas
            
    except Exception as e:
        # Jika tabel error / belum ada, anggap start from beginning
        print(f"Table check warning: {e}")
        pass
        
    return pd.to_datetime("1970-01-01 00:00:00")



@asset(group_name="ingestion", compute_kind="python")
def raw_users():
    """Extract Users data from Source (Mock MySQL)"""
    return pd.DataFrame(MYSQL_USERS)

@asset(group_name="ingestion", compute_kind="python")
def raw_merchants():
    """Extract Merchants data from Source (Mock MySQL)"""
    return pd.DataFrame(MYSQL_MERCHANTS)

# @asset(group_name="ingestion", compute_kind="python")
# def raw_transactions():
#     """Extract Transactions data from Source (Mock MongoDB)"""
#     return pd.DataFrame(MONGO_TRANSACTIONS)


@asset(group_name="ingestion", compute_kind="python")
def raw_transactions():
    """
    Extract Transactions dengan INCREMENTAL LOGIC.
    Hanya mengambil data yang lebih baru dari data di ClickHouse.
    """
    # 1. Get Checkpoint (Last Sync Time)
    last_sync = get_high_watermark()
    print(f"--- CHECKPOINT: Mengambil data setelah {last_sync} ---")

    # 2. Get All Data from Source (Mock)
    df = pd.DataFrame(MONGO_TRANSACTIONS)
    
    # Konversi ke datetime agar bisa dibandingkan
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    
    # 3. FILTER LOGIC (The Core of Incremental Load)
    new_data = df[df['transaction_time'] > last_sync]
    
    count_all = len(df)
    count_new = len(new_data)
    
    print(f"--- RESULT: Ditemukan {count_new} data baru dari total {count_all} data source ---")

    return Output(
        new_data,
        metadata={
            "source_total_rows": count_all,
            "new_rows_ingested": count_new,
            "last_watermark": str(last_sync)
        }
    )
    

# ==========================================
# 2. TRANSFORM (Processing Layer)
# ==========================================

@asset(group_name="processing", compute_kind="pandas")
def transformed_transactions(raw_transactions):
    """
    Cleaning & Transformation Logic:
    1. Flatten JSON metadata (device, promo).
    2. Categorize Amount (Low/Medium/High).
    3. Type Enforcement (DateTime).
    4. Rename columns to match ClickHouse DDL.
    """
    df = raw_transactions.copy()

    # A. Flattening Nested JSON (metadata)
    # Extract 'device' -> 'device_type'
    df['device_type'] = df['metadata'].apply(
        lambda x: x.get('device', 'Unknown') if isinstance(x, dict) else 'Unknown'
    )
    # Extract 'promo' -> 'promo_code'
    df['promo_code'] = df['metadata'].apply(
        lambda x: x.get('promo', '') if isinstance(x, dict) else ''
    )

    # B. Business Logic: Categorize Amount
    def categorize(amount):
        if amount >= 1000000: return 'High Value'
        if amount >= 100000: return 'Medium Value'
        return 'Low Value'
    
    df['amount_category'] = df['amount'].apply(categorize)

    # C. Standardizing Columns
    # Rename MongoDB _id to ClickHouse column name
    df = df.rename(columns={'_id': 'transaction_mongo_id'})
    
    # Ensure Timestamp format
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    
    # D. Cleanup & Ordering
    # Drop original nested column
    df = df.drop(columns=['metadata'])
    
    # E. Add ingestion_time
    df['ingestion_time'] = datetime.now()
    
    # STRICT ORDERING: Must match ClickHouse DDL exactly for cleaner inserts
    target_columns = [
        'transaction_mongo_id', 'user_id', 'merchant_id', 
        'amount', 'currency', 'status', 
        'device_type', 'promo_code', 'amount_category', 
        'transaction_time','ingestion_time'
    ]
    df = df[target_columns]

    # Return Output with Metadata for UI Preview
    return Output(
        df,
        metadata={
            "row_count": len(df),
            "columns": list(df.columns),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

# ==========================================
# 3. LOAD (Warehouse Layer)
# ==========================================

@asset(group_name="warehouse", compute_kind="clickhouse")
def fact_transactions_table(transformed_transactions):
    """
    Load Transformed Data into ClickHouse.
    Ensures table exists before insertion.
    """
    client = get_clickhouse_client()
    
    
    # 1. Ensure Table Exists (Idempotency)
    # DDL ini SINKRON dengan Mock Data & Transformasi Python di atas
    ddl_query = """
    CREATE TABLE IF NOT EXISTS fact_transactions (
        transaction_mongo_id String,
        user_id UInt32,
        merchant_id UInt32,
        amount Decimal128(4),
        currency LowCardinality(String),
        status LowCardinality(String),
        device_type String,
        promo_code String,
        amount_category String,
        transaction_time DateTime,
        ingestion_time DateTime
    ) 
    ENGINE = MergeTree()
    ORDER BY (transaction_time, merchant_id, user_id);
    """
    client.command(ddl_query)
    
    # 2. Insert Data
    # Menggunakan to_dict('records') agar aman saat mapping ke client insert
    data = transformed_transactions.to_dict('records')
    
    # Insert ke tabel 'fact_transactions'
    # Note: Pastikan library ch.py kamu support method insert/insert_df
    # Jika menggunakan clickhouse_connect:
    client.insert('fact_transactions', transformed_transactions)
    
    return MetadataValue.text(f"Successfully loaded {len(transformed_transactions)} rows into ClickHouse.")



# ==========================================
# 4. Incremental LOAD (Warehouse Layer)
# ==========================================

# --- HELPER FUNCTION: CHECKPOINTING ---

