# mock_data.py

# 1. Source: MySQL - Table 'users'
MYSQL_USERS = [
    {"user_id": 1, "full_name": "Andi Setiawan", "email": "andi@mail.com", "phone_number": "081234567890", "created_at": "2023-01-15 08:00:00", "updated_at": "2023-01-15 08:00:00"},
    {"user_id": 2, "full_name": "Budi Santoso", "email": "budi@mail.com", "phone_number": "081298765432", "created_at": "2023-02-20 14:30:00", "updated_at": "2023-02-20 14:30:00"},
    {"user_id": 3, "full_name": "Citra Lestari", "email": "citra@mail.com", "phone_number": "081311223344", "created_at": "2023-03-10 09:15:00", "updated_at": "2023-03-12 10:00:00"}
]

# 2. Source: MySQL - Table 'merchants'
MYSQL_MERCHANTS = [
    {"merchant_id": 101, "merchant_name": "Kopi Kenangan", "category": "F&B", "status": "ACTIVE", "created_at": "2022-12-01 10:00:00"},
    {"merchant_id": 102, "merchant_name": "Indomaret", "category": "Retail", "status": "ACTIVE", "created_at": "2022-11-15 08:00:00"},
    {"merchant_id": 103, "merchant_name": "Toko Kelontong Lama", "category": "Retail", "status": "INACTIVE", "created_at": "2020-01-01 08:00:00"}
]

# 3. Source: MongoDB - Collection 'transactions'
MONGO_TRANSACTIONS = [
    {
        "_id": "64f1a2b3c4d5", "user_id": 1, "merchant_id": 101, "amount": 25000.0, "currency": "IDR",
        "transaction_time": "2023-10-27 10:00:00", "status": "SUCCESS",
        "metadata": {"device": "Android", "promo": "HEMAT10"}
    },
    {
        "_id": "64f1a2b3c4d6", "user_id": 2, "merchant_id": 102, "amount": 150000.0, "currency": "IDR",
        "transaction_time": "2023-10-27 10:30:00", "status": "SUCCESS",
        "metadata": {"device": "iOS", "promo": ""}
    },
    {
        "_id": "64f1a2b3c4d7", "user_id": 1, "merchant_id": 102, "amount": 5000000.0, "currency": "IDR",
        "transaction_time": "2023-10-27 11:00:00", "status": "PENDING",
        "metadata": {"device": "Android"}
    }
]