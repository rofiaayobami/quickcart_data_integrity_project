sql
-- 1. Orders Table
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    customer_email TEXT,
    order_total_cents INT,
    currency TEXT,
    is_test INT,
    created_at TIMESTAMP
);

-- 2. Payments Table
CREATE TABLE IF NOT EXISTS payments (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT REFERENCES orders(order_id),
    attempt_no INT,
    provider TEXT,
    provider_ref TEXT,
    status TEXT,
    amount_cents INT,
    attempted_at TIMESTAMP
);

-- 3. Bank Settlements Table
CREATE TABLE IF NOT EXISTS bank_settlements (
    settlement_id TEXT PRIMARY KEY,
    payment_id TEXT,
    provider_ref TEXT,
    status TEXT, 
    settled_amount_cents INT,
    currency TEXT,
    settled_at TIMESTAMP
);

-- 4. Internal Logs Table (For your Python output)
CREATE TABLE IF NOT EXISTS internal_cleaned_logs (
    transaction_id TEXT,
    amount_usd NUMERIC,
    timestamp TIMESTAMP,
    event_id TEXT
);

