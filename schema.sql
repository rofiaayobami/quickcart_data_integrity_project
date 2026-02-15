
-- 1. Orders Table
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    customer_email TEXT NOT NULL,
    order_total_cents INT NOT NULL,
    currency TEXT NOT NULL,
    is_test INT DEFAULT 0,
    created_at TIMESTAMP NOT NULL
);

-- 2. Payments Table
CREATE TABLE IF NOT EXISTS payments (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT REFERENCES orders(order_id),
    attempt_no INT DEFAULT 1,
    provider TEXT NOT NULL,
    provider_ref TEXT,
    status TEXT not null check (status in ('SUCCESS', 'FAILED', 'PENDING')),
    amount_cents INT NOT NULL,
    attempted_at TIMESTAMP NOT NULL
);

-- 3. Bank Settlements Table
CREATE TABLE IF NOT EXISTS bank_settlements (
    settlement_id TEXT PRIMARY KEY,
    payment_id TEXT REFERENCES PAYMENTS(payment_id),
    provider_ref TEXT,
    status TEXT NOT NULL CHECK (status IN ('SETTLED')), -- SETTLED
    settled_amount_cents INT NOT NULL,
    currency TEXT NOT NULL,
    settled_at TIMESTAMP NOT NULL
);

-- 4. staging table for cleaned logs from Python
CREATE TABLE IF NOT EXISTS internal_cleaned_logs_stg (
    transaction_id TEXT,
    event_id TEXT,
    amount_usd NUMERIC(12,2),
    timestamp TIMESTAMP
);
