-- Database patch 002: Business tables for simulated Saga workflows
-- Adds accounts and orders tracking

CREATE TABLE IF NOT EXISTS saga_accounts (
    account_id VARCHAR(64) PRIMARY KEY,
    balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS saga_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL REFERENCES saga_accounts(account_id),
    amount DECIMAL(15, 2) NOT NULL,
    status VARCHAR(50) NOT NULL -- PENDING, COMPLETED, CANCELLED
);

-- Seed initial test accounts for verification
INSERT INTO saga_accounts (account_id, balance)
VALUES 
    ('acc-alice', 1000.00),
    ('acc-bob', 500.00)
ON CONFLICT (account_id) DO UPDATE 
SET balance = EXCLUDED.balance;
