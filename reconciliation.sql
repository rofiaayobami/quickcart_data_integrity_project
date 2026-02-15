
WITH deduplicated_payments AS (
    SELECT 
        p.order_id,
        p.payment_id,
        p.amount_cents / 100.0 AS amount_usd,
        -- Use the is_test flag from the orders table
        o.is_test,
        ROW_NUMBER() OVER(
            PARTITION BY p.order_id 
            ORDER BY p.attempted_at DESC
        ) AS latest_attempt
    FROM payments p
    -- JOIN with orders to find out which are test records
    JOIN orders o ON p.order_id = o.order_id
    WHERE p.status = 'SUCCESS' 
      AND o.is_test = 0  -- Exclude test records based on the orders table
),


bank_truth AS (
    SELECT 
        payment_id,
        SUM(settled_amount_cents) / 100.0 AS settled_usd
    FROM bank_settlements
    WHERE status = 'SETTLED'
    GROUP BY payment_id
),

report AS (
    SELECT
        -- Internal Reality
        (SELECT SUM(amount_usd) 
         FROM deduplicated_payments 
         WHERE latest_attempt = 1) AS internal_sales,

        -- Bank Reality
        (SELECT SUM(settled_usd) 
         FROM bank_truth) AS bank_settled,

        -- Orphan Settlements
        (SELECT SUM(b.settled_usd)
         FROM bank_truth b
         LEFT JOIN payments p 
            ON b.payment_id = p.payment_id
         WHERE p.order_id IS NULL) AS orphan_total
)

SELECT 
    COALESCE(internal_sales, 0) AS "Total Internal Sales ($)",
    COALESCE(bank_settled, 0) AS "Total Bank Settled ($)",
    COALESCE(orphan_total, 0) AS "Orphan Payments ($)",
    COALESCE(internal_sales, 0) 
      - COALESCE(bank_settled, 0) AS "Discrepancy Gap ($)"
FROM report;
