{{ config(
  materialized='view'
) }}

WITH
  CalculatedPaymentDates AS (
    -- This entire section is your previous query, now acting as a CTE to prepare the data.
    WITH
      OrderInvoice AS (
        -- Step 1: Calculate the invoice amount for each buyer order.
        SELECT
          BuyerOrderID,
          OrderNumber,
          FinalLitres * BuyerPrice AS InvoiceAmount
        FROM
          `tfn-data-warehouse.nto_data.nto_buyer_order`
      ),
      PaymentTotals AS (
        -- Step 2: Calculate total payments and find the first payment date for each order.
        SELECT
          olt.BuyerOrderID,
          SUM(te.Amount) AS TotalPaid,
          MIN(olt.TransactionDate) AS FirstPaymentDate
        FROM
          `tfn-data-warehouse.nto_data.nto_order_load_transaction_all_types` olt
        JOIN
          `tfn-data-warehouse.nto_data.nto_order_load_linked_transaction_type` tt
          ON olt.OrderLoadTransactionTypeID = tt.OrderLoadLinkedTransactionTypeID
        LEFT JOIN
          `tfn-data-warehouse.nto_data.nto_transaction_entry` te
          ON olt.TransactionID = te.TransactionEntryID
        WHERE
          tt.title = 'Payment'
        GROUP BY
          olt.BuyerOrderID
      ),
      FallbackDateCandidates AS (
        -- Step 3: Determine the candidate date for the fallback logic.
        SELECT
          olt.BuyerOrderID,
          CASE
            WHEN tt.title = 'Allocation' THEN olt.CapturedDate
            -- LOGIC: Use CapturedDate from the transaction_entry table for these payments.
            WHEN tt.title = 'Payment' AND olt.TransactionID != '00000000-0000-0000-0000-000000000000' THEN te.CapturedDate
            WHEN tt.title = 'Payment' AND olt.TransactionID = '00000000-0000-0000-0000-000000000000' THEN olt.CapturedDate
          END AS CandidateDate
        FROM
          `tfn-data-warehouse.nto_data.nto_order_load_transaction_all_types` olt
        JOIN
          `tfn-data-warehouse.nto_data.nto_order_load_linked_transaction_type` tt
          ON olt.OrderLoadTransactionTypeID = tt.OrderLoadLinkedTransactionTypeID
        -- CORRECTED JOIN: Ensuring the link to the transaction entry table uses the correct ID.
        LEFT JOIN
          `tfn-data-warehouse.nto_data.nto_transaction_entry` te
          ON olt.TransactionID = te.TransactionEntryID
        WHERE
          tt.title IN ('Payment', 'Allocation')
      ),
      LatestFallbackDate AS (
        -- Step 4: Find the most recent (max) candidate date from the fallback options.
        SELECT
          BuyerOrderID,
          MAX(CandidateDate) AS MaxFallbackDate
        FROM
          FallbackDateCandidates
        GROUP BY
          BuyerOrderID
      )
    -- Step 5: Combine the data and apply the primary and fallback logic.
    SELECT
      oi.BuyerOrderID,
      oi.OrderNumber,
      oi.InvoiceAmount,
      COALESCE(pt.TotalPaid, 0) AS TotalPaid,
      -- This is the core logic that decides which date to use.
      CASE
        WHEN pt.TotalPaid >= oi.InvoiceAmount THEN pt.FirstPaymentDate
        ELSE fd.MaxFallbackDate
      END AS PaymentDate
    FROM
      OrderInvoice oi
    LEFT JOIN
      PaymentTotals pt
      ON oi.BuyerOrderID = pt.BuyerOrderID
    LEFT JOIN
      LatestFallbackDate fd
      ON oi.BuyerOrderID = fd.BuyerOrderID
  )
-- Final Step: Join the calculated payment dates back to the buyer order table and apply the date filter.
SELECT
  cpd.BuyerOrderID,
  bo.OrderNumberTitle AS BuyerOrder,
  cpd.OrderNumber,
  cpd.InvoiceAmount,
  cpd.TotalPaid,
  cpd.PaymentDate,
  bo.CapturedDate AS OrderCapturedDate
  -- You can add any other columns from the nto_buyer_order table you need here.
FROM
  CalculatedPaymentDates cpd
JOIN
  `tfn-data-warehouse.nto_data.nto_buyer_order` bo
  ON cpd.BuyerOrderID = bo.BuyerOrderID
WHERE
  -- Apply the final date filter as requested.
  bo.CapturedDate > '2025-09-01'
ORDER BY
  cpd.OrderNumber
