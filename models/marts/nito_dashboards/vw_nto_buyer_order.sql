{{ config(
  materialized='view'
) }}

WITH wms_data AS (
  -- Get WMS data with conditional aggregation to pivot the Title values
  SELECT 
    a.BuyerOrderID,
    MAX(CASE WHEN b.Title = 'Commission' THEN a.LinkedAmount END) AS CommissionWMS,
    MAX(CASE WHEN b.Title = 'Interest' THEN a.LinkedAmount END) AS InterestWMS,
    MAX(CASE WHEN b.Title = 'Final Invoice Transaction' THEN a.LinkedAmount END) AS InvoiceAmountWMS
  FROM {{ source('nto_data', 'nto_order_load_transaction_all_types') }} a
  JOIN {{ source('nto_data', 'nto_order_load_linked_transaction_type') }} b 
    ON b.OrderLoadLinkedTransactionTypeID = a.OrderLoadTransactionTypeID
  GROUP BY a.BuyerOrderID
),
ranked_loads AS (
  -- Pick one load per buyer order (most recent by LoadId to avoid duplicates)
  SELECT 
    bo.BuyerOrderID,
    bo.LoadId,
    nto_loads.LoadNumberTitle AS LoadNumberTitle,
    nto_loads.TransporterID,
    so.OrderNumberTitle AS SupplierOrder,
    so.SupplierReference AS SupplierRef1,
    so.SupplierReference2 AS SupplierRef2,
    so.DepotTitle AS SupplierDepot,
    afvd.DealIdentifier AS LinkedSupplyDeal,
    ROW_NUMBER() OVER (PARTITION BY bo.BuyerOrderID ORDER BY nto_loads.LoadId DESC) as rn
  FROM {{ source('nto_data', 'nto_buyer_order') }} bo
  LEFT JOIN {{ source('nto_data', 'nto_loads') }} nto_loads
    ON bo.LoadId = nto_loads.LoadId
  LEFT JOIN {{ source('nto_data', 'nto_supplier_order') }} so
    ON bo.LoadId = so.LoadId
  LEFT JOIN {{ source('nto_data', 'nto_adhoc_fixed_volume_deal') }} afvd
    ON so.AdhocFixedVolumeDealID = afvd.AdhocFixedVolumeDealID
),
collection_rebate_prices_raw AS (
  -- Calculate purchase price for collection orders (IsForDelivery = false)
  SELECT 
    bo.BuyerOrderID,
    bo.FinalDepotGridPrice - bcrv2.Rebate as PurchasePrice,
    ROW_NUMBER() OVER (PARTITION BY bo.BuyerOrderID ORDER BY sacr.EffectiveFrom DESC) as rn
  FROM {{ source('nto_data', 'nto_buyer_order') }} bo
  JOIN {{ source('nto_data', 'nto_buyer_collected_rebate_value') }} bcrv 
    ON bcrv.BuyerCollectedRebateValueID = bo.BuyerRebateValueID
  JOIN {{ source('nto_data', 'nto_sales_agent_commission_rate') }} sacr 
    ON sacr.BuyerRebateID = bcrv.BuyerCollectedRebateID
  JOIN {{ source('nto_data', 'nto_buyer_collected_rebate_value') }} bcrv2 
    ON bcrv2.BuyerCollectedRebateID = sacr.VariableRateSalesAgentRebateID 
    AND (
      sacr.EffectiveFrom BETWEEN bcrv2.EffectiveFrom AND bcrv2.EffectiveTo 
      OR 
      (bcrv2.EffectiveTo = CAST('1900-01-01' AS DATETIME) AND sacr.EffectiveFrom > bcrv2.EffectiveFrom)
    )
  WHERE bo.IsForDelivery = false
),
collection_rebate_prices AS (
  -- Filter to ensure 1:1 relationship per BuyerOrderID
  SELECT 
    BuyerOrderID,
    PurchasePrice
  FROM collection_rebate_prices_raw
  WHERE rn = 1
),
delivery_rebate_prices_raw AS (
  -- Calculate purchase price for delivery orders (IsForDelivery = true)
  -- If AdjustedBuyerPrice > 0, use AdjustedBuyerPrice - rebate, else use BuyerPrice - rebate
  SELECT 
    bo.BuyerOrderID,
    CASE 
      WHEN bo.AdjustedBuyerPrice > 0 THEN bo.AdjustedBuyerPrice - bcrv2.Rebate
      ELSE bo.BuyerPrice - bcrv2.Rebate
    END as PurchasePrice,
    ROW_NUMBER() OVER (PARTITION BY bo.BuyerOrderID ORDER BY sacr.EffectiveFrom DESC) as rn
  FROM {{ source('nto_data', 'nto_buyer_order') }} bo
  JOIN {{ source('nto_data', 'nto_buyer_delivered_rebate_band_value') }} bdrbv 
    ON bdrbv.BuyerDeliveredRebateBandValueID = bo.BuyerRebateValueID
  JOIN {{ source('nto_data', 'nto_buyer_delivered_rebate_band_set') }} bdrbs 
    ON bdrbs.BuyerDeliveredRebateBandSetID = bdrbv.BuyerDeliveredRebateBandSetID
  JOIN {{ source('nto_data', 'nto_sales_agent_commission_rate') }} sacr 
    ON sacr.BuyerRebateID = bdrbs.BuyerDeliveredRebateID 
    AND (
      bdrbs.EffectiveFrom BETWEEN sacr.EffectiveFrom 
      AND DATETIME_SUB(CAST(DATE(DATETIME_ADD(sacr.EffectiveTo, INTERVAL 1 HOUR)) AS DATETIME), INTERVAL 1 SECOND)
      OR 
      (
        sacr.EffectiveTo = CAST('1900-01-01' AS DATETIME)
        AND bdrbs.EffectiveFrom >= CAST(DATE(DATETIME_ADD(sacr.EffectiveFrom, INTERVAL 1 HOUR)) AS DATETIME)
      )
    )
  JOIN {{ source('nto_data', 'nto_buyer_collected_rebate_value') }} bcrv2 
    ON bcrv2.BuyerCollectedRebateID = sacr.VariableRateSalesAgentRebateID 
    AND (
      sacr.EffectiveFrom BETWEEN bcrv2.EffectiveFrom AND bcrv2.EffectiveTo 
      OR (bcrv2.EffectiveTo = CAST('1900-01-01' AS DATETIME) AND sacr.EffectiveFrom >= bcrv2.EffectiveFrom)
    )
  WHERE bo.IsForDelivery = true
),
delivery_rebate_prices AS (
  -- Filter to ensure 1:1 relationship per BuyerOrderID
  SELECT 
    BuyerOrderID,
    PurchasePrice
  FROM delivery_rebate_prices_raw
  WHERE rn = 1
)

SELECT
  bo.OrderNumberTitle AS BuyerOrder,
  bo.CustomerTitle AS Buyer,
  bo.StatusID,
  bo.StatusTitle AS StatusTitle,
  bo.FinalLitres AS Litres,
  bo.InvoiceDate AS InvoiceDate,
  bo.PlacedDate AS PlacedDate,
  bo.ProductTitle AS Product,
  CASE WHEN bo.DeliveryDepotTitle is null THEN bo.DepotTitle ELSE rl.SupplierDepot END AS Depot,
  bo.DeliveryDepotTitle AS DeliveryPoint,
  rl.LoadNumberTitle AS LoadNumber,
  transporter.TransporterTitle AS Transporter,
  rl.SupplierOrder AS SupplierOrder,
  rl.SupplierRef1 AS SupplierRef1,
  rl.SupplierRef2 AS SupplierRef2,
  rl.LinkedSupplyDeal AS LinkedSupplyDeal,
  CASE WHEN bo.DeliveryDepotTitle is null THEN 'Collection' ELSE 'Delivery' END AS OrderType,
  customer.CustomerCategory AS BuyerCategory,
  CASE WHEN pd.PaymentDate IS NOT NULL THEN TRUE ELSE FALSE END AS OrderPaid,
  bo.CustomerReference AS POD,
  bo.ModifiedDate AS ModifiedDate,
  -- Purchase_Price -- Calculated from rebate data with WMS commission fallback
  ROUND(
    COALESCE(
      collection_rebate_prices.PurchasePrice,
      delivery_rebate_prices.PurchasePrice,
      CASE 
        WHEN wms.CommissionWMS IS NOT NULL AND bo.FinalLitres > 0 
        THEN bo.BuyerPrice - (wms.CommissionWMS / bo.FinalLitres)
        ELSE 0 
      END
    ), 4
  ) AS PurchasePrice,
  ROUND(
    COALESCE(
      collection_rebate_prices.PurchasePrice,
      delivery_rebate_prices.PurchasePrice,
      CASE 
        WHEN wms.CommissionWMS IS NOT NULL AND bo.FinalLitres > 0 
        THEN bo.BuyerPrice - (wms.CommissionWMS / bo.FinalLitres)
        ELSE 0 
      END
    ), 4
  ) AS PurchasePriceNumeric,
  ROUND(bo.BuyerPrice, 4) AS SellingPrice,
  bo.BuyerPrice AS SellingPriceNumeric,
  ROUND(bo.FinalLitres * bo.BuyerPrice, 2) AS InvoiceAmount,
  ROUND(wms.InvoiceAmountWMS, 2) AS InvoiceAmountWMS,
  ROUND(wms.CommissionWMS, 2) AS CommissionWMS,
  ROUND(
    CASE 
      WHEN wms.CommissionWMS IS NOT NULL AND bo.FinalLitres > 0 THEN
        (
          wms.CommissionWMS
        )
      ELSE 0
    END, 2
  ) AS Commission,
  -- Commission calculation (commented out as PurchasePrice field may not exist) (bo.BuyerPrice - bo.PurchasePrice) * bo.FinalLitres AS Commission
  pd.PaymentDate AS PaymentDate,
  ROUND(
    CASE 
      WHEN wms.InterestWMS = 0 OR wms.InterestWMS IS NULL THEN 0
      WHEN pd.PaymentDate IS NOT NULL THEN
        (
          (
            (
              bo.FinalLitres * bo.BuyerPrice
            ) * (
              DATE_DIFF(pd.PaymentDate, bo.InvoiceDate, DAY)
            )
          ) / 365
        ) * 0.10
      ELSE 0
    END, 2
  ) AS Interest,
  ROUND(wms.InterestWMS, 2) AS InterestWMS,
  bo.DeliveryDepotTitle AS DeliveryDepot,
  bo.CapturedDate AS CapturedDate,
  (CASE WHEN bo.buyerOrderPricingModelTitle = 'Own Stock Purchase' THEN 'Stock delivery' ELSE 'Stock allocation' END) AS StockType

FROM {{ source('nto_data', 'nto_buyer_order') }} bo
LEFT JOIN {{ ref('vw_nto_payment_dates') }} pd
  ON bo.BuyerOrderID = pd.BuyerOrderID
LEFT JOIN wms_data wms
  ON bo.BuyerOrderID = wms.BuyerOrderID
LEFT JOIN ranked_loads rl
  ON bo.BuyerOrderID = rl.BuyerOrderID AND rl.rn = 1  -- Only take the first ranked load per buyer order
LEFT JOIN collection_rebate_prices
  ON bo.BuyerOrderID = collection_rebate_prices.BuyerOrderID
LEFT JOIN delivery_rebate_prices
  ON bo.BuyerOrderID = delivery_rebate_prices.BuyerOrderID
JOIN {{ source('nto_data', 'nto_customer') }} customer
  ON bo.CustomerID = customer.CustomerID
LEFT JOIN {{ source('nto_data', 'nto_transporter') }} transporter
  ON rl.TransporterID = transporter.TransporterID