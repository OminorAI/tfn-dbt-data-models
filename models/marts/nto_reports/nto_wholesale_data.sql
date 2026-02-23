{{ config(
  materialized='view'
  -- TODO: Add a schema name
) }}

WITH buyer_order_litres AS (
  -- Part 1: Buyer Order litres
  SELECT
    deal.AdhocFixedVolumeDealID,
    bo.CapturedDate,
    CASE 
      WHEN bo.FinalLitres = 0 THEN bo.RequestedLitres 
      ELSE bo.FinalLitres 
    END AS BOLitres,
    0 AS SOLitres
  FROM {{ source('nto_data', 'nto_adhoc_fixed_volume_deal') }} deal
  JOIN {{ source('nto_data', 'nto_buyer_order') }} bo 
    ON bo.AdhocFixedVolumeDealID = deal.AdhocFixedVolumeDealID
  JOIN {{ source('nto_data', 'nto_status') }} s 
    ON s.StatusID = bo.StatusID 
    AND s.StatusItemID <> '00000000-E0CE-4A53-9751-BD24654B0622' -- Cancelled
),

supplier_order_litres AS (
  -- Part 2: Supplier Order litres
  SELECT
    deal.AdhocFixedVolumeDealID,
    so.CapturedDate,
    0 AS BOLitres,
    CASE 
      WHEN so.FinalLitres = 0 THEN 
        CASE 
          WHEN so.UpliftedLitres = 0 THEN so.RequestedLitres
          ELSE so.UpliftedLitres
        END
      ELSE so.FinalLitres
    END AS SOLitres
  FROM {{ source('nto_data', 'nto_adhoc_fixed_volume_deal') }} deal
  JOIN {{ source('nto_data', 'nto_supplier_order') }} so 
    ON so.AdhocFixedVolumeDealID = deal.AdhocFixedVolumeDealID
  JOIN {{ source('nto_data', 'nto_status') }} s 
    ON s.StatusID = so.StatusID 
    AND s.StatusItemID <> '00000000-28A0-445C-8608-D7FD1F961F8A' -- Cancelled
),

combined_litres AS (
  -- Combine buyer and supplier order litres
  SELECT * FROM buyer_order_litres
  UNION ALL
  SELECT * FROM supplier_order_litres
),

aggregated_litres AS (
  -- Aggregate litres by deal
  SELECT
    AdhocFixedVolumeDealID,
    SUM(BOLitres) AS BOLitres,
    SUM(SOLitres) AS SOLitres
  FROM combined_litres
  GROUP BY AdhocFixedVolumeDealID
)

SELECT
  deal.DealIdentifier,
  deal.AdhocFixedVolumeDealID,
  p.Title AS Product,
  d.DepotTitle AS Depot,
  deal.Volume,
  agg.BOLitres AS SoldLitres,
  agg.SOLitres AS PurchasedLitres,
  deal.AvailableFromDate,
  deal.StartDate,
  deal.EndDate,
  deal.Rebate, -- These can get changed during the span of the deal
  deal.FinalSupplierPrice, -- These can get changed during the span of the deal
  deal.InitialSupplierNetPrice -- These can get changed during the span of the deal
FROM aggregated_litres agg
JOIN {{ source('nto_data', 'nto_adhoc_fixed_volume_deal') }} deal 
  ON deal.AdhocFixedVolumeDealID = agg.AdhocFixedVolumeDealID
JOIN {{ source('nto_data', 'nto_product') }} p 
  ON p.ProductID = deal.ProductID
JOIN {{ source('nto_data', 'nto_adhoc_fixed_volume_deal_type') }} deal_type 
  ON deal_type.AdhocFixedVolumeDealTypeID = deal.AdhocFixedVolumeDealTypeID
JOIN {{ source('nto_data', 'nto_depot') }} d 
  ON d.DepotID = deal.DepotID

