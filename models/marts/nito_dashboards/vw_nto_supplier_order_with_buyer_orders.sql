{{ config(
  materialized='view'
  -- TODO: Add a schema name
) }}

SELECT 
  so.OrderNumberTitle AS SupplierOrder,
  so.SupplySiteTitle AS Supplier,
  so.StatusID AS StatusID,
  so.StatusTitle AS StatusTitle,
  so.PlacedDate AS PlacedDate,
  so.RequestedLitres AS Litres, -- Maybe also Lifted Litres or Final Litres
  so.FinalLitres AS FinalLitres, -- Blended with requested litres
  so.SupplierReference AS SupplierRef1,
  so.SupplierReference2 AS SupplierRef2,
  so.ProductTitle AS Product,
  so.DepotTitle AS SupplyDepot,
  afvd.DealIdentifier AS SupplyDeal,
  so.ReleaseNumber AS ReleaseNumber,
  loads.LoadNumberTitle AS LoadNo,
  -- Aggregate buyer orders into an array, filtering out NULLs
  ARRAY_AGG(bo.OrderNumberTitle IGNORE NULLS) AS BuyerOrders,
  -- SplitLoad is True when there is more than one BuyerOrder
  COUNTIF(bo.OrderNumberTitle IS NOT NULL) > 1 AS SplitLoad,
  so.Comment AS InternalNotes,
  so.ModifiedDate AS ModifiedDate
  -- InternalNotes

FROM {{ source('nto_data', 'nto_supplier_order') }} so
-- LEFT JOIN {{ source('nto_data', 'nto_buyer_order_loads') }} bol
--   ON so.LoadID = bol.LoadID
LEFT JOIN {{ source('nto_data', 'nto_loads') }} loads
  ON so.LoadID = loads.LoadID
LEFT JOIN {{ source('nto_data', 'nto_buyer_order') }} bo
  ON so.LoadID = bo.LoadID
  -- ON bol.BuyerOrderID = bo.BuyerOrderID
LEFT JOIN {{ source('nto_data', 'nto_adhoc_fixed_volume_deal') }} afvd
  ON so.AdhocFixedVolumeDealID = afvd.AdhocFixedVolumeDealID
-- JOIN {{ source('nto_data', 'nto_supplier_deal') }} sd
--   ON so.SupplierDealID = sd.SupplierDealID
-- JOIN {{ source('nto_data', 'nto_status') }} status
--   ON so.StatusID = status.StatusID
-- JOIN {{ source('nto_data', 'nto_status') }} status_item
--   ON status.statusItemID = status_item.StatusItemID
-- JOIN statusItem table for status ID

GROUP BY 
  so.OrderNumberTitle,
  so.SupplySiteTitle,
  so.StatusID,
  so.StatusTitle,
  so.PlacedDate,
  so.RequestedLitres,
  so.FinalLitres,
  so.SupplierReference,
  so.SupplierReference2,
  so.ProductTitle,
  so.DepotTitle,
  afvd.DealIdentifier,
  so.ReleaseNumber,
  loads.LoadNumberTitle,
  so.Comment,
  so.ModifiedDate
