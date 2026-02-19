{{ config(
    materialized='view'
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
  --loads.LoadID AS LoadNo,
  bo.OrderNumberTitle AS BuyerOrder,
  so.Comment AS InternalNotes
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
