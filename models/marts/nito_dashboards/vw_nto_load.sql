{{ config(materialized='view') }}

-- NITO Loads

SELECT 
  loads.LoadNumber AS LoadName,
  loads.StatusID AS StatusID,
  loads.StatusTitle AS StatusTitle,
  -- loads.date AS LoadingDate,
  bo.DeliveryDate AS DeliveryDate,
  loads.TransporterID AS TransporterID,
  t.TransporterTitle AS Transporter,
  v.FleetNumber AS Fleet,
  loads.DriverFullName AS Driver,
  so.SupplySiteTitle AS LoadingPoint,
  bo.DeliveryDepotID AS DeliveryPointID,
  bo.OrderNumber AS BuyerOrderNo,
  -- Upliftment
  so.SupplierReference AS SupplierRef,
  so.OrderNumber AS SupplierOrderNumber,
  bo.CustomerReference AS POD
  -- Paid

FROM {{ source('nto_data', 'nto_loads') }} loads
JOIN {{ source('nto_data', 'nto_transporter') }} t
  ON loads.TransporterID = t.TransporterID
-- JOIN {{ source('nto_data', 'nto_load_delivery_update') }} ldu
--   ON loads.LoadID = ldu.LoadID
JOIN {{ source('nto_data', 'nto_supplier_order') }} so
  ON so.LoadID = loads.LoadID
JOIN {{ source('nto_data', 'nto_buyer_order') }} bo
  ON loads.LoadID = bo.LoadID
JOIN {{ source('nto_data', 'nto_vehicle') }} v
  ON v.TransporterID = t.TransporterID
