{{ config(
    materialized='view'
) }}

SELECT
    -- Select columns from the ops table
    ptd.date
    ,ptd.Customer
    ,ptd.FullNumber
    ,ptd.SupplySiteID
    ,ptd.DepotName
    ,ptd.LitreAmount
    ,ptd.TransactionAmount
    ,ptd.Region
    ,ptd.VehicleID
    ,ptd.Registration
    ,ptd.ProductTitle
    ,ptd.ProductCategory
    ,ptd.TransactionID
    ,ptd.SupplierDiscountPerLitre
    ,ptd.TFNDiscountPerLitre

    -- Select columns from the monday table
    ,di.PAX
    ,di.Operating_License
    ,di.Supplier_Manager
    ,di.Grid_Zone
    ,di.Price_Structure
    ,di.Live_Location
    ,di.GPS
    ,di.Depot_Type
    ,di.TFN_depot_number
    ,di.routes
    ,di.Promotions

    -- Additional columns from fuel transactions and related tables
    ,ci.ConsignmentPrice
    ,ci.NetLitres AS ConsignmentLitres
    ,ca.isAuthorised
    ,ca.IsManual
    ,ft.Reversed
    ,CASE
        WHEN olt.TransactionID IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS isOrder
    ,ft.GridPrice
    ,ftd.SitePumpPrice AS PumpPrice
    ,CASE
        WHEN ptd.LitreAmount > 0 THEN ROUND(ptd.TransactionAmount / ptd.LitreAmount, 3)
        ELSE NULL
    END AS TFNPricing


FROM
    {{ source('tfn_ops_dashboards', 'precalculated_transaction_data_partitioned') }} AS ptd
JOIN
    {{ source('tfn_monday', 'depot_info') }} AS di
ON
    ptd.SupplySiteID = di.supply_site_id
JOIN
    {{ source('tfn_reports', 'fuel_transactions_all_time') }} AS ft
ON
    ptd.TransactionID = ft.FuelTransactionID
JOIN
    {{ source('tfn_data', 'tfn_pricing_2022_transaction') }} AS pricing
ON
    ft.FuelTransactionId = pricing.FuelTransactionID
LEFT JOIN
    {{ source('tfn_data', 'tfn_pricing_2022_consignmnent_invoice') }} AS ci
ON
    pricing.Pricing2022ConsignmentInvoiceID = ci.Pricing2022ConsignmentInvoiceID
LEFT JOIN
    {{ source('tfn_data', 'tfn_fuel_transaction_discount_all_time') }} AS ftd
ON
    ftd.FuelTransactionID = ft.FuelTransactionID
LEFT JOIN (
    SELECT
        CardAuthorisationID,
        isAuthorised,
        IsManual,
        ROW_NUMBER() OVER (PARTITION BY CardAuthorisationID ORDER BY CardAuthorisationID) as rn
    FROM {{ source('tfn_data_historical', 'card_authorisations') }}
) AS ca
ON
    ca.CardAuthorisationID = ft.CardAuthorisationID
    AND ca.rn = 1
LEFT JOIN (
    SELECT
        TransactionID,
        ROW_NUMBER() OVER (PARTITION BY TransactionID ORDER BY TransactionID) as rn
    FROM {{ source('tfn_data', 'order_instance_item_linked_transaction') }}
) AS olt
ON
    olt.TransactionID = ptd.TransactionID
    AND olt.rn = 1
