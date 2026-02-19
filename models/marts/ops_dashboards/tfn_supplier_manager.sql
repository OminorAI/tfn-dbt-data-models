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
    ,di.Supplier_Manager AS DepotManager
    ,di.Grid_Zone
    ,di.Price_Structure
    ,di.Live_Location
    ,di.GPS
    ,di.Depot_Type
    ,di.TFN_depot_number
    ,di.routes
    ,di.Promotions

FROM
    {{ source('tfn_ops_dashboards', 'precalculated_transaction_data_partitioned') }} AS ptd
JOIN
    {{ source('tfn_monday', 'depot_info') }} AS di
ON
    ptd.SupplySiteID = di.supply_site_id