{{ config(
    materialized='incremental',
    schema='tfn_custom_ops',
    unique_key=['OrderNumberTitle', 'DriverID', 'CapturedDate', 'VehicleID', 'CardID', 'CustomerName'],
    partition_by={
        "field": "CapturedDate",
        "data_type": "datetime",
        "granularity": "month"
    }
) }}

WITH ranked_orders AS (
    SELECT
        o.OrderNumberTitle,
        oii.DriverID,
        o.CapturedDate,
        oii.CardID,
        tdc.FullNumber,
        di.Name AS DepotName,
        tdc.Title AS CustomerName,
        oii.IsDeleted,
        o.VehicleID,
        o.CustomerNumber,
        o.SupplySiteNumber AS SupplierNumber,
        o.ProductCode,
        o.Registration AS VehicleRegistration,
        o.MaxLitres,
        o.MaxAmount,
        o.ValidDateStart,
        o.ValidDateEnd,
        ROW_NUMBER() OVER (
            PARTITION BY o.OrderNumberTitle, oii.DriverID, o.CapturedDate, o.VehicleID, oii.CardID, tdc.Title
            ORDER BY o.CapturedDate DESC
        ) as row_num
    FROM
        {{ source('tfn_reports', 'orders') }} o
    LEFT JOIN
        {{ source('tfn_data', 'order_instance_item') }} oii
        ON o.OrderInstanceID = oii.OrderInstanceID
        AND o.VehicleID = oii.VehicleID
    LEFT JOIN {{ source('tfn_data', 'tfn_live_vehicles') }} tlv
        ON o.VehicleID = tlv.VehicleID
    LEFT JOIN {{ source('tfn_data', 'tfn_demo_customer') }} tdc
        ON tlv.TFNCustomerID = tdc.CustomerID
    LEFT JOIN {{ source('tfn_monday', 'depot_info') }} di
        ON o.SupplySiteID = di.supply_site_id
    WHERE o.CapturedDate >= '2025-01-01'
    {% if is_incremental() %}
        AND o.CapturedDate >= (SELECT MAX(CapturedDate) FROM {{ this }})
    {% endif %}
)

SELECT
    OrderNumberTitle,
    DriverID,
    CapturedDate,
    CardID,
    FullNumber,
    DepotName,
    CustomerName,
    IsDeleted,
    VehicleID,
    CustomerNumber,
    SupplierNumber,
    ProductCode,
    VehicleRegistration,
    MaxLitres,
    MaxAmount,
    ValidDateStart,
    ValidDateEnd
FROM ranked_orders
WHERE row_num = 1
