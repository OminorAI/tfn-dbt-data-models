{{ config(
    materialized='incremental',
    schema='tfn_data_historical',
    unique_key=['Date', 'TFNCustomerID', 'ServiceProviderID', 'CustomerID', 'SupplySiteID'],
    partition_by={
        "field": "Date",
        "data_type": "DATE",
        "granularity": "MONTH"
    },
    cluster_by=['TFNCustomerID', 'ServiceProviderID', 'CustomerID']
) }}

SELECT
  CAST(T.CapturedDate AS DATE) as Date,
  T.TFNCustomerID,
  T.ServiceProviderID,
  CAST(T.CustomerId AS STRING) as CustomerID,
  CAST(T.SupplySiteID AS STRING) as SupplySiteID,
  S.Title as Depot,
  C.Title as Customer,
  SUM(T.LitreAmount) as LitreAmount
FROM {{ source('tfn_data_historical', 'fuel_transactions') }} T
JOIN {{ source('tfn_data', 'supply_site') }} S ON T.SupplySiteID = S.SupplySiteID
JOIN {{ source('tfn_data_historical', 'customer') }} C ON CASE WHEN T.ServiceProviderID <> '00000000-0000-0000-0000-000000000000' THEN T.ServiceProviderID ELSE T.CustomerID END = C.CustomerID
WHERE
  T.Reversed = False
  AND T.IsDeleted = False
  AND T.ReportingOnly = False
  {% if is_incremental() %}
    -- Only process new data during incremental runs
    AND CAST(T.CapturedDate AS DATE) >= (SELECT MAX(Date) FROM {{ this }})
  {% endif %}
GROUP BY
  CAST(T.CapturedDate AS DATE),
  T.TFNCustomerID,
  CASE WHEN T.ServiceProviderID <> '00000000-0000-0000-0000-000000000000' THEN T.ServiceProviderID ELSE T.CustomerID END,
  T.ServiceProviderID,
  T.CustomerId,
  T.SupplySiteID,
  S.Title,
  C.Title
