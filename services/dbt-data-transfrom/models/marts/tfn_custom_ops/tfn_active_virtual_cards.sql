{{ config(
    materialized='view',
    schema='tfn_custom_ops',
    unique_key=['DriverID', 'ExpiryDate', 'CapturedDate'],
    partition_by={
        "field": "CapturedDate",
        "data_type": "datetime",
        "granularity": "month"
    }
) }}
WITH DriverLinkedCodes AS (
  SELECT
    cac.DriverID,
    cac.ExpiryDate,
    CAST(NULL AS STRING) AS VehicleID,
    CAST(NULL AS STRING) AS CardID,
    CAST(NULL AS STRING) AS CustomerID,
    cac.Code AS CurrentVirtualCardNumber,
    'Driver' AS LinkType,
    ROW_NUMBER() OVER (PARTITION BY cac.DriverID ORDER BY cac.ExpiryDate DESC, cac.CapturedDate DESC) as rn
  FROM
    {{ source('tfn_data', 'customer_authorisation_code') }} cac
  WHERE
    cac.ExpiryDate > DATETIME(CURRENT_TIMESTAMP())
    AND cac.CapturedDate < DATETIME(CURRENT_TIMESTAMP())
  QUALIFY
    rn = 1
),
CardLinkedCodes AS (
  WITH RankedAuthorisations AS (
  SELECT
    caat.VehicleID,
    cac.Code AS CardCode,
    caat.CardID,
    CAST(cac.CustomerID AS STRING) AS CustomerID,
    cac.CapturedDate,
    caat.IsDeleted,
    cac.ExpiryDate,
    ROW_NUMBER() OVER (
      PARTITION BY caat.VehicleID
      ORDER BY cac.CapturedDate DESC, cac.ExpiryDate DESC
    ) AS rn
  FROM
    {{ source('tfn_reports', 'card_authorisations_all_time') }} caat
  INNER JOIN
    {{ source('tfn_data', 'customer_authorisation_code') }} cac
    ON caat.CardID = cac.CardID
  WHERE
    cac.ExpiryDate > DATETIME(CURRENT_TIMESTAMP())
    AND cac.CapturedDate < DATETIME(CURRENT_TIMESTAMP())
  
)
SELECT
  VehicleID,
  CardCode AS CurrentVirtualCardNumber,
  CardID,
  CustomerID,
  ExpiryDate,
  CAST(NULL AS STRING) AS DriverID,
  'Card_Vehicle' AS LinkType
FROM
  RankedAuthorisations
WHERE
  rn = 1
),
AllActiveCodes AS (
  SELECT
    DriverID,
    VehicleID,
    ExpiryDate,
    CardID,
    CustomerID,
    CurrentVirtualCardNumber,
    LinkType
  FROM DriverLinkedCodes
  UNION ALL
  SELECT
    DriverID,
    VehicleID,
    ExpiryDate,
    CardID,
    CustomerID,
    CurrentVirtualCardNumber,
    LinkType
  FROM CardLinkedCodes
)
SELECT * FROM AllActiveCodes 
