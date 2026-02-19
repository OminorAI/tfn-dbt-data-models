{{ config(
    materialized='view',
    schema='tfn_reports'
) }}

SELECT
      TFNCustomerID,
      CustomerID,
      FuelTransactionID,
      CardAuthorisationID,
      CardAuthorisationCustomerID,
      CardPaymentMachineOwnedByID,
      DeviceID,
      LinkedFunderTransactionEntryID,
      LinkedFunderTransactionEntrySet,
      FunderID,
      CapturedDate,
      GridPrice,
      TransactionAmount,
      LitreAmount,
      DiscountFromSupplier,
      SubAccountTransactionAmount,
      LinkedTransactionEntryID,
      NoSystemEffect,
      IsDeleted,
      ReportingOnly,
      Reversed,
      SupplySiteID,
      PromotionID,
      ProductID,
      AgentID,
      ServiceProviderID
FROM {{ source('tfn_data_historical', 'fuel_transactions') }}
WHERE CapturedDate >= '2018-01-01'
