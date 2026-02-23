{{
  config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

-- CTEs from your original query
WITH CustomerProfile AS (
  SELECT
    Account_Number AS account,
    Internal_ID,
    NULLIF(`Account_Manager`, '') AS AccountManager
  FROM {{ source('tfn_monday', 'customer_profile') }}
  WHERE NULLIF(`Account_Manager`, '') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Account_Number ORDER BY Account_Number) = 1
),

card_auth AS (
  SELECT
    caat.CapturedDate,
    caat.VehicleID,
    v.Registration,
    v.CustomerID,
    caat.SupplySiteID,
    caat.CardAuthorisationID
  FROM {{ source('tfn_reports', 'card_authorisations_all_time') }} AS caat
  LEFT JOIN {{ source('tfn_data', 'tfn_live_vehicles') }} AS v
    ON caat.VehicleID = v.VehicleID
),

deduplicated_ncp AS (
  -- This is the original subquery structure
  SELECT *
  FROM (
    SELECT
      AS_VALUE.*,
      ROW_NUMBER() OVER (PARTITION BY Account_Number ORDER BY Lead_Capture_date DESC) AS row_num
    FROM {{ source('tfn_monday', 'new_customer_program') }} AS AS_VALUE
  )
  WHERE
    row_num = 1
),

promotions_deduplicated AS (
  SELECT
    CapturedDate,
    SupplySiteID,
    ProductID,
    SUM(SupplierDiscountPerLitre) AS SupplierDiscountPerLitre,
    SUM(TFNDiscountPerLitre) AS TFNDiscountPerLitre
  FROM {{ source('tfn_ops_dashboards', 'tfn_promotions_at_depot') }}
  GROUP BY 1, 2, 3
)

-- Main SELECT statement, reverted to original logic
SELECT
  CAST(FORMAT_DATE('%Y-%m-%d', T.CapturedDate) AS Date) AS date,
  T.CustomerID,
  T.TFNCustomerID,
  T.SupplySiteID,
  C.Title AS Customer,
  C.FullNumber,
  C.ServiceProvider,
  E.AccountManager,
  T.LitreAmount,
  T.Amount AS TransactionAmount,
  T.DepotName,
  CASE WHEN D.FunderTitle IS NULL THEN 'Self-Funded' ELSE D.FunderTitle END AS FunderType,
  B.Region,
  B.Status,
  CA.VehicleID,
  V.Registration,
  P.Title AS ProductTitle,
  PC.Title AS ProductCategory,
  F.Title AS CustomerRegion,
  T.TransactionID,
  G.Name AS AgentName,
  N.AM AS NCP_AM,
  N.Date_Active,
  CASE WHEN N.Account_Number IS NOT NULL THEN TRUE ELSE FALSE END AS NCP,
  CASE WHEN N.Date_Active >= '2025-03-01' THEN TRUE ELSE FALSE END AS NewBusiness,
  pat.SupplierDiscountPerLitre,
  pat.TFNDiscountPerLitre
FROM {{ source('tfn_reports', 'tfn_all_transactions') }} AS T
LEFT JOIN {{ source('tfn_monday', 'depot_info') }} AS B ON T.SupplySiteID = B.supply_site_id
LEFT JOIN {{ source('tfn_data', 'tfn_demo_customer') }} AS C ON T.TFNCustomerID = C.CustomerID
LEFT JOIN CustomerProfile AS E ON C.FullNumber = E.account
LEFT JOIN {{ source('tfn_data', 'tfn_funder') }} AS D ON T.FunderID = D.FunderID
LEFT JOIN card_auth AS CA ON T.CardAuthorisationID = CA.CardAuthorisationID
LEFT JOIN {{ source('tfn_data', 'tfn_live_vehicles') }} AS V ON CA.VehicleID = V.VehicleID
LEFT JOIN {{ source('tfn_data', 'tfn_product') }} AS P ON P.ProductID = T.ProductID
LEFT JOIN {{ source('tfn_data', 'tfn_product_category') }} AS PC ON PC.ProductCategoryID = P.ProductCategoryID
LEFT JOIN {{ source('tfn_data', 'tfn_area') }} AS F ON C.AreaID = F.AreaID
LEFT JOIN {{ source('tfn_data', 'tfn_agent') }} AS G ON T.AgentID = G.AgentID
LEFT JOIN deduplicated_ncp AS N ON C.FullNumber = N.Account_Number
LEFT JOIN promotions_deduplicated AS pat ON CAST(FORMAT_DATE('%Y-%m-%d', T.CapturedDate) AS Date) = pat.CapturedDate AND T.SupplySiteID = pat.SupplySiteID AND T.ProductID = pat.ProductID
