{{ config(
    materialized='view',
    schema='tfn_data'
) }}

SELECT
    CustomerName,
    v.TFNCustomerID,
    v.CustomerID,
    v.Registration,
    vh.VIN,
    v.ConfiguredFuelTankSize,
    v.CurrentVehicleStatus,
    v.CalculatedFuelTankSize,
    v.AverageTransactionLitres,
    v.VehicleID,
    CASE WHEN v.CalculatedFuelTankSize > v.ConfiguredFuelTankSize THEN v.CalculatedFuelTankSize 
    ELSE v.ConfiguredFuelTankSize END AS MaxObservedCapacity
FROM
    {{ source('tfn_data', 'vehicle') }} AS vh
JOIN
    {{ source('tfn_data', 'vbi_vehicle_tank_size') }} AS v
ON
    v.VehicleID = vh.VehicleID