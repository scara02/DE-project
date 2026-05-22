CREATE   PROCEDURE dbo.sp_refresh_warehouse
AS
BEGIN
    SET NOCOUNT ON;

    -- ============ DIMENSIONS ============

    DELETE FROM dbo.DimDate;

    INSERT INTO dbo.DimDate (
        date_key,
        full_date,
        year,
        month,
        day,
        quarter,
        day_of_week,
        month_name,
        day_name,
        is_weekend
    )
    SELECT 
        CAST(FORMAT(date_value, 'yyyyMMdd') AS INT) AS date_key,
        CAST(date_value AS DATE) AS full_date,
        YEAR(date_value) AS year,
        MONTH(date_value) AS month,
        DAY(date_value) AS day,
        DATEPART(quarter, date_value) AS quarter,
        DATEPART(weekday, date_value) AS day_of_week,
        DATENAME(month, date_value) AS month_name,
        DATENAME(weekday, date_value) AS day_name,
        CASE 
            WHEN DATEPART(weekday, date_value) IN (1, 7) THEN 1 
            ELSE 0 
        END AS is_weekend
    FROM (
        SELECT DATEADD(day, value, '2022-01-01') AS date_value
        FROM GENERATE_SERIES(
            0,
            DATEDIFF(day, '2022-01-01', '2026-12-31')
        )
    ) AS dates;


    DELETE FROM dbo.DimFX;

    INSERT INTO dbo.DimFX (
        date_key,
        full_date,
        usd_eur
    )
    SELECT
        CAST(FORMAT(CAST(fx_date AS DATE), 'yyyyMMdd') AS INT) AS date_key,
        CAST(fx_date AS DATE) AS full_date,
        CAST(usd_eur AS FLOAT) AS usd_eur
    FROM LH_UrbanAnalytics.dbo.silver_ecb
    WHERE fx_date >= '2022-01-01'
      AND fx_date IS NOT NULL
      AND usd_eur IS NOT NULL;


    DELETE FROM dbo.DimGDP;

    INSERT INTO dbo.DimGDP (
        year,
        gdp_usd
    )
    SELECT
        CAST(year AS INT) AS year,
        CAST(gdp_usd AS FLOAT) AS gdp_usd
    FROM LH_UrbanAnalytics.dbo.silver_gdp
    WHERE year >= 2022
      AND year IS NOT NULL
      AND gdp_usd IS NOT NULL;


    DELETE FROM dbo.DimZone;

    INSERT INTO dbo.DimZone (
        zone_id,
        borough,
        zone_name
    )
    SELECT
        CAST(zone_id AS INT) AS zone_id,
        CAST(borough AS VARCHAR(50)) AS borough,
        CAST(zone_name AS VARCHAR(100)) AS zone_name
    FROM LH_UrbanAnalytics.dbo.silver_taxi_zones
    WHERE zone_id IS NOT NULL;


    -- ============ FACTS ============

    DELETE FROM dbo.FactTaxiZoneDaily;

    INSERT INTO dbo.FactTaxiZoneDaily (
        date_key,
        pickup_date,
        zone_id,
        borough,
        zone_name,
        trip_count,
        total_revenue_usd,
        avg_revenue_per_trip_usd,
        avg_fare_usd,
        avg_trip_distance,
        avg_trip_duration_min
    )
    SELECT
        CAST(date_key AS INT) AS date_key,
        CAST(pickup_date AS DATE) AS pickup_date,
        CAST(zone_id AS INT) AS zone_id,
        CAST(borough AS VARCHAR(20)) AS borough,
        CAST(zone_name AS VARCHAR(100)) AS zone_name,
        CAST(trip_count AS BIGINT) AS trip_count,
        CAST(total_revenue_usd AS FLOAT) AS total_revenue_usd,
        CAST(avg_revenue_per_trip_usd AS FLOAT) AS avg_revenue_per_trip_usd,
        CAST(avg_fare_usd AS FLOAT) AS avg_fare_usd,
        CAST(avg_trip_distance AS FLOAT) AS avg_trip_distance,
        CAST(avg_trip_duration_min AS FLOAT) AS avg_trip_duration_min
    FROM LH_UrbanAnalytics.dbo.stg_fact_taxi_zone_daily;


    DELETE FROM dbo.FactTaxiDaily;

    INSERT INTO dbo.FactTaxiDaily (
        date_key,
        pickup_date,
        borough,
        trip_count,
        total_revenue_usd,
        avg_revenue_per_trip_usd,
        avg_fare_usd,
        avg_trip_distance,
        avg_trip_duration_min
    )
    SELECT
        CAST(date_key AS INT) AS date_key,
        CAST(pickup_date AS DATE) AS pickup_date,
        CAST(borough AS VARCHAR(50)) AS borough,
        CAST(trip_count AS BIGINT) AS trip_count,
        CAST(total_revenue_usd AS FLOAT) AS total_revenue_usd,
        CAST(avg_revenue_per_trip_usd AS FLOAT) AS avg_revenue_per_trip_usd,
        CAST(avg_fare_usd AS FLOAT) AS avg_fare_usd,
        CAST(avg_trip_distance AS FLOAT) AS avg_trip_distance,
        CAST(avg_trip_duration_min AS FLOAT) AS avg_trip_duration_min
    FROM LH_UrbanAnalytics.dbo.stg_fact_taxi_daily;


    DELETE FROM dbo.FactAirQualityDaily;

    INSERT INTO dbo.FactAirQualityDaily (
        date_key,
        date,
        borough,
        pollutant,
        avg_pollutant_value,
        min_pollutant_value,
        max_pollutant_value,
        measurement_count
    )
    SELECT
        CAST(date_key AS INT) AS date_key,
        CAST(date AS DATE) AS date,
        CAST(borough AS VARCHAR(50)) AS borough,
        CAST(pollutant AS VARCHAR(20)) AS pollutant,
        CAST(avg_pollutant_value AS FLOAT) AS avg_pollutant_value,
        CAST(min_pollutant_value AS FLOAT) AS min_pollutant_value,
        CAST(max_pollutant_value AS FLOAT) AS max_pollutant_value,
        CAST(measurement_count AS BIGINT) AS measurement_count
    FROM LH_UrbanAnalytics.dbo.stg_fact_air_quality_daily;


    -- ============ BOROUGH DIMENSION ============

    DELETE FROM dbo.DimBorough;

    INSERT INTO dbo.DimBorough (
        borough
    )
    SELECT DISTINCT
        CAST(borough AS VARCHAR(20)) AS borough
    FROM dbo.FactAirQualityDaily
    WHERE borough IS NOT NULL;

END;