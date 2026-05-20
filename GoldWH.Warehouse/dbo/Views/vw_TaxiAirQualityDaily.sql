-- Auto Generated (Do not modify) 670B882669697C60E7A595F91ABB22D0D78D3CCD2DDB9A26D82F31F111B29D7D
CREATE   VIEW vw_TaxiAirQualityDaily AS
SELECT
    t.date_key,
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,

    t.borough,
    t.trip_count,
    t.total_revenue_usd,
    t.avg_revenue_per_trip_usd,
    t.avg_fare_usd,
    t.avg_trip_distance,
    t.avg_trip_duration_min,

    a.pollutant,
    a.avg_pollutant_value,
    a.min_pollutant_value,
    a.max_pollutant_value,
    a.measurement_count
FROM FactTaxiDaily t
JOIN FactAirQualityDaily a
    ON t.date_key = a.date_key
   AND t.borough = a.borough
JOIN DimDate d
    ON t.date_key = d.date_key;