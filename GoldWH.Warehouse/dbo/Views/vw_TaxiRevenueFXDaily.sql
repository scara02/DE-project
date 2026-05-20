-- Auto Generated (Do not modify) 68367BF2837FE44A2C5A1E8FAC8A01D016E69A40B22EF6CB9F194C759833DBDF
CREATE   VIEW vw_TaxiRevenueFXDaily AS
SELECT
    t.date_key,
    d.full_date,
    d.year,
    d.month,
    d.month_name,

    t.borough,
    t.trip_count,

    t.total_revenue_usd,
    t.avg_revenue_per_trip_usd,

    fx.usd_eur,

    t.total_revenue_usd * fx.usd_eur AS total_revenue_eur,
    t.avg_revenue_per_trip_usd * fx.usd_eur AS avg_revenue_per_trip_eur
FROM FactTaxiDaily t
JOIN DimDate d
    ON t.date_key = d.date_key
LEFT JOIN DimFX fx
    ON t.date_key = fx.date_key;