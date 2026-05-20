CREATE TABLE [dbo].[FactTaxiDaily] (

	[date_key] int NOT NULL, 
	[pickup_date] date NOT NULL, 
	[borough] varchar(50) NOT NULL, 
	[trip_count] bigint NOT NULL, 
	[total_revenue_usd] float NULL, 
	[avg_revenue_per_trip_usd] float NULL, 
	[avg_fare_usd] float NULL, 
	[avg_trip_distance] float NULL, 
	[avg_trip_duration_min] float NULL
);