CREATE TABLE [dbo].[FactTaxiZoneDaily] (

	[date_key] int NOT NULL, 
	[pickup_date] date NOT NULL, 
	[zone_id] int NOT NULL, 
	[borough] varchar(20) NULL, 
	[zone_name] varchar(100) NULL, 
	[trip_count] bigint NOT NULL, 
	[total_revenue_usd] float NULL, 
	[avg_revenue_per_trip_usd] float NULL, 
	[avg_fare_usd] float NULL, 
	[avg_trip_distance] float NULL, 
	[avg_trip_duration_min] float NULL
);