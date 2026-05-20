CREATE TABLE [dbo].[FactAirQualityDaily] (

	[date_key] int NOT NULL, 
	[date] date NOT NULL, 
	[borough] varchar(50) NOT NULL, 
	[pollutant] varchar(20) NOT NULL, 
	[avg_pollutant_value] float NULL, 
	[min_pollutant_value] float NULL, 
	[max_pollutant_value] float NULL, 
	[measurement_count] bigint NOT NULL
);