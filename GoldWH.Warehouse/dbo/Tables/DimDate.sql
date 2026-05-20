CREATE TABLE [dbo].[DimDate] (

	[date_key] int NOT NULL, 
	[full_date] date NOT NULL, 
	[year] int NOT NULL, 
	[month] int NOT NULL, 
	[day] int NOT NULL, 
	[quarter] int NOT NULL, 
	[day_of_week] int NOT NULL, 
	[month_name] varchar(20) NOT NULL, 
	[day_name] varchar(20) NOT NULL, 
	[is_weekend] bit NOT NULL
);