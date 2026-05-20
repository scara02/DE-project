# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7bc6df61-fa1d-4d3d-867d-e9df41248645",
# META       "default_lakehouse_name": "LH_UrbanAnalytics",
# META       "default_lakehouse_workspace_id": "5d2135e8-4491-4ded-a6fa-a0347daedcc8",
# META       "known_lakehouses": [
# META         {
# META           "id": "7bc6df61-fa1d-4d3d-867d-e9df41248645"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Taxi data

# CELL ********************

taxi = spark.read.table("silver_nyc_taxi")
zones = spark.read.table("silver_taxi_zones")
openaq_boroughs = spark.read.table("silver_openaq_locations").select("borough").dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_with_boroughs = (taxi  
    .join(zones.select(
        F.col("zone_id").alias("PULocationID"),
        F.col("borough")
    ), "PULocationID", "inner"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stg_fact_taxi_daily = (taxi_with_boroughs
    .groupBy("pickup_date", "borough")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue_usd"),
        F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip_usd"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_trip_duration_min")
    )
    .withColumn("date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int"))
    .select(
        "date_key",
        "pickup_date",
        "borough",
        "zone"
        "trip_count",
        "total_revenue_usd",
        "avg_revenue_per_trip_usd",
        "avg_fare_usd",
        "avg_trip_distance",
        "avg_trip_duration_min"
    ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stg_fact_taxi_daily.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("stg_fact_taxi_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_with_zones = (taxi
    .join(
        zones.select(
            F.col("zone_id").alias("PULocationID"),
            F.col("zone_id"),
            F.col("borough"),
            F.col("zone_name")
        ),
        "PULocationID",
        "inner"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stg_fact_taxi_zone_daily = (taxi_with_zones
    .groupBy(
        "pickup_date",
        "zone_id",
        "borough",
        "zone_name"
    )
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue_usd"),
        F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip_usd"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_trip_duration_min")
    )
    .withColumn("date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int"))
    .select(
        "date_key",
        "pickup_date",
        "zone_id",
        "borough",
        "zone_name",
        "trip_count",
        "total_revenue_usd",
        "avg_revenue_per_trip_usd",
        "avg_fare_usd",
        "avg_trip_distance",
        "avg_trip_duration_min"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stg_fact_taxi_zone_daily.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("stg_fact_taxi_zone_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Air quality data

# CELL ********************

air = spark.read.table("silver_openaq_hourly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stg_fact_air_quality_daily = (air
    .groupBy("date", "borough", "pollutant")
    .agg(
        F.round(F.avg("value"), 4).alias("avg_pollutant_value"),
        F.round(F.min("value"), 4).alias("min_pollutant_value"),
        F.round(F.max("value"), 4).alias("max_pollutant_value"),
        F.count("*").alias("measurement_count")
    )
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .select(
        "date_key",
        "date",
        "borough",
        "location_id"
        "pollutant",
        "avg_pollutant_value",
        "min_pollutant_value",
        "max_pollutant_value",
        "measurement_count"
    ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stg_fact_air_quality_daily.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("stg_fact_air_quality_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Taxi daily:", spark.read.table("stg_fact_taxi_daily").count())
print("Air quality daily:", spark.read.table("stg_fact_air_quality_daily").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
