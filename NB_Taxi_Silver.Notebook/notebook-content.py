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

from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, TimestampType
from pyspark.sql import functions as F
from functools import reduce

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_columns = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "RatecodeID": "double",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "long",
    "fare_amount": "double",
    "extra": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "cbd_congestion_fee": "double"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

paths = [
    f"Files/bronze/taxi/yellow/yellow_tripdata_{year}-{month:02d}.parquet"
    for year in range(2022, 2027)
    for month in range(1, 13)
    if not (year == 2026 and month > 3)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def standardize_parquet(path):
    df = spark.read.parquet(path)

    if "Airport_fee" in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")

    for c, dtype in target_columns.items():
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(dtype))
        else:
            df = df.withColumn(c, F.lit(None).cast(dtype))
        
    return df.select([F.col(c) for c in target_columns.keys()])


dfs = []
for p in paths:
    try:
        dfs.append(standardize_parquet(p))
    except Exception as e:
        print(f"Path: {p}. {e}")

df_raw = reduce(lambda a, b: a.unionByName(b), dfs)
total_raw = df_raw.count()
print(f"Total ingested row count: {total_raw}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_raw = df_raw.withColumn(
    "cbd_congestion_fee", 
    F.coalesce(F.col("cbd_congestion_fee"), F.lit(0.0)))

df_clean = (df_raw
    .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "total_amount"])
    .filter(F.year("tpep_pickup_datetime").between(2022, 2026))
    .filter(F.col("total_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("passenger_count") > 0))

total_clean = df_clean.count()
print(total_raw - total_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

quantiles = df_clean.approxQuantile(
    ["trip_distance", "total_amount", "passenger_count"],
    [0.01, 0.05, 0.1, 0.5, 0.95, 0.99],
    0.01
)

quantiles

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean = (df_clean
    .filter(F.col("total_amount").between(3, 300))
    .filter(F.col("trip_distance").between(0.1, 100)))

df_enriched = (df_clean
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
    .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    .withColumn("pickup_dow", F.dayofweek("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("trip_duration_min", 
                F.round((F.unix_timestamp("tpep_dropoff_datetime") 
                - F.unix_timestamp("tpep_pickup_datetime")) / 60.0, 2))
    .filter(F.col("trip_duration_min").between(1, 240))
    .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
    )

print(f"Enriched rows count: ", df_enriched.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enriched.write.format("delta").mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month").saveAsTable("silver_nyc_taxi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT pickup_year, pickup_month,
           COUNT(*) as trips
    FROM silver_nyc_taxi
    GROUP BY 1, 2
    ORDER BY 1, 2
""").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
