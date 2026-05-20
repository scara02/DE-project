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

# CELL ********************

df_raw = spark.sql("SELECT * FROM bronze_openaq_hourly")
total_raw = df_raw.count()
print(f"Total raw count: {total_raw}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean = (df_raw
    .dropDuplicates(["sensor_id", "datetime", "parameter"])
    .dropna(subset=["datetime", "value"])
    .filter(F.col("value") >= 0))

print(f"Clean rows: {df_clean.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_openaq_borough_map = spark.createDataFrame([
    (648, "Brooklyn"),
    (664, "Brooklyn"),
    (626, "Bronx"),
    (665, "Bronx"),
    (384, "Manhattan"),
    (625, "Manhattan"),
    (628, "Queens"),
    (631, "Queens")
], ["location_id", "borough"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enriched = (df_clean
    .withColumnRenamed("parameter", "pollutant")
    .withColumn("datetime", F.to_timestamp("datetime"))
    .withColumn("date", F.to_date("datetime"))
    .withColumn("year", F.year("datetime"))
    .withColumn("month", F.month("datetime"))
    .withColumn("hour", F.hour("datetime"))
    .withColumn("dow", F.dayofweek("datetime"))
    .join(df_openaq_borough_map, "location_id", "left")
    .filter(F.col("borough").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enriched.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month").saveAsTable("silver_openaq_hourly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_locations = (df_enriched
    .select("location_id", "borough")
    .drop_duplicates(["location_id"]))

df_locations.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("silver_openaq_locations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
