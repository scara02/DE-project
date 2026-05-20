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

df_gdp = (spark.sql("SELECT * FROM bronze_gdp")
        .dropna(subset=["year", "gdp_usd"])
        .withColumn("gdp_usd", F.col("gdp_usd").cast("double")))

df_gdp.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_gdp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecb = (spark.sql("SELECT * FROM bronze_ecb")
        .dropna(subset=["fx_date", "usd_eur"])
        .withColumn("fx_date", F.to_date("fx_date"))
        .withColumn("usd_eur", F.col("usd_eur").cast("double")))

df_ecb.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_ecb")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
