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

import requests
import gzip
import io
import csv
from datetime import date, datetime, timedelta
from pyspark.sql import Row
from delta.tables import DeltaTable
from collections import defaultdict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

NYC_LOCATION_IDS = [384, 625, 626, 628, 631, 648, 664, 665]
POLLUTANTS = {'pm25', 'no2', 'o3', 'co', 'pm10'}
S3_BASE = "https://openaq-data-archive.s3.amazonaws.com/records/csv.gz"

TABLE_NAME = "bronze_openaq_hourly"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    last_date = spark.sql(f"""
        SELECT MAX(datetime) as last_date
        FROM {TABLE_NAME}
        """).collect()[0].last_date
except:
    last_date = None

if last_date is None:
    date_from = date(2022, 1, 1)
else:
    date_from = datetime.strptime(str(last_date)[:10], "%Y-%m-%d").date() + timedelta(days=1)

date_to = date.today().replace(day=1) - timedelta(days=1)

if date_from > date_to:
    print("Already up to date.")
    date_from = None
else:
    print(f"Fetching: {date_from} → {date_to}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_location_day(lid, dt):
    url = (f"{S3_BASE}/locationid={lid}/year={dt.strftime('%Y')}/month={dt.strftime('%m')}"
    f"/location-{lid}-{dt.strftime('%Y%m%d')}.csv.gz")

    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            return []
        if response.status_code != 200:
            print(f"Status {response.status_code} for location {lid} {dt}")
            return []

        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            reader = csv.DictReader(f)
            return [
                {
                    "location_id": int(row["location_id"]),
                    "sensor_id": int(row["sensors_id"]),
                    "location": row["location"],
                    "lat": float(row["lat"]) if row["lat"] else None,
                    "lon": float(row["lon"]) if row["lon"] else None,
                    "datetime": row["datetime"][:19],
                    "parameter": row["parameter"],
                    "units": row["units"],
                    "value": float(row["value"]) if row["value"] else None,
                }
                for row in reader
                if row["parameter"] in POLLUTANTS
            ]
    except Exception as e:
        print(f"Error: {e}")
        return []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_batch_to_delta(rows, month_key):
    if not rows:
        return 0

    df_batch = spark.createDataFrame([Row(**r) for r in rows])

    if last_date:
        write_mode = "append"
    else:
        write_mode = "overwrite"

    df_batch.write.format("delta").mode(write_mode) \
                .option("mergeSchema", "true").saveAsTable(TABLE_NAME)

    row_count = len(rows)
    print(f"Wrote {row_count:,} rows for {month_key}")

    return row_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if date_from:
    current = date_from
    total_days = (date_to - date_from).days + 1
    day_num = 0
    total_rows = 0
    monthly_rows = []
    current_month = current.strftime("%Y-%m")

    while current <= date_to:
        day_num += 1
        month_key = current.strftime("%Y-%m")

        if month_key != current_month and monthly_rows:
            total_rows += write_batch_to_delta(monthly_rows, current_month)

            monthly_rows = []
            current_month = month_key

        for lid in NYC_LOCATION_IDS:
            monthly_rows.extend(fetch_location_day(lid, current))

        current += timedelta(days=1)

    total_rows += write_batch_to_delta(monthly_rows, current_month)

    print(f"Total rows: {total_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
