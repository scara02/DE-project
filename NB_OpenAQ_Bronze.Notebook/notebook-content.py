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

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

import requests
import time
import pandas as pd
from pyspark.sql import Row
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

API_KEY = notebookutils.credentials.getSecret(
    "https://api-fabric.vault.azure.net/",
    "openaq-api-key"
)

BBOX = "-74.259,40.477,-73.700,40.917"

BASE_URL = "https://api.openaq.org/v3"

HEADERS = {"X-API-Key": API_KEY}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try: 
    last_date = spark.sql("""
        SELECT MAX(date) as last_date
        FROM bronze_openaq_daily""").collect()[0].last_date
except:
    last_date = None

if last_date is None:
    date_from = datetime(2022, 1, 1)
else:
    date_from = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)

date_to = datetime.today().replace(day=1) - relativedelta(months=1) - timedelta(days=1)
if date_from > date_to:
    DATE_RANGES = []
else:
    DATE_RANGES = []
    current = date_from
    while current < date_to:
        range_end = min(current + relativedelta(months=6) - timedelta(days=1), date_to)
        DATE_RANGES.append((str(current), str(range_end)))
        current = range_end + timedelta(days=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_locations(bbox, base_url, headers):
    all_locations = []
    page = 1
    limit = 100
    
    while True:
        url = f"{base_url}/locations"
        params = {
            "bbox": bbox,
            "limit": limit,
            "page": page
        }

        try:
            response = requests.get(url, params=params, headers=headers)
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break

        data = response.json()
        meta = data.get("meta", {})
        found = meta.get("found", 0)

        results = data.get("results", [])

        all_locations.extend(results)

        if len(all_locations) >= found:
            break

        page+=1
        time.sleep(1)

    return all_locations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_sensors(locations):
    sensors = []

    for loc in locations:
        loc_id = loc.get("id")
        loc_name = loc.get("name", "")
        locality = loc.get("locality", "")
        is_monitor = loc.get("isMonitor", False)
        provider = loc.get("provider", {}).get("name", "")
        datetime_first = loc.get("datetimeFirst", {}).get("utc", "")
        datetime_last = loc.get("datetimeLast", {}).get("utc", "")
        loc_sensors = loc.get("sensors", [])

        for sensor in loc_sensors:
            sensors.append({
                "location_id": loc_id,
                "location_name": loc_name,
                "locality": locality,
                "is_monitor": is_monitor,
                "provider": provider,
                "sensor_id": sensor.get("id"),
                "pollutant": sensor.get("parameter", {}).get("name", ""),
                "pollutant_unit": sensor.get("parameter", {}).get("units", ""),
                "datetime_first": datetime_first,
                "datetime_last": datetime_last
            })

    return sensors

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze_openaq_sensors").collect()[0].cnt

if sensors_count == 0:
    locations = fetch_locations(BBOX, BASE_URL, HEADERS)
    sensors = get_sensors(locations)
    
    df = spark.createDataFrame([Row(**s) for s in sensors])
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable("bronze_openaq_sensors")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_df = spark.sql(f"""
            SELECT sensor_id, pollutant, pollutant_unit
            FROM bronze_openaq_sensors
            WHERE is_monitor = true
            AND datetime_first <= '{str(date_from)}'
            AND datetime_last >= '{str(date_to)}'
            """)

sensors = sensors_df.collect()
print(f"Sensors to collect from count: {len(sensors)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_measurements(sensor_id, date_ranges, base_url, headers):
    all_results = []

    for date_from, date_to in date_ranges:
        page = 1
        limit = 1000
        
        while True:
            url = f"{base_url}/sensors/{sensor_id}/days"
            params = {
                "date_from": date_from,
                "date_to": date_to,
                "limit": limit,
                "page": page
            }

            try:
                response = requests.get(url, params=params, headers=headers, timeout=30)
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                break

            if response.status_code == 429:
                print("Rate limit exceeded. Waiting...")
                time.sleep(30)
                continue

            if response.status_code != 200:
                print(f"status: {response.status_code}. {response.json()}")
                break

            data = response.json()
            results = data.get("results", [])

            if not results:
                break

            all_results.extend(results)

            page+=1
            time.sleep(1)

    return all_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_sensor_day(day, sensor_id):
    period = day.get("period", {})
    summary = day.get("summary", {})
    parameter = day.get("parameter", {})

    date = period.get("datetimeFrom", {}).get("local", "")[:10]

    return {
        "sensor_id": sensor_id,
        "date": date,
        "pollutant": parameter.get("name", ""),
        "avg": summary.get("avg"),
        "min": summary.get("min"),
        "max": summary.get("max"),
        "median": summary.get("median"),
        "q25": summary.get("q25"),
        "q75": summary.get("q75"),
        "sd": summary.get("sd"),
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_measurements = []
if DATE_RANGES:
    for i, sensor in enumerate(sensors, 1):
        sid = sensor.sensor_id
        try:
            days = fetch_measurements(sid, DATE_RANGES, BASE_URL, HEADERS)
            if not days:
                continue
            
            parsed = [parse_sensor_day(day, sid) for day in days]
            all_measurements.extend(parsed)
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(1)

print(f"Total collected records: {len(all_measurements)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not all_measurements:
    print("No new data written.")
else:
    df = spark.createDataFrame([Row(**r) for r in all_measurements])
    df.createOrReplaceTempView("new_measurements")

    df.write.format("delta").mode("append") \
        .option("overwriteSchema", "true").saveAsTable("bronze_openaq_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
