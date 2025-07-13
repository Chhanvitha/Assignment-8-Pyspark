# NYC Taxi Dataset Analysis with PySpark (Databricks)

This project performs analytical queries on NYC Yellow Taxi Trip Data (March 2016) using **PySpark on Databricks**. The analysis includes fare trends, vendor earnings, passenger patterns by area, and time-based aggregations.

---

## 📂 Dataset Source

You can download the required dataset from **Kaggle**:

**Dataset Name:** `yellow_tripdata_2016-03.csv`  
**Link:** [Kaggle NYC Taxi Data](https://www.kaggle.com/datasets/szamil/nyc-taxi-trip-duration/data?select=yellow_tripdata_2016-03.csv)

> ⚠️ The file size exceeds 25MB, so it's not included in this repository. Please download it manually and upload to Databricks manually (see setup steps below).

---

## ⚙️ Environment Setup

This project is developed and tested on **Databricks Community Edition** using Python and PySpark.

### 1. Create a Databricks Workspace
- Sign up at [community.cloud.databricks.com](https://community.cloud.databricks.com)
- Create a new **Python notebook**

### 2. Upload Dataset Manually to Workspace
- After downloading `yellow_tripdata_2016-03.csv`:
  - Go to **Workspace → Your folder → Click “Upload”**
  - Upload the CSV file
  - Note the final path, e.g.:
    ```
    /Workspace/Users/<your-email>/yellow_tripdata_2016-03.csv
    ```

---

## 🔍 Data Load & Preprocessing

Import required modules:
```python
from pyspark.sql.functions import col, sum, avg, count, round, to_timestamp, window, unix_timestamp, max as max_, lit


Load data:

python
Copy code
df = spark.read.option("header", True).csv("/Workspace/Users/<your-email>/yellow_tripdata_2016-03.csv", inferSchema=True)
Fix datetime parsing:

python
Copy code
df = df.withColumn("pickup_time", to_timestamp("tpep_pickup_datetime", "dd-MM-yyyy HH:mm"))
✅ Queries Performed
Query 1: Add "Revenue" Column
python
Copy code
df = df.withColumn("Revenue", 
    col("fare_amount") + col("extra") + col("mta_tax") + 
    col("improvement_surcharge") + col("tip_amount") + 
    col("tolls_amount") + col("total_amount")
)
Query 2: Passenger Count by Pickup Area
python
Copy code
df.groupBy(
    round(col("pickup_longitude"), 2), 
    round(col("pickup_latitude"), 2)
).agg(sum("passenger_count").alias("total_passengers")) \
 .orderBy("total_passengers") \
 .show()
Query 3: Real-time Avg Fare and Earning by Vendor
python
Copy code
df.groupBy("VendorID") \
  .agg(
      round(avg("fare_amount"), 2).alias("avg_fare"),
      round(avg("total_amount"), 2).alias("avg_total_earning")
  ).show()
Query 4: Moving Count of Payments by Mode (1-hour window)
python
Copy code
df.groupBy(window("pickup_time", "1 hour"), "payment_type") \
  .agg(count("*").alias("payment_count")) \
  .orderBy("window") \
  .show()
Query 5: Top 2 Vendors by Earnings on a Specific Date
python
Copy code
from pyspark.sql.functions import to_date

df_filtered = df.withColumn("trip_date", to_date("pickup_time"))

df_filtered.filter(col("trip_date") == "2016-03-01") \
    .groupBy("VendorID") \
    .agg(
        sum("total_amount").alias("total_revenue"),
        sum("passenger_count").alias("total_passengers"),
        sum("trip_distance").alias("total_distance")
    ).orderBy(col("total_revenue").desc()) \
    .limit(2) \
    .show()
Query 6: Most Frequent Passenger Route (Pickup → Dropoff)
python
Copy code
df.groupBy(
    round(col("pickup_longitude"), 2), 
    round(col("pickup_latitude"), 2), 
    round(col("dropoff_longitude"), 2), 
    round(col("dropoff_latitude"), 2)
).agg(sum("passenger_count").alias("total_passengers")) \
 .orderBy(col("total_passengers").desc()) \
 .show(1)
Query 7: Top Pickup Locations in Last 10 Seconds of Data
python
Copy code
latest_time = df.select(max_("pickup_time")).first()[0]

df.filter(
    unix_timestamp("pickup_time") >= unix_timestamp(lit(latest_time)) - 10
).groupBy(
    "pickup_longitude", "pickup_latitude"
).agg(
    sum("passenger_count").alias("total_passengers")
).orderBy("total_passengers", ascending=False).show()
📌 Notes
Make sure the timestamp format is correctly parsed (dd-MM-yyyy HH:mm) for your version of the CSV.

If pickup_time conversion fails, check the column format using:

python
Copy code
df.select("tpep_pickup_datetime").show()
