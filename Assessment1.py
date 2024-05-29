import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, countDistinct, to_date
from pyspark.sql.functions import lit
from pyspark.sql.functions import  from_json, from_unixtime, date_format


# Initialize Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("User Click Data ETL") \
    .getOrCreate()

# creating file path
file_path="user_click_data1.json"

# reading the file path if exists are not
if os.path.exists(file_path):
    print("File exists")
    user_df = spark.read.json(file_path)
    user_df.show()
else:
    print("File does not exist at the specified path")


# Read the JSON data
user_df = spark.read \
    .json(file_path)

# Correct data types
user1_df = user_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
user_final_df = user1_df.withColumn("event_date", date_format(col("timestamp"), "MM-dd-yyyy"))

# represents a random time spent in minutes
result_df = user_final_df.withColumn("time_spent", col("timestamp").cast("long") % 60)  


# Group by URL, country, and date and aggregate
agg_df = result_df.groupBy("url", "country", "event_date").agg(
    avg("time_spent").alias("average_minutes_spent"),
    countDistinct("user_id").alias("unique_users_count"),
    count("click_event_id").alias("click_count")
)

# Show the result
agg_df.show()

# Write the output to a CSV file
agg_df.write.mode("overwrite").csv("output_ETL_data", header=True)
