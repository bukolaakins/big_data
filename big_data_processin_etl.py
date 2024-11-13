#!/usr/bin/env python
# coding: utf-8

# In[11]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
from sqlalchemy import create_engine
import json
import logging

# logging to track erros
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 


# Initialize Spark session
spark = SparkSession.builder \
    .appName("AppName") \
    .config("spark.jars", "C:/Program Files/postgresql-42.7.3.jar") \
    .getOrCreate()

# Set legacy time parser policy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


with open('db_params.json') as config_file:
    db_params = json.load(config_file)

    
# Define schema for the dataset
schema = StructType([
    StructField("trip_id", IntegerType(), False),
    StructField("starttime", StringType(), False),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), False),
    StructField("trip_duration", IntegerType(), False),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", StringType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])

# Function to load data
def load_data(file_path):
    loaded_data =  spark.read.csv(file_path, header=True, schema=schema)
    return loaded_data

# Function to parse date columns
def parse_dates(bike_data):

    date_formats = [
        "M/d/yyyy HH:mm:ss", "MM/d/yyyy HH:mm:ss", "M/dd/yyyy HH:mm:ss",
        "MM/dd/yyyy HH:mm:ss", "M/d/yyyy H:mm:ss", "MM/d/yyyy H:mm:ss",
        "M/dd/yyyy H:mm:ss", "MM/dd/yyyy H:mm:ss",
        "M/d/yyyy HH:mm", "MM/d/yyyy HH:mm", "M/dd/yyyy HH:mm",
        "MM/dd/yyyy HH:mm", "M/d/yyyy H:mm", "MM/d/yyyy H:mm",
        "M/dd/yyyy H:mm", "MM/dd/yyyy H:mm"
    ]

    # Create a conditional expression to parse the date formats
    date_expr = F.coalesce(
        *[F.to_timestamp(F.col("starttime"), fmt) for fmt in date_formats]
    )

    # Apply to both starttime and stoptime
    bike_data = bike_data.withColumn("starttime", date_expr) \
                         .withColumn("stoptime", date_expr)



    bike_data = bike_data.withColumn("date", to_date(col("starttime")))
    return bike_data

# # Function to prepare data
# def prepare_data(bike_data,no_of_partitions):
#     bike_data = bike_data.repartition(no_of_partitions, "date", "usertype")
#     return bike_data

# Function to aggregate data
def aggregate_data(bike_data):
    aggregated_data=bike_data.groupBy("date", "usertype").agg(
        F.count("trip_id").alias("total_rides"),
        F.round(mean("trip_duration"), 2).alias("avg_trip_duration"),
        F.round(F.sum("trip_duration"), 2).alias("total_trip_duration")
    )

    return aggregated_data

#load transformed data into db

def load_data_to_db(dataframe, table_name):
    try:
        dataframe.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_params['host']}:{db_params['port']}/{db_params['database']}") \
            .option("dbtable", table_name) \
            .option("user", db_params['username']) \
            .option("password", db_params['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logging.info(f"Data successfully loaded into the {table_name} table.")
    except Exception as e:
        logging.error(f"Error loading data to the database: {e}")
        raise


# Main code execution
file_path = r"C:\Users\PC\Documents\bike_data.csv"
bike_data = load_data(file_path)
bike_data = parse_dates(bike_data)
transformed_data = aggregate_data(bike_data)
load_data_to_db(transformed_data, "bike_data")

# Stop Spark session
spark.stop()



# In[ ]:





# In[ ]:





# In[ ]:




