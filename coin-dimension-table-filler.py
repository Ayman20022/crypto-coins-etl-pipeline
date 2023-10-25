import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col

# Define the Spark session
spark = SparkSession.builder \
    .appName("CoinDimensionLoader") \
    .getOrCreate()

# List of coins to fetch
coins = ['bitcoin', 'ethereum', 'eos', 'stellar', 'litecoin', 'cardano', 'tether', 'iota', 'tron']

# Define the CoinCap API endpoint
coincap_api_url = 'https://api.coincap.io/v2/assets'

# Create an empty list to store data for all coins
coin_data_list = []

# Fetch data for each coin
for coin_id in coins:
    coin_url = f'{coincap_api_url}/{coin_id}'
    response = requests.get(coin_url)
    data = response.json()
    coin_data_list.append(data['data'])

# Create a DataFrame from the list of coin data
coin_df = spark.createDataFrame(coin_data_list)

# Define the schema for the Coin Dimension
coin_schema = StructType([
    StructField("id", StringType()),
    StructField("symbol", StringType()),
    StructField("name", StringType())
])

# Select only the relevant columns and rename them to match the schema
coin_df = coin_df.select(col("id").alias("coin_id"), "symbol", "name")

# Write the data to the Coin Dimension table
coin_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/coins") \
    .option("dbtable", "coin_dimension") \
    .option("user", "admin") \
    .option("password", "ayman") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()