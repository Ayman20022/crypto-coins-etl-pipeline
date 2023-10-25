from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, from_unixtime, year, month, dayofmonth, hour,minute, second, col,current_timestamp,unix_timestamp,expr,concat,lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType,IntegerType
# Create a Spark session
spark = SparkSession.builder \
    .appName("CoinETL") \
    .getOrCreate()

# Define the schema for the JSON data
json_schema = StructType([
    StructField("coin", StructType([
        StructField("id", StringType()),
        StructField("rank", IntegerType()),
        StructField("symbol", StringType()),
        StructField("name", StringType()),
        StructField("supply", FloatType()),
        StructField("maxSupply", FloatType()),
        StructField("marketCapUsd", FloatType()),
        StructField("volumeUsd24Hr", FloatType()),
        StructField("priceUsd", FloatType()),
        StructField("changePercent24Hr", FloatType()),
        StructField("vwap24Hr", FloatType()),
        StructField("explorer", StringType())
    ])),
    StructField("date", StringType())
])

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "coin_topic") \
     .option("startingOffsets", "earliest") \
    .load()



df = df.withColumn("value", col("value").cast("string"))

# Parse the JSON data
df = df.select(from_json(col("value"), json_schema).alias("data"))



# Flatten the nested structure
df = df.select("data.coin.*", "data.date")

"""
counter = 0

# Define a UDF (User-Defined Function) to generate a unique identifier
def generate_unique_id():
    global counter
    counter += 1
    return counter

# Register the UDF
spark.udf.register("generate_unique_id", generate_unique_id)

# Add a column with the unique identifier to "df"
df = df.withColumn("unique_id", lit(None).cast("integer"))
df = df.withColumn("unique_id", expr("generate_unique_id()"))
df = df.withColumn("unique_id",col("unique_id").cast("integer"))
"""





def write_to_postgres_fact(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/coins") \
        .option("dbtable","fact_table") \
        .option("user", "admin") \
        .option("password", "ayman") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    


def write_to_postgres_date(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/coins") \
        .option("dbtable", "date_dimension") \
        .option("user", "admin") \
        .option("password", "ayman") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


    









"""coin_counter = 0



def process_records(batch_df, epoch_id):
    global coin_counter 
    batch_df.show()
    # Extract data for coin_dimension
    coin_dimension_records = batch_df.select(col("id").alias("coin_id"), "name", "symbol").distinct()

    # Extract data for date_dimension
    date_dimension_records = batch_df.select(
        col("unique_id").alias("id"),
        (year(from_unixtime(col("date") / 1000))).cast("bigint").alias("year"),
        (month(from_unixtime(col("date") / 1000))).cast("bigint").alias("month"),
        (dayofmonth(from_unixtime(col("date") / 1000))).cast("bigint").alias("day"),
        (hour(from_unixtime(col("date") / 1000))).cast("bigint").alias("hour"),
        (minute(from_unixtime(col("date") / 1000))).cast("bigint").alias("minute"),
        (second(from_unixtime(col("date") / 1000))).cast("bigint").alias("second")
    ).distinct()

    # Extract data for fact_table
    fact_table_records = batch_df.select(
        col("unique_id").alias("id"),
        col("id").alias("coin_id"),
        "rank", "priceUsd", "supply", "marketCapUsd", "changePercent24Hr", "volumeUsd24Hr", "vwap24Hr"
    )
    if counter_coin<9 :
        write_to_postgres_coin(coin_dimension_records, epoch_id)
        coin_counter += 1  # Increment the counter

    # Write records to the corresponding tables
    write_to_postgres_date(date_dimension_records, epoch_id)
    write_to_postgres_fact(fact_table_records, epoch_id)



"""



from pyspark.sql.functions import from_unixtime, col, unix_timestamp, year, month, dayofmonth, hour, minute, second

def process_records(batch_df, epoch_id):
    for row in batch_df.collect():
        # Process each record in the batch individually
        # Convert the 'date' column to a timestamp (assuming it's in milliseconds)
        timestamp_milliseconds = int(row.date)
          # Cast 'date' to a numeric type
        t = timestamp_milliseconds / 1000.0  # Convert to seconds
        data = [(t,)]
        date_dimension = spark.createDataFrame(data, ["timestamp_seconds"])
        date_dimension = date_dimension.withColumn("timestamp", from_unixtime("timestamp_seconds"))
        date_dimension = date_dimension.withColumn("year", year("timestamp"))
        date_dimension = date_dimension.withColumn("month", month("timestamp"))
        date_dimension = date_dimension.withColumn("day", dayofmonth("timestamp"))
        date_dimension = date_dimension.withColumn("hour", hour("timestamp"))
        date_dimension = date_dimension.withColumn("minute", minute("timestamp"))
        date_dimension = date_dimension.withColumn("second", second("timestamp"))
        date_dimension = date_dimension.drop('timestamp_seconds')
        date_dimension = date_dimension.drop('timestamp')

        # Extract data for fact_table for the current record
        fact_table_record = spark.createDataFrame(
    [(row.id,
      row.rank,
      row.priceUsd,
      row.supply,
      row.marketCapUsd,
      row.changePercent24Hr,
      row.volumeUsd24Hr,
      row.vwap24Hr)],
    ["coin_id", "rank", "priceUsd", "supply", "marketCapUsd", "changePercent24Hr", "volumeUsd24Hr", "vwap24Hr"]
)


        # Write records to the corresponding tables for the current record
        write_to_postgres_date(date_dimension, epoch_id)
        write_to_postgres_fact(fact_table_record, epoch_id)

streaming_query = df.writeStream.foreachBatch(process_records).start()
streaming_query.awaitTermination()





