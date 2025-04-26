# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, IntegerType, FloatType, DoubleType, TimestampType

# # Define the schema of the Kafka message
# schema = StructType() \
#     .add("timestamp", DoubleType()) \
#     .add("sensor_id", IntegerType()) \
#     .add("value", FloatType())

# # Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("KafkaSparkStreamingApp") \
#     .master("local[*]") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Read from Kafka
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "realtime-data") \
#     .option("startingOffsets", "latest") \
#     .load()

# # Parse the Kafka value field as JSON
# df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json("json_str", schema).alias("data")) \
#     .select("data.*")

# # Output to console
# query = df_parsed.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()


# streaming_app.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, IntegerType, FloatType, DoubleType
import happybase

# Define schema
schema = StructType() \
    .add("timestamp", DoubleType()) \
    .add("sensor_id", IntegerType()) \
    .add("value", FloatType())

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkToHBase") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "realtime-data") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON message
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*")


# Function to write each batch to HBase
def write_to_hbase(batch_df, batch_id):
    import happybase
    import socket

    try:
        # HBase connection
        connection = happybase.Connection(host='hbase', port=9090, timeout=5000)
        connection.open()

        table_name = b'sensor_data'
        if table_name not in connection.tables():
            connection.create_table(
                table_name,
                {'cf': dict()}
            )
        table = connection.table(table_name)

        # Write each row to HBase
        for row in batch_df.collect():
            row_key = f"sensor-{row['sensor_id']}_{int(row['timestamp'])}"
            table.put(
                row_key.encode(),
                {
                    b'cf:timestamp': str(row['timestamp']).encode(),
                    b'cf:sensor_id': str(row['sensor_id']).encode(),
                    b'cf:value': str(row['value']).encode(),
                }
            )
        connection.close()
        print(f"[+] Batch {batch_id} written to HBase.")

    except Exception as e:
        print(f"[!] Error writing to HBase: {e}")


# Stream write with HBase sink
query = df_parsed.writeStream \
    .foreachBatch(write_to_hbase) \
    .outputMode("append") \
    .start()

query.awaitTermination()
