from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pymongo

def write_to_mongodb(data, collection_name):
    client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = client["factures_db"]
    collection = db[collection_name]
    collection.insert_many(data)

def process_factures():
    spark = SparkSession.builder \
        .appName("FactureProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "factures") \
        .load()

    # Process data and write to MongoDB
    # (Full code from previous example goes here)

if __name__ == "__main__":
    process_factures()