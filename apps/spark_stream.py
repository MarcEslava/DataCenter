from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col
import logging
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")    # or "localhost:9092" if running on host
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://root:example@mongo:27017/?authSource=admin")
DB_NAME         = os.getenv("MONGO_DB", "spark_streams")
COLL_NAME       = os.getenv("MONGO_COLL", "created_users")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR", "/work/checkpoint")

def create_spark():
    spark = (
        SparkSession.builder
        .appName("KafkaToMongo")
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
        )
        # Mongo connector global config (optional—can also pass in write options)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def read_from_kafka(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "users_created")
        .option("startingOffsets", "earliest")
        .load()
    )

def parse_json(df):
    schema = StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name",  StringType(), True),
        StructField("email",      StringType(), True),
    ])
    return (
        df.selectExpr("CAST(value AS STRING) AS json_str")  \
          .select(from_json(col("json_str"), schema).alias("data"))  \
          .select("data.*")
    )

def main():
    logging.basicConfig(level=logging.INFO)
    spark = create_spark()
    raw = read_from_kafka(spark)
    parsed = parse_json(raw)

    query = (
        parsed.writeStream
              .format("mongodb")  # MongoDB Spark Connector v10 sink
              .option("checkpointLocation", CHECKPOINT_DIR)
              .option("database", DB_NAME)
              .option("collection", COLL_NAME)
              # If you didn’t set the global connection uri:
              .option("uri", MONGO_URI)
              # Optional: upsert on _id if you add one
              # .option("replaceDocument", "false")
              .outputMode("append")
              .start()
    )
    query.awaitTermination()

main()

