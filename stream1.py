# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 stream1.py

from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *


if __name__ == "__main__":

    data_path = "/user/SamSong/rsvp_sink/json"
    chkpoint_path = "/spark_streaming/checkpoint_1"
    kafka_server = "localhost:9092"
    kafka_topic = "quickstart-events"

    spark = (
        SparkSession.builder.appName('Meetup RSVP streaming')
        .config("spark.dynamicAllocation.enabled", "false") # default is true
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.sql.shuffle.partitions", 2) # def 200
        .getOrCreate()
        )

    spark.sparkContext.setLogLevel("WARN")

    df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        )

    df.printSchema()

    df1=df.select(col("value").cast("string"))
    df1.printSchema()

    (df1.writeStream
        .trigger(processingTime='60 seconds')
        .format("text")
        .option("path", data_path)
        .option("checkpointLocation", chkpoint_path)
        .outputMode("append")
        .start()
        .awaitTermination()
    )