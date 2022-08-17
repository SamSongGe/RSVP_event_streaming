from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *


if __name__ == "__main__":

    kafka_server = "localhost:9092"
    kafka_topic = "quickstart-events"

    spark = (
        SparkSession.builder.appName('Meetup RSVP streaming')
        .config("spark.dynamicAllocation.enabled", False) # default is true
        .config("spark.streaming.backpressure.enabled", True)
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


    # Tumbling windows
    df1 = (df
      .withWatermark("timestamp", "1 hours")
      .groupBy(window(col("timestamp"), "120 seconds")).count().orderBy(desc("window"))
    )

    # Sliding window
    # df1 = df.groupBy(window(col("timestamp"), "5 minutes", "3 minutes")).count()

    (df1.writeStream
      .trigger(processingTime="60 seconds")
      .option("truncate", False)
      .option("checkpointLocation", "/spark_streaming/checkpoint_3")
      .queryName("record_count_per_2_minutes")
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
    )


# spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 stream3.py
