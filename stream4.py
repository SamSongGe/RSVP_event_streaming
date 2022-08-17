from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":

    kafka_server = "localhost:9092"
    kafka_topic = "quickstart-events"
    schema_path = "/user/SamSong/rsvp_sink/json"
    chkpoint_path = "/spark_streaming/checkpoint_4"

    spark = (
        SparkSession.builder.appName('Meetup RSVP streaming')
        .config("spark.dynamicAllocation.enabled", "false") # default is true
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.sql.shuffle.partitions", 2) # def 200
        .getOrCreate()
        )

    spark.sparkContext.setLogLevel("WARN")

    historical = spark.read.json(schema_path)
    rsvp_schema = historical.schema

    df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        )

    df.printSchema()


    df1=df.select(
      col("value").cast("string"),
      col("timestamp")
    )
    df1.printSchema()

    df2 = df1.select(
      from_json(col("value"), rsvp_schema).alias('record'),
      col("timestamp")
    )

    df3 = df2.select(col("record.state"), col("timestamp"))
    df3.printSchema()

    final_df = (df3
      .withWatermark("timestamp", "20 minutes")
      .groupBy(
        window(col("timestamp"), "3 minutes", "1 minutes"),
        col("state")
      ).count().sort(desc("window"), col("state")))

    (final_df.writeStream
      .trigger(processingTime="60 seconds")
      .option("truncate", False)
      .option("checkpointLocation", chkpoint_path)
      .queryName("window_count_by_country")
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
    )


# spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 stream4.py

