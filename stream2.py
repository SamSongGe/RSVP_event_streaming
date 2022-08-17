from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# pyspark --master local[2] --conf spark.streaming.backpressure.enabled=True --conf spark.dynamicAllocation.enabled=False
# pyspark --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --properties-file spark-streaming.properties
# CDH kudu --packages org.apache.kudu:kudu-spark2_2.11:1.10.0-cdh6.3.2

# pyspark --master local[2] --repositories https://repository.cloudera.com/artifactory/cloudera-repos/ --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kudu:kudu-spark2_2.11:1.10.0-cdh6.3.2 --properties-file streaming.properties
if __name__ == "__main__":

    schema_path = "/user/SamSong/rsvp_sink/json"
    data_path = "/user/SamSong/rsvp_sink/parquet"
    chkpoint_path = "/user/SamSong/spark_streaming/checkpoint_2_new"
    kafka_server = "ip-172-31-91-232.ec2.internal:9092,ip-172-31-89-11.ec2.internal:9092"
    kafka_topic = "meetup_rsvp"

    spark = (
        SparkSession.builder.appName('Meetup RSVP streaming')
        .config("spark.dynamicAllocation.enabled", False) # default is true
        .config("spark.streaming.backpressure.enabled", True)
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

    df1=df.select(col("value").cast("string"))
    df1.printSchema()

    df2 = df1.select(
      from_json(col("value"), rsvp_schema).alias("record")
    )
    df2.printSchema()


    df3 = df2.select(col("record.*"))
    df3.printSchema()

    #df4 = df3.where("_corrupt_record is null").drop("_corrupt_record")
    if ("_corrupt_record" in df3.columns):
        df4 = df3.where("_corrupt_record is null").drop("_corrupt_record")
    else:
        df4 = df3
    df4.printSchema()

    (df4.writeStream
      .trigger(processingTime="60 seconds")
      .queryName("persist_record_to_HDFS_in_parquet_format")
      .format("parquet")
      .option("path", data_path)
      .option("checkpointLocation", chkpoint_path)
      .outputMode("append")
      .start()
      .awaitTermination()
    )
  

#spark-submit --repositories https://repository.cloudera.com/artifactory/cloudera-repos/ --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0-cdh6.3.2 --master yarn --deploy_mode client stream2.py