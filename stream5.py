# pyspark --master local[2]
#--repositories https://repository.cloudera.com/artifactory/cloudera-repos/
#--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0-cdh6.3.2,org.apache.kudu:kudu-spark2_2.11:1.10.0-cdh6.3.2 --properties-file streaming.properties

# spark-submit --repositories https://repository.cloudera.com/artifactory/cloudera-repos/ --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0-cdh6.3.2,org.apache.kudu:kudu-spark2_2.11:1.10.0-cdh6.3.2 stream5.py

from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *


if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName('Meetup RSVP streaming')
        .config("spark.dynamicAllocation.enabled", False) # default is true
        .config("spark.streaming.backpressure.enabled", True)
        .config("spark.sql.shuffle.partitions", 2) # def 200
        .getOrCreate()
        )

    spark.sparkContext.setLogLevel("WARN")

    historical = spark.read.json('/user/SamSong/rsvp_sink/text')
    rsvp_schema = historical.schema

    df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "quickstart-events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        )

    df.printSchema()

    df1=df.select(col("value").cast("string"))
    df1.printSchema()

    df2 = df1.select(
      from_json(col("value"), rsvp_schema).alias("record")
    )
    df2.printSchema()

    df3 = df2.select(col("record.*"))
    df3.printSchema()

    if ("_corrupt_record" in df3.columns):
        df4 = df3.where("_corrupt_record is null").drop("_corrupt_record")
    else:
        df4 = df3
    df4.printSchema()

    df5 = df4.select(
        "rsvp_id",
        "member.member_id",
        "member.member_name",
        "group.group_id",
        "group.group_name",
        "group.group_city",
        "group.group_country",
        "event.event_name",
        "event.time"
    )
    df5.printSchema()

    (df5.writeStream
      .trigger(processingTime="60 seconds")
      .format("kudu")
      .option("kudu.master", "localhost:7051")
      .option("kudu.table", "impala::rsvp.rsvp_kudu")
      .option("kudu.operation", "upsert")
      .option("checkpointLocation", "/spark_streaming/checkpoint_5")
      .start()
      .awaitTermination())