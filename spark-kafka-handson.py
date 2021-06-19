# from pyspark.sql import SparkSession
import findspark
findspark.init()
from pyspark.sql import SparkSession


# Spark session & context
spark = (SparkSession
         .builder
         .master('local')
         .appName('wiki-changes-event-consumer')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")
         .getOrCreate())
sc = spark.sparkContext

df = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092") # kafka server
  .option("subscribe", "myTopic") # topic
  .option("startingOffsets", "earliest") # start from beginning
  .load())

df.printSchema()

query=df.selectExpr("CAST(value AS STRING)").writeStream.outputMode('append')\
    .format('console')\
    .option('truncate', 'false')\
    .start()

query.awaitTermination()