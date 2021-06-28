from pyspark.sql.functions import *
from pyspark.sql import *


def df_writer(df, epochId):
  table_path='C:\\Users\\Agam_Kumar\\Desktop\\streaming_output\\'
  df.write \
    .format("csv") \
    .mode('overwrite') \
    .option('header',True)\
    .save(table_path)



if __name__ == "__main__":
    print("Application Started ...")

    spark = SparkSession \
            .builder \
            .appName("Socket Streaming Demo") \
            .master("local[*]") \
            .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "1")
    checkpoint_dir='C:\\Users\\Agam_Kumar\\Desktop\\streaming_checkpoint\\socket_triggering_handson\\'



    stream_df = spark \
                .readStream \
                .format("socket") \
                .option("host", "localhost") \
                .option("port", "9000") \
                .load()

    print(stream_df.isStreaming)
    stream_df.printSchema()

    stream_words_df = stream_df \
                        .select(explode(split("value", ' ')).alias("word"))

    stream_word_count_df = stream_words_df \
                            .groupBy("word").count()

    write_query = stream_word_count_df \
    .toDF("word", "count")\
    .writeStream \
    .foreachBatch(df_writer)\
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_dir)\
    .trigger(processingTime="5 second") \
    .start()

    write_query.awaitTermination()

    print("Application Completed.")