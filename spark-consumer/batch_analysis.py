# spark-consumer/batch_analysis.py
"""
Bu script, Kafka'dan, raw-tweets-stream topiğini consume eder, ardından spark fonksiyonları kullanarak, verileri işler.
"""
from configs.settings import KAFKA_SERVER
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField,StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, to_timestamp
StructField,StringType, IntegerType, TimestampType
StructField,StringType, IntegerType, TimestampType
from utils.logger import setup_logger

LOG = setup_logger("batch_analysis")
KAFKA_TOPIC = "raw-tweets-stream"

def main():

    LOG.info(f"Batch consumer script başlatıldı. Topic: {KAFKA_TOPIC}")
    kafka_connector_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

    tweets_df = None
    try:
        LOG.info("Spark Session oluşturuyluyor...")
        spark = (
            SparkSession.builder
            .appName("TweetsConsumerBatchAnalysis")
            .master("local[*]")
            .config("spark.jars.packages", kafka_connector_packages)
            .getOrCreate()
        )
        LOG.info("Spark Session oluşturuldu.")
    except Exception as e:
        LOG.error(f"Spark Session oluşturulamadı: {e}", exc_info=True)
        return

    LOG.info(f"{KAFKA_TOPIC} topiğinden bilgiler okunuyor.")
    try:
        tweets_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers",KAFKA_SERVER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )
        LOG.info("Veriler Kafkadan Okundu.")
    except Exception as e:
        LOG.critical(f"Spark, Kafka Topiğini okurken hata oluştu:{e}", exc_info=True)
        spark.stop()
        return
    
    if tweets_df is None:
        LOG.error(f"{KAFKA_TOPIC} topiğinde veri yok. İşlem sonlandırıldı.")
        spark.stop()
        return
        

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("author_id", StringType(), True),
        StructField("public_metrics", StructType([
            StructField("retweet_count",IntegerType(), True),
            StructField("reply_count",IntegerType(), True),
            StructField("like_count",IntegerType(), True),
            StructField("quote_count",IntegerType(), True),
            StructField("impression_count",IntegerType(), True),
        ]),True),
        StructField("lang", StringType(), True),
        StructField("source", StringType(), True)

    ])

    LOG.info("Veriler ayrıştırılıyor...")
    value_df = tweets_df.select(
        from_json(col("value").cast("string"), schema=schema).alias("data")
    ).select("data.*")

    value_df = value_df.withColumn("created_at", to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    value_df.cache()
    LOG.info(f"Toplam {value_df.count()} kayıt işlenmeye hazır.")

    LOG.info("İşlenmiş verinin ilk 5 satırı ve şeması:")
    value_df.printSchema()
    value_df.show(5, truncate=False)  

    date_group_df = value_df.groupBy("created_at").count().withColumnRenamed("created_at","dateGroup")

    retweet_df = value_df.groupBy("created_at").sum("retweet_count").withColumnRenamed("sum(retweet_count)","retweet_count")

    date_group_df.join(retweet_df, retweet_df["created_at"] == date_group_df["dateGroup"]).select("dateGroup","count","retweet_count").show()

    spark.stop()
    LOG.info("Spark Session başarıyla tamamlandı ve durduruldu.")

if __name__ == '__main__':
    main()

    

