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
from kafka.errors import KafkaError
from utils.logger import setup_logger

logger = setup_logger("batch_analysis")
KAFKA_TOPIC = "raw-tweets-stream"

def main():

    logger.info(f"Batch consumer script başlatıldı. Topic: {KAFKA_TOPIC}")

    try:
        logger.info("Spark Session oluşturuyluyor...")
        spark = (
            SparkSession.builder
            .appName("TweetsConsumerBatchAnalysis")
            .master("local[*]")
            .getOrCreate()
        )
        logger.info("Spark Session oluşturuldu.")
    except Exception as e:
        logger.error(f"Spark Session oluşturulamadı: {e}", exc_info=True)

    logger.info(f"{KAFKA_TOPIC} topiğinden bilgiler okunuyor.")
    tweets_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.server",KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    logger.info("Veriler Kafkadan Okundu.")
    
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

    logger.info("Veriler ayrıştırılıyor...")
    value_df = tweets_df.select(
        from_json(col("value").cast("string"), schema=schema).alias("data")
    ).select("data.*")

    value_df = value_df.withColumn("created_at", to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    

