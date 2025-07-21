# spark-consumer/batch_analysis.py
"""
Bu script, Kafka'dan, raw-tweets-stream topiğini consume eder, ardından spark fonksiyonları kullanarak, verileri işler.
"""
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField,StringType, IntegerType
from pyspark.sql import functions as F
from utils.logger import setup_logger
import os

LOG = setup_logger("batch_analysis")
KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092") 
PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

def create_spark_session(appName: str) -> SparkSession:
    """
    Spark session oluşturur.
        Args:
            appName(String): Spark Oturumunun ismi.
        Returns:
            SparkSession: Fonksiyon ile oluşturulan Spark oturum nenesidir.
    """
    try:
        LOG.info("Spark Session oluşturuluyor..")
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        spark = (
            SparkSession.builder
            .appName(appName)
            .master("local[*]")  
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        LOG.info("Spark Session başarıyla oluşturuldu.")
        return spark
    except Exception as e:
        LOG.critical(f"Spark Session oluşturulurken hata: {e}",exc_info=True)
        sys.exit(1) # Çıkış -> Spark Session Oluşmadan Analize Devam Edilemez 

def read_from_kafka(spark: SparkSession, kafka_topic: str) -> DataFrame:
    """
    Girdi olarak verilen, Spark Oturumu ve Kafka Topic ismi kullanılarak
    topikteki veriler okunur ve DataFrame olarak döndürülür.
        Args:
            spark(SparkSession): Kafka Topiğini okuyacak olan spark oturum nesnesidir.
            kafka_topic(String): Okunması gereken kafka topiğinin adıdır.
        Returns:
            DataFrame: Okunan verinin spark tarafından işlenebilmesi için DataFrame nesnesine dönüşür.
    """
    try:
        LOG.info(f"{kafka_topic} topiğinden veriler okunuyor...")
        df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        LOG.info(f"{kafka_topic} topiğinden veriler başarıyla okundu.")
        return df
    except Exception as e:
        LOG.error(f"Spark, Kafka Topiğini okurken hata oluştu: {e}", exc_info=True)
        return None
    

    
def transform_tweets(raw_df: DataFrame) -> DataFrame:
    """
    Ham Kafka verisini alır, value sütunundaki bilgileri json formatına ayrıştırır,
    schema yapısına göre tipleri düzeltir ve kalite kontrolü yapar.
        Args:
            raw_df(DataFrame): Ham kafka verisi.
        Returns:
            DataFrame: Uygun şemaya göre düzenlemesi yapılmış veri çerçevesi. 
    """
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

    parsed_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), schema=schema).alias("data")
    ).select("data.*")

    valid_records_df = parsed_df.filter(F.col("id").isNotNull())

    invalid_records_df = parsed_df.filter(F.col("id").isNull()).count()

    if invalid_records_df > 0:
            LOG.warning(f"{invalid_records_df} adet kayıt, id alanı eksik olduğu için atlandı.")
    
    # Zaman damgasının manuel "yyyy-MM-dd'T'HH:mm:ss'Z'" ifadesiyle düzeltilmesi:
    final_df = parsed_df.withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    return final_df

def analyze_daily_trends(processed_df: DataFrame) -> DataFrame:
    """
    İşlenmiş tweet verisini, günlük bazda toplam tweet ve retweet sayısını gösteren DataFrame'e dömüştürür ve döndürür.
        Args:
            processed_df(DataFrame): Analize hazır işlenmiş DataFrame.
        Returns:
            DataFrame: GÜnlük bazda toplam tweet ve retweet sayısını içeren DataFrame.
    """
    daily_df = processed_df.withColumn("day",F.date_trunc("day",F.col("created_at")))

    daily_analysis_df = daily_df.groupBy("day").agg(
        F.count("*").alias("toplam_tweet_sayisi"),
        F.sum(F.col("public_metrics.retweet_count")).alias("toplam_retweet_sayisi")
    ).orderBy(F.col("day").desc())

    LOG.info("Günlük analiz sonuçları:")
    daily_analysis_df.printSchema()
    daily_analysis_df.show(10, truncate=False)

    return daily_analysis_df



def main():
    """
    Ana iş akışını yöneten fonksiyondur. 
    Terminalde `spark-submit [options] batch_analysis.py <kafka_topic_adi> spark-consumer/batch_analysis.py´ 
    komutuyla Script başlatıldığında <kafka topic> bilgisi main fonksyionu içerisindeki kontrollerde alınır.
    Gerekli paketler [options] kısmından belirtilerek, işlemler gerçekleşmeden indirilir veya yüklenir. 
        Args:
            None
        Returns:
            None
    """
    
    # spark-submit komutundaki argümanları kontrol eder:
    if len(sys.argv) < 2:
        LOG.error("Kullanım Hatası: Lütfen okunacak Kafka topic adını argüman olarak belirtin.")
        LOG.error("Örnek: spark-submit ... batch_analysis.py raw-tweets-stream")
        sys.exit(1) # Çıkış -> Hatalı kullanımda ilgili kafka topiğine erişilemez, ananliz sonlandırılır.
    kafka_topic = sys.argv[1]
    LOG.info(f"Batch analysis başlatıldı. Kafka Topic: {kafka_topic}")

    spark = create_spark_session("TweetsConsumerBatchAnalysis")
    
    
    # (Extract) Ham Veri Çekilir:
    raw_tweets_df = None
    raw_tweets_df = read_from_kafka(spark=spark,kafka_topic=kafka_topic)

    if raw_tweets_df is None:
        LOG.warning(f"'{kafka_topic}' topiğinde işlenecek  veri bulunamadı. İşlem sonlandırılıyor.")
        spark.stop()
        return
    
    # (Transform) 
    # Ham Veri İşlenebilir Yapıya Dönüştürülür:
    transformed_df = transform_tweets(raw_tweets_df)
    transformed_df.cache()

    # Verilerin Günlük Bazda Toplam Tweet ve Retweet Analiz Tablosu Oluşturulur
    daily_analysis_df = analyze_daily_trends(transformed_df)

    spark.stop()
    LOG.info("Spark Session başarıyla tamamlandı ve durduruldu.")

"""
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
        F.from_json(F.col("value").cast("string"), schema=schema).alias("data")
    ).select("data.*")

    value_df = value_df.withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    value_df.cache()
    LOG.info(f"Toplam {value_df.count()} kayıt işlenmeye hazır.")

    LOG.info("İşlenmiş verinin ilk 5 satırı ve şeması:")
    value_df.printSchema()
    value_df.show(5, truncate=False)  

    daily_df = value_df.withColumn("day",F.date_trunc("day",F.col("created_at")))

    daily_analysis_df = daily_df.groupBy("day").agg(
        F.count("*").alias("toplam_tweet_sayisi"),
        F.sum(F.col("public_metrics.retweet_count")).alias("toplam_retweet_sayisi")
    ).orderBy(F.col("day").desc())

    LOG.info("Günlük analiz sonuçları:")
    daily_analysis_df.printSchema()
    daily_analysis_df.show(10, truncate=False)

    spark.stop()
    LOG.info("Spark Session başarıyla tamamlandı ve durduruldu.")
"""


if __name__ == '__main__':
    main()

    

