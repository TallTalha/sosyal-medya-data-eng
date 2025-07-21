# spark-consumer/daily_trend_analysis.py
"""
Bu script, Kafka'dan, raw-tweets-stream topiğini consume eder, ardından spark fonksiyonları kullanarak, verileri işler.
Sonuç olarak, günlük bazda toplam tweet ve retweet sayısını gösteren DataFrame döndürür.
"""
import sys
from configs.settings import KAFKA_SERVER
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField,StringType, IntegerType
from pyspark.sql import functions as F
from utils.logger import setup_logger

LOG = setup_logger("batch_analysis")

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
    

    
def transform_tweets(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Ham Kafka verisini alır, value sütunundaki bilgileri json formatına ayrıştırır,
    schema yapısına göre tipleri düzeltir ve kalite kontrolü yapar.
        Args:
            raw_df(DataFrame): Ham kafka verisi.
            schema(StructType): Ham verinin giydirilmesi gereken şema.
        Returns:
            DataFrame: Uygun şemaya göre düzenlemesi yapılmış veri çerçevesi. 
    """
    schema = schema

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
    schema =  StructType([
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("author_id", StringType(), True),
        StructField("author_username", StringType(), True), 
        StructField("follower_count", IntegerType(), True), 
        StructField("friends_count", IntegerType(), True), 
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
    transformed_df = transform_tweets(raw_tweets_df,schema=schema)
    transformed_df.cache()

    # Verilerin Günlük Bazda Toplam Tweet ve Retweet Analiz Tablosunu Oluşturur:
    daily_analysis_df = analyze_daily_trends(transformed_df)
    daily_analysis_df.show()

    spark.stop()
    LOG.info("Spark Session başarıyla tamamlandı ve durduruldu.")


if __name__ == '__main__':
    main()

    

