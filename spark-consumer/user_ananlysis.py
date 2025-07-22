# spark-consumer/user_analysis.py
"""
Açıklama:
    Kafka'dan, raw-tweets-stream topiğini consume eder,
    bu topik belirli kelimeleri içeren tweetler ile produce edilmiştir.
    Bu script, spark fonksiyonları kullanarak, 
    verileri kullanıcı bazında aşağıdaki sorulara yanıt veren DataFrame'ler oluşturur:
        - 1: Konu hakkında en çok tweet atan kullanıcılar
        - 2: Konu hakkında tweet atmış en çok takipçili 5 kullanıcı (Influencer'lar)
        - 3: Kullanıcıları "Ünlü" olarak etiketleme
        - 4: "Ünlü" olan ve olmayanların tweet sayılarının dağılımı
        - 5: Düşük takipçili (potansiyel fake/yeni) hesapların aktivitesi
Ne işe yarar ?:
    Örnneğin, şirket ile alaklı kelimeleri içeren tweet'ler ile dolu olan bir topiğimiz varsa,
    yukarıdaki sorulara cevap veren fonksiyonlar sayesinde, şirketin Twitter/X 
    platformundaki etkileşimleri, kullanıcı bazında analiz edilebilir tablolara dönüşür.
"""
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from configs.settings import KAFKA_SERVER
from utils.logger import setup_logger

LOG = setup_logger("user_analysis")

def create_spark_session(appName: str) -> SparkSession:
    """
    Spark session oluşturur ve döndürür.
        Args:
            appName(String): Spark Oturumunun ismi.
        Returns:
            SparkSession: Fonksiyon ile oluşturulan Spark oturum nenesidir.
    """
    try:
        LOG.info("Spark Session oluşturuluyor...")
        spark = (
            SparkSession.builder
            .appName(appName)
            .master("local[*]")
            .getOrCreate()
        )   
        LOG.info("Spark Session başarıyla oluşturuldu.")
        return spark
    except Exception as e:
        LOG.critical(f"Spark Session oluşuturulurken hata:{e}", exc_info=True)
        sys.exit(1) # ÇIKIŞ -> Spark Oturumu olmadan işlem yapılamaz.

def read_from_kafka(spark: SparkSession,kafka_server: str, kafka_topic: str) -> DataFrame:
    """
    Girdi olarak verilen, Spark Oturumu ve Kafka Topic ismi kullanılarak
    topikteki veriler okunur ve DataFrame olarak döndürülür.
        Args:
            spark(SparkSession): Kafka Topiğini okuyacak olan spark oturum nesnesidir.
            kafka_server(String): Consume edilmesi gereken kafka bootstrap server adresi.  
            kafka_topic(String): Consume edilmesi gereken kafka topiğinin adıdır.
        Returns:
            DataFrame: Okunan verinin spark tarafından işlenebilmesi için DataFrame nesnesine dönüşür.
    """
    LOG.info(f"Kafka {kafka_topic} topiğinden veriler okunuyor.")
    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers",kafka_server)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    LOG.info(f"Kafka {kafka_topic} topiğinden veriler başarıyla okundu.")
    return raw_df


def transform_raw_data(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Ham Kafka verisini alır, value sütunundaki bilgileri json formatına ayrıştırır,
    schema yapısına göre tipleri düzeltir ve created_at sütunundaki zaman damgasını 
    timestamp formatına dönüştürür.
        Args:
            raw_df(DataFrame): Ham kafka verisi.
            schema(StructType): Ham verinin giydirilmesi gereken şema.
        Returns:
            DataFrame: Uygun şemaya göre düzenlemesi yapılmış veri çerçevesi. 
    """
    transformed_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), schema=schema).alias("data")
        ).select("data.*")
    
    final_df = transformed_df.withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    
    return final_df

def validate_data(df: DataFrame) -> DataFrame:
    """
    Girdi olarak verilen, DataFrame'deki, ID key alanı Null olan kayıtları çıkarır.
        Args:
            df(DataFrame): ID key alanı kontrol edilecek olan DataFrame.
        Returns:
            valid_records_df(DataFrame): ID key alanı hiç Null olmayan yeni tablo.
    """

    valid_records_df = df.filter(F.col("id").isNotNull())

    invalid_records_count = df.filter(F.col("id").isNull()).count()

    if invalid_records_count > 0:
        LOG.info(f"{invalid_records_count} adet kayıt id alanına sahip değil, bu kayıtlar çıkarıldı.")
    
    return valid_records_df

# Analiz 1: Konu hakkında en çok tweet atan kullanıcılar
def get_top_tweeters(df: DataFrame) -> DataFrame:
    """
        İşlenebilir hale getirilen tweet verilerinde, en çok tweet atan kullanıcıların tablosunu döndürür.
            Args:
                df(DataFrame): İşlenmeye hazır tweet DataFrame değişkeni.
            Returns:
                top_tweeters_df(DataFrame): En çok tweet atan kullanıcıların tablosu.
    """
    LOG.info("Konu hakkında en çok tweet atan kullanıcılar analizi...")
    top_tweeters_df = (
        df.groupBy("author_id","author_username")
        .count()
        .orderBy(F.desc("count"))
        )
    return top_tweeters_df

 # Analiz 2: Konu hakkında tweet atmış en çok takipçili kullanıcılar ve tweet sayıları (Influencer'lar)
def get_famous_top_tweeters(df: DataFrame) -> DataFrame:
    """
    İşlenebilir hale getirilen tweet verilerinde, en çok takipçili, ünlü kullanıcıların tablosunu döndürür.
        Args:
            df(DataFrame): İşlenmeye hazır tweet DataFrame değişkeni.
        Returns:
            top_tweeters_df(DataFrame): En takipçili, ünlü kullanıcıların tablosu.
    """
    LOG.info("Konu hakkında tweet atmış en çok takipçili kullanıcılar ve tweet sayıları (Influencer'lar) analizi...")
    famous_tweeters_df = (
        df.groupBy("author_id", "author_username") 
        .agg(
            F.max("follower_count")
            .alias("max_follower_count")
        )
    )
    famous_tweeters_df = famous_tweeters_df.orderBy(F.desc("max_follower_count"))
    return famous_tweeters_df

# Analiz 3: Kullanıcıları "Ünlü" olarak etiketleme
def add_isFamous_col(df: DataFrame, point : int = 100000) -> DataFrame:
    """
    İşlenebilir hale getirilen tweet verilerine isFamous alanı eklenir. 
    Point parametresinin üstünde olanlar isFamous : True diğer türlü False değerini alır.
        Args:
            df(DataFrame): İşlenmeye hazır tweet DataFrame değişkeni.
            point(int, Default=100000): Ünlü sayılmak için takipçi sayısının alt sınırıdır.
        Returns:
            isFamous_df(DataFrame): isFamous alanına sahip yeni DataFrame.
    """
    LOG.info(f"Takipçi sayısı {point}'den fazla kullanıcılar, isFamous sütünunda True olarak etiketlenecek...")
    isFamous_df = df.withColumn(
        F.when( 
            "isFamous",
            F.col("follower_count") >= point , True
        ).otherwise(False)
    )
    LOG.info(f"Takipçi sayısı {point}'den fazla kullanıcılar, isFamous sütünunda True olarak etikenlendi, diğerleri False.")
    return isFamous_df

 # Analiz 4: "Ünlü" olan ve olmayanların tweet sayılarının dağılımı
def get_isFamous_tweet_distribution(isFamous_df: DataFrame) -> DataFrame:
    """
    Tweet verilerideki isFamous alanı baz alınarak, tweet sayılarının dağılımı DataFrame olarak döndürülür. 
        Args:
            df(DataFrame): isFamous alanı bulunan orijinal DataFrame.
        Returns:
            distribution_df(DataFrame): DataFrame formatında, isFamous alanına göre gruplanmış tweet sayıları.
    """
    LOG.info("Ünlü olan ve olmayanların tweet sayılarının dağılım analizi...")
    distribution_df = isFamous_df.groupBy("isFamous").count()
    return distribution_df

# Analiz 5: Düşük takipçili (potansiyel fake/yeni) hesapların aktivitesi
def get_low_follower_activity(df: DataFrame, point: int = 100) -> DataFrame:
    """
    Tweet verilerindeki, follower_count miktarı az olan kullanıcıların, tweet atma aktivitelerini DataFrame olarak döndürür.
        Args:
            df(DataFrame): Tweet'leri içeren DataFrame.
            point(int, Default=100): Aktivitesi izlenmek istenen kullanıcıların follower_count üst limiti.
        Returns:
            activity_df(DataFrame): follower_count miktarı point parametresinden az olan kullanıcıların tweet aktivitesi DataFrame'i.
    """
    LOG.info(f"{point}'den az, düşük takipçili (potansiyel fake/yeni) hesapların aktivite analizi...")
    activity_df = (
        (df.filter(F.col("follower_count") < point))
        .groupBy("author_id","author_username","follower_count")
        .count()
        .orderBy(F.asc("follower_count"))
        )
    return activity_df

def main():
    """
    Ana iş akışını yöneten fonksiyondur. 
    Terminalde `spark-submit [options] spark-consumer/user_ananlysis.py <kafka_topic_adi>´ 
    komutuyla Script başlatıldığında <kafka topic> bilgisi main fonksyionu içerisindeki kontrollerde alınır.
    Gerekli paketler [options] kısmından belirtilerek, işlemler gerçekleşmeden indirilir veya yüklenir. 
        Args:
            None
        Returns:
            None
    """
    LOG.info("Kullanıcı bazlı analiz başlatıldı.")

    # spark-submit komutundaki argümanları kontrol eder:
    if len(sys.argv) < 2:
        LOG.error("Kullanım Hatası: Lütfen okunacak Kafka topic adını argüman olarak belirtin.")
        LOG.error("Örnek: spark-submit ... user_ananlysis.py <kafka topic>")
        sys.exit(1) # Çıkış -> Hatalı kullanımda ilgili kafka topiğine erişilemez, ananliz sonlandırılır.
    kafka_topic = sys.argv[1]
    LOG.info(f"Batch analysis başlatıldı. Kafka Topic: {kafka_topic}")

    # Sprak Session Oluşturulur
    spark = create_spark_session("UserBasedBatchAnalysis")

    # Kafkadan Veri Okunur: (Lazy Evulation dolayısyla try-expect bloğu okuma sırasında işe yaramaz)
    raw_tweets_df = read_from_kafka(spark=spark,kafka_server=KAFKA_SERVER, kafka_topic=kafka_topic)
    if raw_tweets_df is None:
        LOG.info(f"Kafka {kafka_topic} topiğinde veri yok.")
        spark.stop()
        LOG.info(f"Spark Oturumu sonlnadırıldı.")
        return
    

    # Okunan Veri Uygun Shcema ya dönüştürülür:
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
    
    # Ham veri analiz edilebilir formata ayarlanır.
    transformed_tweets_df = transform_raw_data(raw_df=raw_tweets_df, schema=schema)

    # id alanları kontrol edilir:
    final_df = validate_data(transformed_tweets_df)
    
    # LAZY EVULATION dolayısıyla Spark hata kontrolü bu kısımda yapılır:
    try:
       LOG.info("Spark analiz işlemleri başlatıldı... ")

       # Analizler Sırayla Uygulanır:

       # Analiz 1: Konu hakkında en çok tweet atan kullanıcılar
       get_top_tweeters(final_df).show(10, truncate=False)

       # Analiz 2: Konu hakkında tweet atmış en çok takipçili kullanıcılar ve tweet sayıları (Influencer'lar)
       get_famous_top_tweeters(final_df).show(10,  truncate=False)

       # Analiz 3: Kullanıcıları "Ünlü" olarak etiketleme
       isFamous_df = add_isFamous_col(final_df, point=100000)
       isFamous_df.show(10, truncate=False)

       # Analiz 4: "Ünlü" olan ve olmayanların tweet sayılarının dağılımı
       get_isFamous_tweet_distribution(isFamous_df).show(10, truncate=False)

       # Analiz 5: Düşük takipçili (potansiyel fake/yeni) hesapların aktivitesi
       get_low_follower_activity(final_df, point=100).show(10 , truncate=False)
       LOG.info("Spark ile analizler başarıyla tamamlandı.")
    except Exception as e:
        LOG.critical(f"Spark Analizleri sırasında hata oluştu: {e}", exc_info=True)
    
    finally:
        spark.stop()
        LOG.info("Spark Oturumu durduruldu.")

if __name__ == '__main__':
    main()






