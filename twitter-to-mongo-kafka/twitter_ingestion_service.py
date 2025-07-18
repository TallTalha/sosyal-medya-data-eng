# twitter-to-mongo-kafka/twitter_ingestion_service.py
"""
Twitter'dan hem MongoDB'ye hem de Apache Kafka Topiğine veri akışı sağlayan bir Python scriptidir.
Bu script, Twitter API'si veya Faker kütüphanesinin fake veri üretimi aracılığıyla gelen tweet'leri 
MongoDB veritabanına kayıt eder ve Kakfa Topiğine Produce eder.
"""

# Hem kolaylık hem güvenlik açısından ortam değişkenleri .env dosyasında tutulur.
from configs.settings import X_API_KEY, X_API_KEY_SECRET, X_BEARER_TOKEN, MONGO_CLIENT, KAFKA_SERVER

# Logger kurulumu için gereklü import:
from utils.logger import setup_logger

# X Api kullanımı ve mongoDB bağlantısı için gerekli kütüphaneler:
import tweepy
import pymongo
import pymongo.collection
import pymongo.errors

# Kafka Topiğine Produce edebilmemiz için gerekli kütüphaneler:
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# X Api Liimitleri aşıldığında fake veri üretimi için kütüphaneler:
from datetime import datetime, timezone
import random
from faker import Faker

logger = setup_logger("twitter_ingestion_service")
faker= Faker("tr_TR") 

def create_twitter_api_client() -> tweepy.Client:
    """ 
    Twitter API istemcisini oluşturur ve döndürür.
        Args:
            None
        Returns:
            tweepy.Client: Twitter API istemcisi. 
    """
    try:
        # Tweepy kütüphanesi ile Twitter API istemcisini oluşturur.
        logger.info("Twitter API istemcisi oluşturuluyor...")
        client = tweepy.Client(
            bearer_token=X_BEARER_TOKEN,
            consumer_key=X_API_KEY,
            consumer_secret=X_API_KEY_SECRET,
            #wait_on_rate_limit=True
        )
        logger.info("Twitter API istemcisi başarıyla oluşturuldu.")
        return client
    except Exception as e:
        logger.critical(f"Twitter API istemcisi oluşturulurken hata oluştu: {e}", exc_info=True)
        return None
    
def get_mongodb_collection(db_name: str = "social_media_db", collection_name:str = "raw_tweets"):
    """
    MongoDB'ye bağlanır ve istenen koleksiyon nesnesini döndürür.
        Args:
            db_name: MongoDB de bulunan database ismi.
            collection_name: İlgili Database de bulunan koleksiyon ismi.
        Returns:
            pymongo.Collection: Koleksiyon Nesnesi
    """
    try:
        logger.info(f"MongoDB bağlantısı kuruluyor... Veritabanı:{db_name} & Koleksiyon:{collection_name}")
        # Digital Ocean Sunucularındaki Uzak Makineye SSH ile bağlantı sağladığımız
        # için Sunucu IPv4 adresi olmadan localhost ifadesiyle bağlanabiliyoruz.
        client = pymongo.MongoClient(MONGO_CLIENT)
        db = client[db_name]
        collection = db[collection_name]
        logger.info("MongoDB koleksiyonu başarıyla alındı.")
        return collection
    except Exception as e:
        logger.critical(f"MongoDB bağlantısı kurulurken hata oluştu: {e}",exc_info=True)
        return None

def create_kafka_producer():
    """
    Kafka Producer nesnesini oluşturur ve döndürür.
        Args:
            None
        Returns:
            kafka.KafkaProducer: Kafka Producer Nesnesi
    """
    logger.info("Kafka producer oluşturuluyor...")
    try:
        producer = KafkaProducer(
            bootstarpserver= KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        logger.info("Kafka Producer oluşturuldu.")
        return producer
    except KafkaError as ke:
        logger.critical(f"Kafa Producer oluşuturulurken KRİTİK hata oluştu: {ke}", exc_info=True)
        return None


def _generate_fake_tweet(query: str = "sahte_veri") -> dict:
    """
    API limitine takıldığında, gerçek bir tweet yapısını taklit eden
    sahte bir Python sözlüğü üretir.
        Args:
            query: API ye gönderilecek user_name veya search_term gibi parametrelerin yerini alır.
        Returns
            dict: API den dönen response verisini taklit eden bir dict yapısıdır.
    """
    now = datetime.now(timezone.utc)
    return {
        'id': str(faker.random_number(digits=18)),
        'text': f"Bu, '{query}' araması için Faker ile üretilmiş sahte bir tweettir. {faker.sentence(nb_words=15)} #SahteVeri",
        'created_at': now.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'author_id': str(faker.random_number(digits=10)),
        'public_metrics': {
            'retweet_count': random.randint(0, 500),
            'reply_count': random.randint(0, 50),
            'like_count': random.randint(0, 2000),
            'quote_count': random.randint(0, 30),
            'impression_count': random.randint(1000, 50000)
        },
        'lang': 'tr',
        'source': 'FakeTweetGenerator'
    }

def _save_tweets_to_mongo(tweets_batch: list, collection: pymongo.collection.Collection):
    """
    Gelen tweet listesini batch(toplu) olarak insert_many ile toplu olarak MongoDB'ye yazar.
    Kod tekrarını önleyen bir yardımcı fonksiyondur.
        Args:
            tweets_batch (list): Liste formatındaki tweet listesidir.
            collection (pymongo.collection.Collection): Verilerin yazılacağı koleksiyon ismidir.
        Returns:
            None
    """
    if not tweets_batch:
        logger.info("Tweet listesi boş.")
        return
    
    try:
        documents_to_insert = [tweet.data if hasattr(tweet, 'data') else tweet for tweet in tweets_batch]

        logger.info("Tweet listesi koleksiyona yazılıyor.")
        collection.insert_many(documents_to_insert, ordered=False)
        
        logger.info(f"{len(documents_to_insert)} adet tweet içeren batch MongoDB'ye yazıldı.")  
    except pymongo.errors.BulkWriteError as bwe:
        logger.warning("Toplu (batch) yazma işlemi sırasında bazı kayıtlar duplike olabilir: {bwe.details}")
    except Exception as e:
        logger.error(f"MondoDB'ye toplu yazma sırasında hata:{e}", exc_info=True)


def fetch_user_timeline(client: tweepy.Client, collection: pymongo.collection.Collection,
                         producer: KafkaProducer, kafka_topic: str,
                         username: str = "elonmusk", max_tweets: int = 10
                        ):
    """
    Belirli bil kullanıcının tweet'lerini alır ve MongoDB koleksiyonuna kaydeder.
        Args:
            client (tweepy.Client): Twitter API istemcisi.
            collection (pymongo.collection.Collection): MongoDB koleksiyonu.
            producer (KafkaProducer): Verilerin produce edileceği kafka producer nesnesi.
            kafka_topic (str): Produce edilen verileri dinleyen kafka topiğinin ismi.
            username (str): Tweet'leri alınacak kullanıcının ID'si. Varsayılan: "elonmusk".
            max_tweets (int): Alınacak maksimum tweet sayısı. Varsayılan: 200.
        Returns:
            None
    """
    logger.info(f"{username} kullanıcısının tweet'leri alınıyor... Maksimum tweets:{max_tweets}")
    try:
        # Kullanıcı sorgulanır ve  verileri alınır.
        user = client.get_user(username=username)
        
        if not user:
            logger.error(f"{username} kullanıcısı bulunamadı.")
            return
        
        user_id = user.data.id
        # MongoDB ye performanslı veri göndermek için batch(toplu) işlem yapılcak.
        # Toplu kaydetme için boş bir liste oluşturuldu.
        tweets_to_insert = []
        
        # Paginator sayesinde API limitleri yönetilir ve tweet'ler alınır.
        for tweet in tweepy.Paginator(
            client.get_users_tweets,
            id=user_id,
            max_results=5
        ).flatten(limit=max_tweets):
            #Kafkanın kendi veri yöneticisi olduğu için toplu işlem yerine tekil olarak veriler gönderildi.
            try:
                producer.send(kafka_topic, value=tweet.data)
            except KafkaError as ke:
                logger.error(f"Kafka ya tekil mesaj gönderilirken hata: {ke}")
            
            tweets_to_insert.append(tweet)
            
            if len(tweets_to_insert) >= 5:
                _save_tweets_to_mongo(tweets_to_insert,collection=collection)
                tweets_to_insert = []

        #Döngü bittiğinde 100'den az tweet varsa onları da MongoDB ye yazarız
        _save_tweets_to_mongo(tweets_to_insert,collection=collection)
        logger.info(f"{username} kullanıcısının verileri, MongoDB ve Kafka ya gönderildi.")
    
    except tweepy.errors.TooManyRequests:
        # EĞER HIZ LİMİTİNE TAKILIRSAK, BU BLOK ÇALIŞIR
        logger.warning(f"X API hız limitine takılındı. '{username}' kullanıcısı için sahte veri üretimine geçiliyor...")
        for _ in range(max_tweets):
            fake_tweet = _generate_fake_tweet(username)
            try:
                producer.send(kafka_topic, value=fake_tweet)
            except KafkaError as ke:
                logger.error(f"Kafka ya tekil sahte mesaj gönderilirken hata: {ke}")
            tweets_to_insert.append(fake_tweet)
        
        # API Hız Limitlerini aşıldıysa sahte verileri MongoDB ye yazarız
        _save_tweets_to_mongo(tweets_to_insert,collection=collection)
        logger.info(f"{username} kullanıcısının sahte verileri, MongoDB ve Kafka ya gönderildi.")

    except Exception as e:
        logger.error(f"{username} kullanıcısının tweet'leri alınırken hata oluştu: {e}", exc_info=True)

    # Döngü veya faker veri üretimi bittiğinde, Kafka'nın tamponunda kalan tüm mesajların gönderildiğinden emin oluruz.
    producer.flush()

def search_text_in_tweets(client: tweepy.Client, collection: pymongo.collection.Collection,
                            producer: KafkaProducer, kafka_topic: str,
                            query: str = '"SahteVeri" lang:tr -is:retweet', max_tweets: int = 10):
    """
    Belirli bir metni içeren tweet'leri alır ve MongoDB koleksiyonuna kaydeder.
        Args:
            client (tweepy.Client): Twitter API istemcisi.
            collection (pymongo.collection.Collection): MongoDB koleksiyonu.
            producer (KafkaProducer): Verilerin produce edileceği kafka producer nesnesi.
            kafka_topic (str): Produce edilen verileri dinleyen kafka topiğinin ismi.
            query (str): Aranacak metin. Varsayılan: '"Büyük Veri" lang:tr -is:retweet'.
            max_tweets (int): Alınacak maksimum tweet sayısı. Varsayılan: 200.
            
        Returns:
            None 
    """
    logger.info(f"{query} metnini içeren tweet'ler aranıyor...")
    try:

        tweets_to_insert = []
        # Paginator sayesinde API limitleri yönetilir ve tweet'ler alınır.
        for tweet in tweepy.Paginator(
            client.search_recent_tweets,
            query=query,
            max_results=5
        ).flatten(limit=max_tweets):
            
            try:
                producer.send(kafka_topic, value=tweet.data)
            except KafkaError as ke:
                logger.error(f"Kafka ya tekil  mesaj gönderilirken hata: {ke}")
            
            tweets_to_insert.append(tweet)

            if len(tweets_to_insert) >= 5: 
                _save_tweets_to_mongo(tweets_to_insert,collection=collection) 
                tweets_to_insert = [] # Toplu yazım sonrası liste temizlenir.

        # Listede 100'den az tweet klaydıysa onlarıda yazmak için tekrar fonksiyon çağrılır. 
        _save_tweets_to_mongo(tweets_to_insert,collection=collection)
        logger.info(f"{query} içerikli tweetler, MongoDB ve Kafka ya gönderildi.")

    except tweepy.errors.TooManyRequests:
        logger.warning(f"X API hız limitine takılındı. '{query}' için sahte veri üretimine geçiliyor...")
        
        for _ in range(max_tweets):
            fake_tweet = _generate_fake_tweet(query)
            try:
                producer.send(kafka_topic, value=tweet.data)
            except KafkaError as ke:
                logger.error(f"Kafka ya tekil sahte mesaj gönderilirken hata: {ke}")
            tweets_to_insert.append(tweet)
                
        _save_tweets_to_mongo(tweets_to_insert, collection)
        logger.info(f"{query} içerikli tweetler için sahte veriler, MongoDB ve Kafka ya gönderildi.")

    except Exception as e:
        logger.error(f"{query} metnini içeren tweet'ler alınırken hata oluştu: {e}", exc_info=True)
    
    # Döngü veya faker veri üretimi bittiğinde, Kafka'nın tamponunda kalan tüm mesajların gönderildiğinden emin oluruz.
    producer.flush()
    
if __name__ == "__main__":    
    logger.info("Çift yönlü (Mongo & Kafka) veri toplama servisi başlatıldı.")
    
    client = create_twitter_api_client()
    mongo_collection = get_mongodb_collection()
    kafka_producer = create_kafka_producer()
    kafka_topic = "raw-tweets"
    
    if client and mongo_collection is not None and kafka_producer:
        # Belirli bir kullanıcının tweet'lerini alır ve MongoDB koleksiyonuna kaydeder.
        fetch_user_timeline(client, mongo_collection,
                            producer=kafka_producer,kafka_topic=kafka_topic,
                            username="elonmusk", max_tweets=10
                            )
        
        # Belirli bir metni içeren tweet'leri alır ve MongoDB koleksiyonuna kaydeder.
        search_text_in_tweets(client, mongo_collection,
                                producer=kafka_producer, kafka_topic=kafka_topic,
                                query='"SahteVeri" lang:tr -is:retweet', max_tweets=10)
    else:
        logger.error("Twitter API istemcisi veya MongoDB koleksiyonu veya Kafka Producer oluşturulamadı. Uygulama sonlandırılıyor.")
    logger.info("twitter_producer.py sonlandırıldı.")
    # Uygulama sonlandırıldıktan sonra MongoDB bağlantısı otomatik olarak kapanır.
    # pymongo.MongoClient() ile oluşturulan bağlantı, Python'un çöp toplayıcısı tarafından otomatik olarak yönetilir.
    # Bu nedenle, bağlantıyı manuel olarak kapatmaya gerek yoktur.

    
