# twitter_producer.py
"""
Twitter'dan MongoDB'ye veri akışı sağlayan bir Python uygulaması.
Bu uygulama, Twitter API'si aracılığıyla gelen tweet'leri MongoDB veritabanına kaydeder.
"""

# Ortam değişkenlerini configs/settings.py  dosyasından alır.
import pymongo.collation
import pymongo.collection
import pymongo.errors
from configs.settings import X_API_KEY, X_API_KEY_SECRET, X_BEARER_TOKEN

# Logger kurulumu için utils/logger.py dosyasından setup_logger fonksiyonunu alır.
from utils.logger import setup_logger
import logging

# X Api kullanımı ve mongoDB bağlantısı için gerekli kütüphaneler:
import tweepy
import pymongo
import pymongo.collection

logger = setup_logger("twitter_producer")

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
            consumer_secret=X_API_KEY_SECRET
        )
        logger.info("Twitter API istemcisi başarıyla oluşturuldu.")
        return client
    except Exception as e:
        logger.critical(f"Twitter API istemcisi oluşturulurken hata oluştu: {e}", exc_info=True)
        return None
    
def get_mongodb_collection(db_name: str = "social_media_db", collection_name:str = "raw_tweets"):
    """
    MongoDB'ye bağlanır ve istenen koleksiyon nesnesini döndürür.
    """
    try:
        logger.info(f"MongoDB bağlantısı kuruluyor... Veritabanı:{db_name} & Koleksiyon:{collection_name}")
        # Digital Ocean Sunucularındaki Uzak Makineye SSH ile bağlantı sağladığımız
        # için Sunucu IPv4 adresi olmadan localhost ifadesiyle bağlanabiliyoruz.
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        collection = db[collection_name]
        logger.info("MongoDB koleksiyonu başarıyla alındı.")
        return collection
    except Exception as e:
        logger.critical(f"MongoDB bağlantısı kurulurken hata oluştu: {e}",exc_info=True)
        return None

def _save_tweets_batch(tweets_batch: list, collection: pymongo.collection.Collection):
    """
    Gelen tweet listesini batch(toplu) olarak insert_many ile toplu olarak MongoDB'ye yazar.
    Kod tekrarını önleyen bir yardımcı fonksiyondur.
        Args:
            tweets (list): Liste formatındaki tweet listesidir.
            collection (pymongo.collection.Collection): Verilerin yazılacağı koleksiyon ismidir.
        Returns:
            None
    """
    if not tweets_batch:
        logger.info("Tweet listesi boş.")
        return
    
    try:
        logger.info("Tweet listesi koleksiyona yazılıyor.")
        collection.insert_many(tweets_batch, ordered=False)
        logger.info(f"{len(tweets_batch)} adet tweet içeren batch MongoDB'ye yazıldı.")
    except pymongo.errors.BulkWriteError as bwe:
        logger.warning("Toplu (batch) yazma işlemi sırasında bazı kayıtlar duplike olabilir: {bwe.details}")
    except Exception as e:
        logger.error(f"MondoDB'ye toplu yazma sırasında hata:{e}", exc_info=True)


    
def fetch_user_timeline(client: tweepy.Client, collection: pymongo.collection.Collection, username: str = "elonmusk", max_tweets: int = 200):
    """
    Belirli bil kullanıcının tweet'lerini alır ve MongoDB koleksiyonuna kaydeder.
        Args:
            client (tweepy.Client): Twitter API istemcisi.
            collection (pymongo.collection.Collection): MongoDB koleksiyonu.
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
        tweets_to_insert = []
        
        # Paginator sayesinde API limitleri yönetilir ve tweet'ler alınır.
        for tweet in tweepy.Paginator(
            client.get_users_tweets,
            id=user_id,
            max_results=100
        ).flatten(limit=max_tweets):
            
            tweets_to_insert.append(tweet)
            
            if len(tweets_to_insert) >= 100:
                _save_tweets_batch(tweets_to_insert,collection=collection)
                tweets_to_insert = []
        #Döngü bittiğinde 100'den az tweet varsa onları da yazarız
        _save_tweets_batch(tweets_to_insert,collection=collection)

        logger.info(f"{username} kullanıcısının tweet'leri çekildi ve MongoDB'ye yazıldı.")
    
    except Exception as e:
        logger.error(f"{username} kullanıcısının tweet'leri alınırken hata oluştu: {e}", exc_info=True)

def search_text_in_tweets(client: tweepy.Client, collection: pymongo.collection.Collection, max_tweets: int = 200, query: str = '"Büyük Veri" lang:tr -is:retweet'):
    """
    Belirli bir metni içeren tweet'leri alır ve MongoDB koleksiyonuna kaydeder.
        Args:
            client (tweepy.Client): Twitter API istemcisi.
            query (str): Aranacak metin. Varsayılan: '"Büyük Veri" lang:tr -is:retweet'.
            max_tweets (int): Alınacak maksimum tweet sayısı. Varsayılan: 200.
            collection (pymongo.collection.Collection): MongoDB koleksiyonu.
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
            max_results=100
        ).flatten(limit=max_tweets):
            
            tweets_to_insert.append(tweet)
            if len(tweets_to_insert) >= 100: 
                _save_tweets_batch(tweets_to_insert,collection=collection) 
                tweets_to_insert = [] # Toplu yazım sonrası liste temizlenir.

        # Listede 100'den az tweet klaydıysa onlarıda yazmak için tekrar fonksiyon çağrılır. 
        _save_tweets_batch(tweets_to_insert,collection=collection)

        logger.info(f"{query} metnini için arama ve MongoDB'ye yazma işlemi tamamlandı.")
    except Exception as e:
        logger.error(f"{query} metnini içeren tweet'ler alınırken hata oluştu: {e}", exc_info=True)
    
    
if __name__ == "__main__":
    logger.info("Twitter'dan tweet'ler MongoDB'ye kaydediliyor...")
    client = create_twitter_api_client()
    mongo_collection = get_mongodb_collection()

    if client and mongo_collection:
        # Belirli bir kullanıcının tweet'lerini alır ve MongoDB koleksiyonuna kaydeder.
        fetch_user_timeline(client, mongo_collection, username="elonmusk", max_tweets=200)
        
        # Belirli bir metni içeren tweet'leri alır ve MongoDB koleksiyonuna kaydeder.
        search_text_in_tweets(client, mongo_collection, max_tweets=200, query='"Büyük Veri" lang:tr -is:retweet')
    else:
        logger.error("Twitter API istemcisi veya MongoDB koleksiyonu oluşturulamadı. Uygulama sonlandırılıyor.")
    logger.info("twitter_producer.py sonlandırıldı.")
    # Uygulama sonlandırıldıktan sonra MongoDB bağlantısı otomatik olarak kapanır.
    # pymongo.MongoClient() ile oluşturulan bağlantı, Python'un çöp toplayıcı tarafından otomatik olarak yönetilir.
    # Bu nedenle, bağlantıyı manuel olarak kapatmaya gerek yoktur.

    
