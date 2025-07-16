# twitter_producer.py
"""
Twitter'dan MongoDB'ye veri akışı sağlayan bir Python uygulaması.
Bu uygulama, Twitter API'si aracılığıyla gelen tweet'leri MongoDB veritabanına kaydeder.
"""

# Ortam değişkenlerini configs/settings.py  dosyasından alır.
from configs.settings import X_API_KEY, X_API_KEY_SECRET, X_BEARER_TOKEN

# Logger kurulumu için utils/logger.py dosyasından setup_logger fonksiyonunu alır.
from utils.logger import setup_logger
import logging

# X Api kullanımı ve mongoDB bağlantısı için gerekli kütüphaneler:
import tweepy
import pymongo
import pymongo.collection

logger = setup_logger("twitter_producer")

logger.setLevel(logging.INFO)

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
        logger.critical(f"Twitter API istemcisi oluşturulurken hata oluştu: {e}")
        return None
    
def get_mongodb_collection():
    """
    MongoDB socail_media_db veritabanına bağlanır ve 'raw_tweets' koleksiyonunu döndürür.
        Args:
            None
        Returns:
            pymongo.collection.Collection: MongoDB 'raw_tweets' koleksiyonu.
    """
    try:
        logger.info("MongoDB bağlantısı kuruluyor...")
        # Digital Ocean Sunucularındaki Uzak Makineye SSH ile bağlantı sağladığımız
        # için Sunucu IPv4 adresi olmadan localhost ifadesiyle bağlanabiliyoruz.
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["social_media_db"]
        collection = db["raw_tweets"]
        logger.info("MongoDB koleksiyonu başarıyla alındı.")
        return collection
    except Exception as e:
        logger.critical(f"MongoDB bağlantısı kurulurken hata oluştu: {e}")
        return None
    
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
    logger.info(f"{username} kullanıcısının tweet'leri alınıyor...")
    try:
        # Kullanıcı sorgulanır ve  verileri alınır.
        user = client.get_user(username=username)
        
        if not user:
            logger.error(f"{username} kullanıcısı bulunamadı.")
            return
        
        user_id = user.data.id
        
        # Paginator sayesinde API limitleri yönetilir ve tweet'ler alınır.
        for tweet_batch in tweepy.Paginator(
            client.get_users_tweets,
            id=user_id,
            max_results=100
        ).flatten(limit=max_tweets):
            collection.insert_one(tweet_batch.data)
        logger.info(f"{username} kullanıcısının {max_tweets} tweet'i MongoDB koleksiyonuna kaydedildi.")
    
    except Exception as e:
        logger.error(f"{username} kullanıcısının tweet'leri alınırken hata oluştu: {e}")

def search_text_recent_tweets(client: tweepy.Client, collection: pymongo.collection.Collection, max_tweets: int = 200, query: str = '"Büyük Veri" lang:tr -is:retweet'):
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
        # Paginator sayesinde API limitleri yönetilir ve tweet'ler alınır.
        for tweet_batch in tweepy.Paginator(
            client.search_recent_tweets,
            query=query,
            max_results=100
        ).flatten(limit=max_tweets):
            collection.insert_one(tweet_batch.data)
        logger.info(f"{query} metnini içeren {max_tweets} tweet MongoDB koleksiyonuna kaydedildi.")
    except Exception as e:
        logger.error(f"{query} metnini içeren tweet'ler alınırken hata oluştu: {e}")
    
    
if __name__ == "__main__":
    logger.info("Twitter'dan tweet'ler MongoDB'ye kaydediliyor...")
    client = create_twitter_api_client()
    mongo_collection = get_mongodb_collection()

    if client and mongo_collection:
        # Belirli bir kullanıcının tweet'lerini alır ve MongoDB koleksiyonuna kaydeder.
        fetch_user_timeline(client, mongo_collection, username="elonmusk", max_tweets=200)
        
        # Belirli bir metni içeren tweet'leri alır ve MongoDB koleksiyonuna kaydeder.
        search_text_recent_tweets(client, mongo_collection, max_tweets=200, query='"Büyük Veri" lang:tr -is:retweet')
    else:
        logger.error("Twitter API istemcisi veya MongoDB koleksiyonu oluşturulamadı. Uygulama sonlandırılıyor.")
    logger.info("twitter_producer.py sonlandırıldı.")
    # Uygulama sonlandırıldıktan sonra MongoDB bağlantısı otomatik olarak kapanır.
    # pymongo.MongoClient() ile oluşturulan bağlantı, Python'un çöp toplayıcı tarafından otomatik olarak yönetilir.
    # Bu nedenle, bağlantıyı manuel olarak kapatmaya gerek yoktur.

    
