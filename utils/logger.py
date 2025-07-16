# utils/logger.py
"""
Bu modül, uygulama genelinde kullanılacak logger'ı ayarlar.
Logger, hem konsola hem de dosyaya loglama yapar.
Log dosyaları, uygulamanın kök dizinindeki logs klasöründe saklanır.
Log dosyaları, modül adıyla adlandırılır ve her modül için ayrı bir log dosyası oluşturulur.
Loglama formatı, tarih, modül adı, log seviyesi ve mesajı içerir.
Log dosyaları, 10 MB boyutuna ulaştığında yeni bir dosya oluşturur ve en fazla 5 yedek dosya tutar. 
"""

import logging
import logging.handlers
import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = os.path.join(PROJECT_ROOT, 'logs')

# # Log için ana dizin yoksa oluştur
os.makedirs(LOG_FILE, exist_ok=True)

def setup_logger(name: str, level=logging.INFO):
    """
    Logger'ı ayarlar ve döndürür. Her logger, log dosyasında kendi modül adıyla ayrı bir dosyaya yazılır.
    Örneğin. main.py modülü için logs/main.log dosyasına yazılır.
        Args:
            name (str): Logger'ın adı, genellikle modül adı olarak kullanılır.
            level (int): Log seviyesini belirler. Varsayılan olarak INFO seviyesidir.
        Returns:
            logging.Logger: Ayarlanmış logger nesnesi.
    """

    # Modül için özel log dosyası oluşturur
    log_file = os.path.join(LOG_FILE, f"{name}.log")

    # Loglama formatı
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Log dosyasının boyutunu sınırlamak için RotatingFileHandler kullanırız
    # 10 MB boyutuna ulaştığında yeni bir dosya oluşturur ve en fazla 5 yedek dosya tutar
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(formatter)

    # Konsol çıktısı için StreamHandler kullanırız
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Logger Oluştururuz
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Handler kontrolü yapılarak handler ekleriz
    if not logger.hasHandlers():
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger
    
    

