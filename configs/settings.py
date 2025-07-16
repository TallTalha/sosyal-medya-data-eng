#configs/settings.py
"""
Bu modül, uygulama için gerekli ayarları ve API anahtarlarını içerir.
Ayarlar, çevresel değişkenlerden alınır ve uygulama genelinde kullanılabilir.
"""
import os
from dotenv import load_dotenv

# .env değişkenlerini yükler
load_dotenv()

# Proje İçin Gerekli X APIv2 Anahtarları
X_API_KEY = os.getenv('X_API_KEY')
X_API_KEY_SECRET = os.getenv('X_API_KEY_SECRET')
X_BEARER_TOKEN = os.getenv('X_BEARER_TOKEN')



