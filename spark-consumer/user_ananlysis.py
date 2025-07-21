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