import os
from dotenv import load_dotenv

load_dotenv()

api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")

# ID канала или чата, куда бот будет отправлять уведомления
channel = os.getenv("CHANNEL")

# Включить или отключить автоматический вывод средств
avto_vivod = True

# Тег, на который будут отправляться чеки при автовыводе
avto_vivod_tag = 'username'

# Автоматическая отписка от каналов после выполнения действий
avto_otpiska = True

# Включить антикаптчу (распознавание капчи через OCR)
anti_captcha = True

OCR_API_KEY = 'K88711811888957' # Ключ API для OCR. Я его нашел на лолзе. Так что мне пофигу.
MAX_RETRIES = 3  # Максимальное количество попыток для операций
RETRY_DELAY = 5  # Задержка между попытками в секундах
MAX_CACHED_CHECKS = 1000  # Максимальное количество кэшированных чеков
CLEANUP_INTERVAL = 3600  # Интервал очистки в секундах
JOIN_TIMEOUT = 30  # Таймаут для присоединения к каналам
REQUEST_TIMEOUT = 10  # Таймаут для запросов