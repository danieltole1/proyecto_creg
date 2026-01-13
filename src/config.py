import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")




# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# App
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
TIMEZONE = os.getenv("TIMEZONE", "America/Bogota")

# Validación mínima
if not TELEGRAM_BOT_TOKEN:
    raise ValueError(f"[ERROR] TELEGRAM_BOT_TOKEN no encontrado en el entorno de Railway ni en .env. Variables detectadas: {list(os.environ.keys())}")

if not OPENAI_API_KEY:
    raise ValueError("[ERROR] OPENAI_API_KEY no encontrado.")

print("[OK] Configuración cargada correctamente")
