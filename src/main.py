#!/usr/bin/env python3
import os
from dotenv import load_dotenv

load_dotenv()

print("=" * 60)
print("✅ CREG Vectorizer Bot")
print("=" * 60)
print("")
print("Configuración cargada:")
print(f" - Telegram: {'✅' if os.getenv('TELEGRAM_BOT_TOKEN') else '❌'}")
print(f" - OpenAI: {'✅' if os.getenv('OPENAI_API_KEY') else '❌'}")
print("")
print("=" * 60)

if __name__ == "__main__":
    print("Bot listo para desarrollo")
