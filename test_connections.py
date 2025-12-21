#!/usr/bin/env python3
# Test: Validar conectividad

import os
import sys
from dotenv import load_dotenv
import google.generativeai as genai
from sentence_transformers import SentenceTransformer

load_dotenv()

print("=" * 60)
print("TEST: Validación de Conectividad")
print("=" * 60)

print("\n[1] Verificando credenciales...")
telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
gemini_key = os.getenv("GOOGLE_API_KEY")

if telegram_token:
    print("✅ Telegram token cargado")
else:
    print("❌ Telegram token NO configurado")
    sys.exit(1)

if gemini_key:
    print("✅ Gemini API key cargado")
else:
    print("❌ Gemini API key NO configurado")
    sys.exit(1)

print("\n[2] Probando Gemini API...")
try:
    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel("gemini-flash-latest")
    response = model.generate_content("Hola, ¿estás funcionando?")
    print(f"✅ Gemini responde: {response.text[:80]}...")
except Exception as e:
    print(f"❌ Gemini ERROR: {str(e)}")
    sys.exit(1)

print("\n[3] Probando modelo de embeddings...")
try:
    print("   Descargando modelo (puede tardar 1-2 min la primera vez)...")
    model_emb = SentenceTransformer("all-MiniLM-L6-v2")
    embedding = model_emb.encode("Test")
    print(f"✅ Embeddings OK ({len(embedding)} dimensiones)")
except Exception as e:
    print(f"❌ Embeddings ERROR: {str(e)}")
    sys.exit(1)

print("\n" + "=" * 60)
print("✅✅✅ TODOS LOS TESTS PASARON")
print("=" * 60)
