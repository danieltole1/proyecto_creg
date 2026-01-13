# src/telegram_bot_lambda.py

import logging
import json
import asyncio
from typing import Optional, List, Dict
from datetime import datetime

from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.constants import ChatAction

from supabase import create_client, Client
from openai import OpenAI

from src.config import (
    TELEGRAM_TOKEN,
    SUPABASE_URL,
    SUPABASE_KEY,
    OPENAI_API_KEY,
    OPENAI_MODEL,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CREGBot:
    """Bot Telegram con RAG integrado para consultas CREG"""
    
    def __init__(self):
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.openai = OpenAI(api_key=OPENAI_API_KEY)
        self.model = OPENAI_MODEL or "gpt-3.5-turbo"
        
        logger.info("✅ CREGBot inicializado (RAG + Supabase + OpenAI)")
    
    # -------- RAG: Búsqueda semántica en Supabase --------
    
    async def search_relevant_chunks(self, query: str, top_k: int = 5) -> List[Dict]:
        """
        Buscar chunks relevantes basados en similitud semántica.
        
        Args:
            query: Pregunta del usuario
            top_k: Cantidad de chunks a retornar
            
        Returns:
            Lista de chunks relevantes con contexto
        """
        try:
            # Generar embedding de la pregunta
            embedding_response = self.openai.embeddings.create(
                model="text-embedding-3-small",
                input=query,
            )
            query_embedding = embedding_response.data[0].embedding
            
            # Búsqueda en Supabase (similarity search)
            # Supabase tiene soporte para pgvector
            result = self.supabase.rpc(
                "match_chunks",
                {
                    "query_embedding": query_embedding,
                    "match_count": top_k,
                    "similarity_threshold": 0.3,
                }
            ).execute()
            
            chunks = result.data if result.data else []
            
            if chunks:
                logger.info(f"🔍 Encontrados {len(chunks)} chunks relevantes")
            else:
                logger.warning(f"⚠️ No se encontraron chunks relevantes para: {query}")
            
            return chunks
            
        except Exception as e:
            logger.error(f"❌ Error en búsqueda RAG: {e}")
            return []
    
    def build_rag_context(self, chunks: List[Dict]) -> str:
        """
        Construir contexto para el prompt de OpenAI.
        
        Args:
            chunks: Lista de chunks relevantes
            
        Returns:
            String con el contexto formateado
        """
        if not chunks:
            return "No se encontraron normas relevantes."
        
        context = "=== NORMAS RELEVANTES DE CREG ===\n\n"
        
        for i, chunk in enumerate(chunks, 1):
            norma_numero = chunk.get("numero", "N/A")
            norma_año = chunk.get("año", "N/A")
            tipo_chunk = chunk.get("tipo_chunk", "texto")
            texto = chunk.get("texto", "")
            
            context += f"[{i}] Resolución {norma_numero} ({norma_año})\n"
            context += f"    Tipo: {tipo_chunk}\n"
            context += f"    Contenido: {texto[:300]}...\n\n"
        
        return context
    
    # -------- OpenAI: Generar respuestas con contexto --------
    
    async def generate_response(self, query: str, context: str) -> str:
        """
        Generar respuesta usando OpenAI con contexto RAG.
        
        Args:
            query: Pregunta del usuario
            context: Contexto de chunks relevantes
            
        Returns:
            Respuesta generada
        """
        try:
            prompt = f"""Eres un asistente experto en regulaciones de CREG (Comisión de Regulación de Energía y Gas de Colombia).

PREGUNTA DEL USUARIO:
{query}

CONTEXTO DE NORMAS RELEVANTES:
{context}

Instrucciones:
1. Responde basándote PRINCIPALMENTE en las normas proporcionadas
2. Si la información no está en las normas, indícalo claramente
3. Usa un tono profesional y educado
4. Si hay artículos específicos, cítalos
5. Responde en español
6. Si no sabes la respuesta, di "No encontré información relevante en las normas CREG disponibles"

RESPUESTA:"""

            response = self.openai.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "Eres un asistente experto en normativas de CREG. Responde de manera clara, precisa y profesional."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,
                max_tokens=1000,
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"❌ Error generando respuesta: {e}")
            return "Disculpa, hubo un error al procesar tu pregunta. Intenta de nuevo."
    
    # -------- Handlers Telegram --------
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler para /start"""
        welcome_msg = """
🤖 **Bienvenido al Bot CREG**

Soy tu asistente para consultas sobre regulaciones de la Comisión de Regulación de Energía y Gas (CREG) de Colombia.

**Comandos disponibles:**
/start - Ver este mensaje
/help - Ayuda
/search - Buscar una resolución

**Cómo usar:**
Simplemente envía tu pregunta sobre regulaciones CREG y te proporcionaré información basada en las normas oficiales.

Ejemplo: "¿Qué dice la resolución sobre transporte de electricidad?"
        """
        await update.message.reply_text(welcome_msg, parse_mode="Markdown")
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler para /help"""
        help_msg = """
**Ayuda - Bot CREG**

Este bot utiliza Retrieval-Augmented Generation (RAG) para responder preguntas sobre regulaciones CREG.

**Cómo funciona:**
1. Haces una pregunta
2. El bot busca las normas más relevantes en la base de datos
3. Genera una respuesta basada en esas normas

**Ejemplos de preguntas:**
- ¿Cuáles son los requisitos para transportistas?
- ¿Qué dice sobre protección del consumidor?
- ¿Cuáles son las obligaciones de distribuidores?

**Limitaciones:**
- La información viene de normas CREG hasta 2024
- Para consultas legales precisas, contacta a CREG directamente
        """
        await update.message.reply_text(help_msg, parse_mode="Markdown")
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler principal para mensajes de texto"""
        query = update.message.text
        
        logger.info(f"📨 Pregunta recibida: {query}")
        
        # Mostrar indicador de "escribiendo"
        await update.message.chat.send_action(ChatAction.TYPING)
        
        try:
            # Paso 1: Buscar chunks relevantes (RAG)
            chunks = await self.search_relevant_chunks(query, top_k=5)
            
            # Paso 2: Construir contexto
            context = self.build_rag_context(chunks)
            
            # Paso 3: Generar respuesta con OpenAI
            response = await self.generate_response(query, context)
            
            # Paso 4: Enviar respuesta
            await update.message.reply_text(response, parse_mode="Markdown")
            
            logger.info("✅ Respuesta enviada exitosamente")
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje: {e}")
            await update.message.reply_text(
                "Disculpa, hubo un error al procesar tu pregunta. Por favor intenta de nuevo."
            )
    
    # -------- Inicialización de Application --------
    
    def build_application(self) -> Application:
        """Construir Application de python-telegram-bot"""
        app = Application.builder().token(TELEGRAM_TOKEN).build()
        
        # Agregar handlers
        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(CommandHandler("help", self.help_command))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        return app


# -------- AWS Lambda Handler --------

bot_instance = None

def lambda_handler(event, context):
    """
    Handler para AWS Lambda.
    Procesa webhooks de Telegram.
    """
    global bot_instance
    
    if bot_instance is None:
        bot_instance = CREGBot()
        app = bot_instance.build_application()
    else:
        app = bot_instance.build_application()
    
    try:
        # Parsear el body del evento Lambda
        if isinstance(event.get("body"), str):
            body = json.loads(event["body"])
        else:
            body = event.get("body", {})
        
        # Crear Update desde el webhook
        update = Update.de_json(body, app.bot)
        
        # Procesar update de forma síncrona
        asyncio.run(app.process_update(update))
        
        return {
            "statusCode": 200,
            "body": json.dumps({"ok": True}),
        }
        
    except Exception as e:
        logger.error(f"❌ Error en Lambda handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"ok": False, "error": str(e)}),
        }


# -------- Local polling (para testing) --------

async def main_polling():
    """Ejecutar bot en modo polling (local)"""
    bot = CREGBot()
    app = bot.build_application()
    
    logger.info("🚀 Bot iniciado en modo polling...")
    await app.run_polling()


if __name__ == "__main__":
    asyncio.run(main_polling())