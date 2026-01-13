#!/usr/bin/env python3
"""
Telegram Bot para CREG - Bot de Preguntas sobre Regulación de Energía
Integración: Supabase (pgvector) + OpenAI GPT

Flujo:
1. Usuario envía pregunta por Telegram
2. Bot busca chunks relevantes en Supabase
3. Bot genera respuesta con OpenAI
4. Bot devuelve respuesta + referencias a normas
"""

import logging
import asyncio
from typing import Optional, Dict, List

from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from src.config import (
    TELEGRAM_BOT_TOKEN,
    OPENAI_API_KEY,
    SUPABASE_URL,
    SUPABASE_KEY,
)
from src.agent import CREGAgent

# ============ LOGGING ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============ AGENTE CREG ============
agent = CREGAgent()

# ============ HANDLERS ============

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Comando /start"""
    welcome = """
🤖 **Bienvenido al Bot CREG**

Soy un asistente experto en regulación de energía y gas en Colombia.

📋 **Comandos:**
/start - Ver este mensaje
/help - Ayuda y ejemplos
/search - Buscar normas (escribe: /search ¿tu pregunta?)
/status - Estado del sistema

💬 **Cómo usar:**
Simplemente escribe tu pregunta sobre regulación CREG y te daré la respuesta basada en las normas oficiales.

📚 **Ejemplos de preguntas:**
- ¿Cuál es la metodología para calcular tarifas?
- ¿Cuáles son los estándares de calidad de servicio?
- ¿Cómo se expanden las redes de distribución?
"""
    await update.message.reply_text(welcome, parse_mode="Markdown")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Comando /help"""
    help_text = """
📖 **Ayuda - Cómo usar el Bot CREG**

**Tipos de preguntas que puedo responder:**
✅ Normativa regulatoria (Resoluciones CREG)
✅ Metodologías de cálculo de tarifas
✅ Estándares de calidad de servicio
✅ Procedimientos administrativos
✅ Definiciones técnicas

**Formato de respuesta:**
1️⃣ Respuesta a tu pregunta
2️⃣ Normas relevantes utilizadas (Resolución, Año, Similitud)
3️⃣ Fragmentos de texto de la norma
4️⃣ Enlaces a documentos

**Tips:**
- Sé específico en tus preguntas
- Usa términos técnicos si es posible
- Puedo responder en español

**Problemas?**
Usa /status para verificar conectividad
"""
    await update.message.reply_text(help_text, parse_mode="Markdown")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Comando /status - Estado del sistema"""
    try:
        health = agent.vectordb.health_check()
        status_msg = "🟢 Sistema OPERATIVO" if health else "🔴 Sistema CON PROBLEMAS"
        await update.message.reply_text(f"{status_msg}\n\n✅ Supabase: {'Conectado' if health else 'Desconectado'}")
    except Exception as e:
        logger.error(f"Error en status: {e}")
        await update.message.reply_text(f"❌ Error verificando estado: {str(e)[:100]}")


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Comando /search - Búsqueda directa"""
    if not context.args:
        await update.message.reply_text("Uso: /search ¿Tu pregunta sobre CREG?")
        return
    
    query = " ".join(context.args)
    await handle_message_internal(update, context, query)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler para mensajes de texto normales"""
    user_message = update.message.text
    await handle_message_internal(update, context, user_message)


async def handle_message_internal(
    update: Update, context: ContextTypes.DEFAULT_TYPE, query: str
) -> None:
    """Lógica interna para procesar pregunta"""
    user_id = update.effective_user.id
    username = update.effective_user.first_name or "Usuario"
    
    logger.info(f"[{user_id}] {username}: {query[:100]}")
    
    # Mensaje de "escribiendo..."
    typing_msg = await update.message.reply_text("⏳ Buscando normas relevantes...")
    
    try:
        # 1. Buscar normas
        logger.info(f"[{user_id}] Buscando normas...")
        normas = agent.search_normas(query, n_results=3)
        
        if not normas:
            await typing_msg.edit_text(
                "❌ No encontré normas relevantes para tu pregunta.\n\n"
                "Intenta reformular la pregunta con términos más específicos."
            )
            return
        
        # 2. Construir contexto
        context_text = agent.build_context(normas)
        
        # 3. Generar respuesta
        logger.info(f"[{user_id}] Generando respuesta...")
        respuesta = agent.generate_response(query, context_text)
        
        # 4. Formatear respuesta para Telegram
        response_msg = format_telegram_response(query, respuesta, normas)
        
        # Actualizar mensaje con respuesta
        await typing_msg.edit_text(response_msg, parse_mode="Markdown")
        
        logger.info(f"[{user_id}] ✅ Respuesta enviada")
        
    except Exception as e:
        logger.error(f"[{user_id}] Error procesando pregunta: {e}")
        await typing_msg.edit_text(
            f"❌ Error procesando tu pregunta:\n\n{str(e)[:200]}",
            parse_mode="Markdown"
        )


def format_telegram_response(
    pregunta: str, respuesta: str, normas: Optional[List[Dict]]
) -> str:
    """Formatea la respuesta para Telegram"""
    
    # Limitar respuesta a 4096 caracteres (límite Telegram)
    respuesta = respuesta[:2000] if respuesta else "Sin respuesta"
    
    msg = f"""
*📌 Tu Pregunta:*
{pregunta}

*✅ Respuesta:*
{respuesta}

"""
    
    if normas:
        msg += "*📚 Normas Utilizadas:*\n"
        for i, n in enumerate(normas, 1):
            norma_num = n.get("norma_numero", "N/A")
            año = n.get("año", "N/A")
            similitud = n.get("similitud", 0)
            url = n.get("url", "")
            
            msg += f"\n[{i}] Resolución {norma_num} ({año})\n"
            msg += f"    📊 Similitud: {similitud*100:.1f}%\n"
            
            if url:
                msg += f"    🔗 [Ver documento]({url})\n"
    
    return msg


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler de errores global"""
    logger.error(f"Error no manejado: {context.error}")
    if update and update.message:
        try:
            await update.message.reply_text(
                "❌ Error interno del bot. Por favor intenta más tarde."
            )
        except Exception as e:
            logger.error(f"Error enviando mensaje de error: {e}")


async def set_commands(application: Application) -> None:
    """Configura los comandos visibles en Telegram"""
    commands = [
        BotCommand("start", "Ver mensaje de bienvenida"),
        BotCommand("help", "Ver ayuda y ejemplos"),
        BotCommand("status", "Verificar estado del sistema"),
        BotCommand("search", "Buscar normas (escribe: /search tu pregunta)"),
    ]
    await application.bot.set_my_commands(commands)


async def main() -> None:
    """Función principal - inicia el bot"""
    
    # Validar configuración
    if not TELEGRAM_BOT_TOKEN:
        raise ValueError("❌ TELEGRAM_BOT_TOKEN no está configurado en .env")
    
    logger.info("=" * 60)
    logger.info("🤖 Iniciando Bot CREG (Telegram)")
    logger.info("=" * 60)
    
    # Crear aplicación
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Registrar handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("search", search_command))
    
    # Handler para mensajes de texto (debe ser último)
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )
    
    # Error handler
    application.add_error_handler(error_handler)
    
    # Configurar comandos
    await set_commands(application)
    
    logger.info("✅ Bot inicializado correctamente")
    logger.info("📱 Escuchando mensajes...")
    
    # Iniciar polling
    await application.run_polling()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⏸️  Bot detenido por el usuario")
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
