#!/usr/bin/env python3
import logging
import sys

from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from src.config import TELEGRAM_BOT_TOKEN
from src.agent import CREGAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

agent = CREGAgent()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info(f"[{update.effective_user.id}] /start")
    await update.message.reply_text("Bienvenido al Bot CREG")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info(f"[{update.effective_user.id}] /help")
    await update.message.reply_text("Ayuda: Pregunta sobre regulacion CREG")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info(f"[{update.effective_user.id}] /status")
    try:
        health = agent.vectordb.health_check()
        msg = "Sistema OK" if health else "Sistema ERROR"
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)[:100]}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info(">>> MENSAJE RECIBIDO <<<")
    user_id = update.effective_user.id
    query = update.message.text
    
    logger.info(f"[{user_id}] {query[:100]}")
    
    msg = await update.message.reply_text("Buscando...")
    
    try:
        normas = agent.search_normas(query, n_results=3)
        
        if not normas:
            await msg.edit_text("Sin resultados")
            return
        
        context_text = agent.build_context(normas)
        respuesta = agent.generate_response(query, context_text)
        
        # Construir respuesta completa
        response = f"Pregunta: {query}\n\nRespuesta:\n{respuesta}"
        
        # Si la respuesta es muy larga, dividirla en chunks
        if len(response) > 4096:
            chunks = [response[i:i+4000] for i in range(0, len(response), 4000)]
            await msg.edit_text(chunks[0])
            for chunk in chunks[1:]:
                await update.message.reply_text(chunk)
        else:
            await msg.edit_text(response)
        
        logger.info(f"[{user_id}] Respondido")
        
    except Exception as e:
        logger.error(f"[{user_id}] Error: {e}", exc_info=True)
        await msg.edit_text(f"Error: {str(e)[:100]}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Error: {context.error}", exc_info=True)

def main() -> None:
    logger.info("=" * 60)
    logger.info("Iniciando Bot CREG")
    logger.info("=" * 60)
    
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)
    
    logger.info("Handlers registrados")
    logger.info("Escuchando mensajes...")
    
    app.run_polling()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot detenido")
    except Exception as e:
        logger.error(f"Error fatal: {e}", exc_info=True)
