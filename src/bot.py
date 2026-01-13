#!/usr/bin/env python3
# src/bot.py
import logging
import asyncio
import time
from telegram import Update, constants
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from src.config import TELEGRAM_BOT_TOKEN
from src.core.agent import CREGAgent

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Configuración de Experiencia de Usuario
SESSION_TIMEOUT = 3 * 3600  # 3 horas en segundos
WELCOME_MSG = (
    "🤖 *¡Bienvenido al Bot de Consulta CREG!* ⚖️\n\n"
    "Soy tu asistente experto en la normativa de energía y gas en Colombia. "
    "Puedo ayudarte a encontrar resoluciones y explicarte la regulación.\n\n"
    "📖 *¿Cómo usarme?*\n"
    "Simplemente escribe tu duda o el número de una resolución. Por ejemplo:\n"
    "• _\"¿Cuál es la fórmula tarifaria de gas?\"_\n"
    "• _\"Busca normas de 2024 sobre energía.\"_\n"
    "• _\"Resolución 101-042\"_\n\n"
    "💡 *Tip:* Soy proactivo buscando fuentes oficiales para mis respuestas."
)

HELP_TOAST = "👋 *¡Hola de nuevo!* Recuerda que puedes preguntarme sobre cualquier Resolución CREG o tema regulatorio de energía y gas."


class CREGBot:
    def __init__(self):
        self.app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        self.agent = CREGAgent()
        self.setup_handlers()

    def setup_handlers(self):
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Al usar /start, reseteamos el cronómetro de sesión
        context.user_data["last_interaction"] = time.time()
        await update.message.reply_text(WELCOME_MSG, parse_mode=constants.ParseMode.MARKDOWN)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(WELCOME_MSG, parse_mode=constants.ParseMode.MARKDOWN)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_message = update.message.text
        now = time.time()
        last_interaction = context.user_data.get("last_interaction")

        # Lógica de Sesión: Mostrar recordatorio si han pasado > 3 horas o es primera vez
        if last_interaction is None or (now - last_interaction) > SESSION_TIMEOUT:
            # Si es un mensaje muy corto (hola, etc), enviamos el mensaje de bienvenida completo
            if len(user_message.strip()) < 5:
                await update.message.reply_text(WELCOME_MSG, parse_mode=constants.ParseMode.MARKDOWN)
                context.user_data["last_interaction"] = now
                return
            
            # Si es una pregunta, enviamos un pequeño saludo previo
            await update.message.reply_text(HELP_TOAST, parse_mode=constants.ParseMode.MARKDOWN)

        # Actualizar timestamp de interacción
        context.user_data["last_interaction"] = now

        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

        try:
            # Ahora la llamada es asíncrona y soporta múltiples usuarios sin bloquear
            result = await self.agent.answer(user_message)
            respuesta = result.get("respuesta", "")
            normas = result.get("normas_usadas", [])

            msg = respuesta
            if normas:
                msg += "\n\n📚 Normas consultadas:\n"
                for n in normas:
                    msg += f"- Resolución {n.get('norma_numero')} ({n.get('año')})\n"

            # Telegram limita 4096 chars
            for i in range(0, len(msg), 4096):
                await update.message.reply_text(msg[i:i+4096])

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            await update.message.reply_text("❌ Error interno. Intenta de nuevo.")

    def run(self):
        logger.info("🤖 Bot CREG iniciando (Async Ready)...")
        self.app.run_polling()


if __name__ == "__main__":
    CREGBot().run()
