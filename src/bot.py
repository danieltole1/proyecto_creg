#!/usr/bin/env python3
# src/bot.py
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from src.config import TELEGRAM_BOT_TOKEN
from src.core.agent import CREGAgent

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


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
        await update.message.reply_text("CREG Bot (Supabase + OpenAI). Escribe tu pregunta.")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Escribe tu pregunta sobre normas CREG y te respondo con fuentes.")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_message = update.message.text
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
                    msg += f"- Resolución {n.get('norma_numero')} ({n.get('año')}) [{n.get('fuente', '')}]\n"

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
