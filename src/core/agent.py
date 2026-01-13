# src/core/agent.py
"""
Agente que integra Supabase (Hybrid Search) + OpenAI (GPT) - Versión Async
"""

import logging
from typing import List, Dict, Optional

from openai import AsyncOpenAI

from src.config import OPENAI_API_KEY, OPENAI_MODEL
from src.db.vectordb_supabase import VectorDBSupabase

logger = logging.getLogger(__name__)


class CREGAgent:
    """
    Pipeline Async:
    1) Busca chunks relevantes en Supabase (Hybrid Search: Texto + Vector)
    2) Construye contexto
    3) Genera respuesta con OpenAI (Async)
    """

    def __init__(self):
        self.vectordb = VectorDBSupabase()
        self.client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.model = OPENAI_MODEL
        logger.info("✅ Agent inicializado (Async Pipeline)")

    async def search_normas(self, query: str, n_results: int = 3) -> Optional[List[Dict]]:
        # La búsqueda ahora es asíncrona e híbrida
        results = await self.vectordb.search(query, n_results=n_results, threshold=0.4)
        if not results:
            return None

        normas = []
        for i, r in enumerate(results, 1):
            meta = r.get("metadata", {})
            similarity = float(meta.get("similarity", 0) or 0)

            normas.append(
                {
                    "rank": i,
                    "similitud": round(similarity, 3),
                    "fragmento": (meta.get("texto_completo") or r.get("documento") or "")[:300],
                    "norma_id": meta.get("norma_id"),
                    "norma_numero": meta.get("normanumero"),
                    "año": meta.get("año"),
                    "url": meta.get("url"),
                    "fuente": meta.get("fuente", "desconocida")
                }
            )
        return normas

    def build_context(self, normas: List[Dict]) -> str:
        if not normas:
            return "No se encontraron normas relevantes en la base de datos."

        context = "NORMAS RELEVANTES ENCONTRADAS EN CREG:\n\n"
        for n in normas:
            context += f"[{n['rank']}] Resolución {n['norma_numero']} ({n['año']})\n"
            context += f" Similitud/Relevancia: {n['similitud']*100:.1f}%\n"
            context += f" Fragmento: {n['fragmento']}...\n"
            context += f" URL: {n['url']}\n\n"
        return context

    async def generate_response(self, user_question: str, context: str) -> str:
        system_prompt = (
            "Eres un asistente experto en regulación de energía y gas en Colombia (CREG). "
            "Responde en español, claro y conciso. Máximo 500 palabras. "
            "Si encuentras una norma por búsqueda textual exacta, dale prioridad."
        )

        user_prompt = f"""CONTEXTO DE NORMAS RELEVANTES:
{context}

PREGUNTA DEL USUARIO:
{user_question}

INSTRUCCIONES:
1. Responde basándote en las normas proporcionadas.
2. Cita las resoluciones que usaste (ej: \"Resolución 502-149 de 2025\").
3. Si no hay información suficiente, di: \"No encontré información en las normas disponibles\".
"""

        try:
            resp = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.2,
            )
            return (resp.choices[0].message.content or "").strip() or "No pude generar respuesta."
        except Exception as e:
            logger.error(f"❌ Error con OpenAI: {e}")
            return f"Error al procesar: {str(e)}"

    async def answer(self, user_question: str) -> Dict:
        # Todo el pipeline es ahora awaitable
        normas = await self.search_normas(user_question, n_results=3)
        context = self.build_context(normas) if normas else "No hay normas disponibles."
        respuesta = await self.generate_response(user_question, context)

        return {
            "pregunta": user_question,
            "respuesta": respuesta,
            "normas_usadas": normas or [],
        }

# ============ FIN src/core/agent.py ============
