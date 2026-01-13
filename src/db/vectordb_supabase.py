"""
src/db/vectordb_supabase.py
VectorDB para Supabase pgvector + RPC match_chunks + Hybrid Search (Async)
"""

import logging
import asyncio
import re
from typing import List, Dict, Optional

from supabase import create_client, Client
from openai import AsyncOpenAI

from src.config import (
    SUPABASE_URL,
    SUPABASE_KEY,
    OPENAI_API_KEY,
    OPENAI_EMBEDDING_MODEL,
)

logger = logging.getLogger(__name__)


class VectorDBSupabase:
    """
    Wrapper para Supabase pgvector + RPC match_chunks + Hybrid Search.
    Ahora soporta operaciones asíncronas.
    """

    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Faltan SUPABASE_URL o SUPABASE_KEY en .env")
        if not OPENAI_API_KEY:
            raise ValueError("Falta OPENAI_API_KEY en .env")

        # El cliente de Supabase es síncrono por defecto, lo usaremos con to_thread para no bloquear
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.openai = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.embedding_model = OPENAI_EMBEDDING_MODEL or "text-embedding-3-small"

        logger.info("✅ VectorDBSupabase inicializado (Async + Hybrid Search)")

    async def generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        Genera embedding con OpenAI (Async).
        """
        text = (text or "").strip()
        if not text:
            return None

        try:
            resp = await self.openai.embeddings.create(
                model=self.embedding_model,
                input=text,
            )
            return resp.data[0].embedding
        except Exception as e:
            logger.error(f"❌ Error generando embedding OpenAI: {e}")
            return None

    async def search_by_text(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Búsqueda por texto exacto (número y año) para mejorar precisión.
        """
        # Buscar todos los números en el mensaje
        nums = re.findall(r"\b(\d+)\b", query)
        if not nums:
            return []
        
        # Intentar identificar número de resolución y año
        # El año suele ser el que está entre 1990-2026 o el que viene después de 'de', 'del'
        numero = nums[0]
        año = None
        
        for n in nums:
            val = int(n)
            # Años de 4 dígitos o años de 2 dígitos (90-99 o 00-26)
            if 1990 <= val <= 2026:
                año = str(val)
            elif (90 <= val <= 99) or (0 <= val <= 26):
                # Convertir 95 -> 1995, 24 -> 2024
                año = str(1900 + val) if val >= 90 else str(2000 + val)

        try:
            logger.info(f"🔎 Búsqueda textual: numero={numero}, año={año}")
            
            # Construir query base
            query_builder = self.supabase.table("normas").select("id, numero, año, url, titulo")
            
            # Filtro por número (exacto o ilike si es corto)
            if len(numero) >= 1:
                query_builder = query_builder.ilike("numero", f"%{numero}")
            
            # Filtro por año si se detectó
            if año:
                query_builder = query_builder.eq("año", año)
            
            res = await asyncio.to_thread(lambda: query_builder.limit(limit).execute())
            
            results = []
            for row in res.data:
                # Traer el primer chunk para contexto
                chunks = await asyncio.to_thread(
                    lambda: self.supabase.table("chunks")
                    .select("texto, indice")
                    .eq("norma_id", row["id"])
                    .limit(1)
                    .execute()
                )
                
                texto = chunks.data[0]["texto"] if chunks.data else "Documento encontrado."
                
                results.append({
                    "documento": texto[:300],
                    "distancia": 0.0,
                    "metadata": {
                        "norma_id": row["id"],
                        "normanumero": row.get("numero", "N/A"),
                        "año": row.get("año", "N/A"),
                        "url": row.get("url", ""),
                        "similarity": 1.0,
                        "indice": chunks.data[0]["indice"] if chunks.data else 0,
                        "texto_completo": texto,
                        "fuente": "búsqueda_textual"
                    }
                })
            return results
        except Exception as e:
            logger.error(f"⚠️ Error en búsqueda textual avanzada: {e}")
            return []

    async def search_by_vector(
        self,
        query: str,
        n_results: int = 3,
        threshold: float = 0.5,
    ) -> List[Dict]:
        """
        Busca chunks similares usando embeddings (Vector Search).
        """
        query_embedding = await self.generate_embedding(query)
        if not query_embedding:
            return []

        try:
            # RPC match_chunks (usamos to_thread para Supabase sync)
            rpc = await asyncio.to_thread(
                lambda: self.supabase.rpc(
                    "match_chunks",
                    {
                        "query_embedding": query_embedding,
                        "match_threshold": threshold,
                        "match_count": n_results,
                    },
                ).execute()
            )

            if not rpc.data:
                return []

            results = []
            for row in rpc.data:
                norma_id = row.get("norma_id")
                indice = row.get("indice")
                similarity = float(row.get("similarity", 0) or 0)

                tr = await asyncio.to_thread(
                    lambda: self.supabase.table("chunks")
                    .select("texto, normas(numero, año, url)")
                    .eq("norma_id", norma_id)
                    .eq("indice", indice)
                    .limit(1)
                    .execute()
                )

                if not tr.data:
                    continue

                row0 = tr.data[0]
                texto_completo = row0.get("texto") or ""
                norma_meta = row0.get("normas") or {}

                results.append({
                    "documento": texto_completo[:300],
                    "distancia": 1 - similarity,
                    "metadata": {
                        "norma_id": norma_id,
                        "normanumero": norma_meta.get("numero", "N/A"),
                        "año": norma_meta.get("año", "N/A"),
                        "url": norma_meta.get("url", ""),
                        "similarity": similarity,
                        "indice": indice,
                        "texto_completo": texto_completo,
                        "fuente": "búsqueda_vectorial"
                    }
                })
            return results
        except Exception as e:
            logger.error(f"⚠️ Error en búsqueda vectorial: {e}")
            return []

    async def search(
        self,
        query: str,
        n_results: int = 3,
        threshold: float = 0.5,
    ) -> Optional[List[Dict]]:
        """
        Pipeline Hybrid Search: Texto + Vectorial.
        """
        logger.info(f"🔍 Búsqueda Híbrida iniciando: {query}")
        
        # Ejecutamos ambas búsquedas en paralelo
        text_task = self.search_by_text(query)
        vector_task = self.search_by_vector(query, n_results, threshold)
        
        text_results, vector_results = await asyncio.gather(text_task, vector_task)
        
        # Combinar resultados (priorizando texto si hay match directo)
        seen_norms = set()
        final_results = []
        
        for r in text_results + vector_results:
            norm_id = r["metadata"]["norma_id"]
            if norm_id not in seen_norms:
                final_results.append(r)
                seen_norms.add(norm_id)
            
            if len(final_results) >= n_results:
                break
                
        return final_results if final_results else None

    async def health_check(self) -> bool:
        """
        Verifica que Supabase esté disponible (Async).
        """
        try:
            _ = await asyncio.to_thread(
                lambda: self.supabase.table("chunks").select("id").limit(1).execute()
            )
            return True
        except:
            return False

# ============ FIN src/db/vectordb_supabase.py ============