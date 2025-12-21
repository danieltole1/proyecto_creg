"""
src/vectordb_supabase.py
VectorDB para Supabase pgvector + RPC match_chunks (OpenAI embeddings 1536)

- Genera embeddings con OpenAI
- Consulta Supabase vía RPC match_chunks
- Recupera texto + metadata desde tabla chunks + relación normas
"""

import logging
from typing import List, Dict, Optional

from supabase import create_client, Client
from openai import OpenAI

from src.config import (
    SUPABASE_URL,
    SUPABASE_KEY,
    OPENAI_API_KEY,
    OPENAI_EMBEDDING_MODEL,
)

logger = logging.getLogger(__name__)


class VectorDBSupabase:
    """
    Wrapper para Supabase pgvector + RPC match_chunks.
    """

    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Faltan SUPABASE_URL o SUPABASE_KEY en .env")
        if not OPENAI_API_KEY:
            raise ValueError("Falta OPENAI_API_KEY en .env")

        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.openai = OpenAI(api_key=OPENAI_API_KEY)
        self.embedding_model = OPENAI_EMBEDDING_MODEL or "text-embedding-3-small"

        logger.info("✅ VectorDBSupabase inicializado (Supabase + OpenAI embeddings)")

    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        Genera embedding con OpenAI.
        """
        text = (text or "").strip()
        if not text:
            return None

        try:
            resp = self.openai.embeddings.create(
                model=self.embedding_model,
                input=text,
            )
            return resp.data[0].embedding
        except Exception as e:
            logger.error(f"❌ Error generando embedding OpenAI: {e}")
            return None

    def search(
        self,
        query: str,
        n_results: int = 3,
        threshold: float = 0.5,
    ) -> Optional[List[Dict]]:
        """
        Busca chunks similares en Supabase usando RPC match_chunks.

        Returns:
            Lista de dicts:
            {
              'documento': str (preview),
              'distancia': float,
              'metadata': {... incluye texto_completo ...}
            }
        """
        try:
            logger.info(f"🔍 Buscando: {query}")

            query_embedding = self.generate_embedding(query)
            if not query_embedding:
                logger.warning("No se pudo generar embedding de la pregunta")
                return None

            # RPC match_chunks
            rpc = self.supabase.rpc(
                "match_chunks",
                {
                    "query_embedding": query_embedding,
                    "match_threshold": threshold,
                    "match_count": n_results,
                },
            ).execute()

            if not rpc.data:
                logger.warning("No se encontraron chunks similares")
                return None

            results = []
            for i, row in enumerate(rpc.data):
                norma_id = row.get("norma_id")
                indice = row.get("indice")
                similarity = float(row.get("similarity", 0) or 0)

                # Traer texto completo + metadata norma
                try:
                    # Importante:
                    # - quitamos .single() porque tu DB tiene duplicados (PGRST116)
                    # - usamos limit(1) y tomamos la primera fila
                    # - 'año' es con ñ (según tu hint de Postgres)
                    tr = (
                        self.supabase.table("chunks")
                        .select("texto, normas(numero, año, url)")
                        .eq("norma_id", norma_id)
                        .eq("indice", indice)
                        .limit(1)
                        .execute()
                    )

                    if not tr.data:
                        logger.warning(f"No se encontró texto para chunk {norma_id}/{indice}")
                        continue

                    row0 = tr.data[0]
                    texto_completo = row0.get("texto") or ""
                    norma_meta = row0.get("normas") or {}

                    results.append(
                        {
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
                            },
                        }
                    )

                    logger.debug(
                        f"  [{i+1}] norma_id={norma_id} indice={indice} similarity={similarity:.3f}"
                    )

                except Exception as e:
                    logger.error(f"Error recuperando chunk {norma_id}/{indice}: {e}")
                    continue

            return results if results else None

        except Exception as e:
            logger.error(f"❌ Error en búsqueda Supabase: {e}")
            return None

    def health_check(self) -> bool:
        """
        Verifica que Supabase esté disponible.
        """
        try:
            _ = self.supabase.table("chunks").select("id").limit(1).execute()
            logger.info("✅ Health check Supabase OK")
            return True
        except Exception as e:
            logger.error(f"❌ Health check Supabase FAIL: {e}")
            return False
# ============ FIN src/vectordb_supabase.py ============