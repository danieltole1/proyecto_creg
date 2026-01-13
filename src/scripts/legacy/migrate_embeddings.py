#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de Migración: Qdrant Vector Migration
Gemini API (768 dims) → SentenceTransformers (384 dims)
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from src.vectordb_qdrant import VectorDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()


def main():
    logger.info("=" * 70)
    logger.info("MIGRACIÓN: Qdrant Vector Database")
    logger.info("Cambio: Gemini API (768 dims) → SentenceTransformers (384 dims)")
    logger.info("=" * 70)

    # PASO 1: Conectar a PostgreSQL
    logger.info("\n[1/6] 🔌 Conectando a PostgreSQL...")
    
    try:
        pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "creg_system"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        pg_cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
        logger.info("✅ Conectado a PostgreSQL")
    except Exception as e:
        logger.error("❌ Error conectando a PostgreSQL: %s", e)
        return

    # PASO 2: Inicializar VectorDB
    logger.info("\n[2/6] 🤖 Inicializando VectorDB con SentenceTransformers...")
    
    try:
        vdb = VectorDB()
        logger.info("✅ VectorDB inicializado (384 dims)")
    except Exception as e:
        logger.error("❌ Error inicializando VectorDB: %s", e)
        pg_conn.close()
        return

    # PASO 3: Borrar colección antigua
    logger.info("\n[3/6] 🗑️  Borrando colección antigua...")
    
    try:
        vdb.client.delete_collection(collection_name=vdb.collection_name)
        logger.info("✅ Colección '%s' borrada", vdb.collection_name)
    except Exception as e:
        logger.warning("⚠️  No se pudo borrar: %s", e)

    # PASO 4: Crear colección nueva
    logger.info("\n[4/6] 📁 Creando colección nueva con 384 dimensiones...")
    vdb._ensure_collection_exists()
    logger.info("✅ Colección '%s' lista (384 dims)", vdb.collection_name)

    # PASO 5: Obtener chunks de PostgreSQL
    logger.info("\n[5/6] 📖 Obteniendo chunks de PostgreSQL...")
    
    try:
        query = """
            SELECT 
                c.id as chunk_id,
                c.texto as text,
                c.indice as chunk_index,
                n.id as norma_id,
                n.titulo as title,
                n.numero as resolution_number,
                n.año as year,
                n.fecha_publicacion as publication_date
            FROM chunks c
            JOIN normas n ON c.norma_id = n.id
            ORDER BY n.id, c.indice
        """
        pg_cursor.execute(query)
        chunks = pg_cursor.fetchall()
        logger.info("✅ %d chunks obtenidos de PostgreSQL", len(chunks))
    except Exception as e:
        logger.error("❌ Error obteniendo chunks: %s", e)
        pg_conn.close()
        return

    if not chunks:
        logger.warning("⚠️  No hay chunks para migrar.")
        pg_conn.close()
        return

    # PASO 6: Re-vectorizar y guardar en Qdrant
    logger.info("\n[6/6] 🔄 Re-vectorizando %d chunks con SentenceTransformers...", len(chunks))
    logger.info("⏱️  Tiempo estimado: 5-10 minutos")
    
    success_count = 0
    error_count = 0

    for idx, chunk in enumerate(chunks, 1):
        try:
            metadata = {
                "chunk_id": str(chunk["chunk_id"]),
                "title": chunk["title"],
                "resolution_number": chunk["resolution_number"],
                "year": chunk["year"],
                "publication_date": (
                    chunk["publication_date"].isoformat()
                    if chunk["publication_date"]
                    else None
                ),
            }

            success = vdb.add_document(
                document_id=str(chunk["norma_id"]),
                content=chunk["text"],
                chunk_index=chunk["chunk_index"],
                metadata=metadata,
            )

            if success:
                success_count += 1
            else:
                error_count += 1

            # Log de progreso cada 50 chunks
            if idx % 50 == 0:
                logger.info("  ✅ Progreso: %d/%d chunks (%.1f%%)", 
                           idx, len(chunks), (idx/len(chunks))*100)

        except Exception as e:
            logger.error("❌ Error en chunk %s: %s", chunk["chunk_id"], e)
            error_count += 1

    # RESUMEN
    logger.info("\n" + "=" * 70)
    logger.info("✅ MIGRACIÓN COMPLETADA")
    logger.info("=" * 70)
    logger.info("Total chunks:      %d", len(chunks))
    logger.info("Exitosos:          %d", success_count)
    logger.info("Fallidos:          %d", error_count)
    logger.info("Vector size:       384 (SentenceTransformers)")
    logger.info("Colección:         %s", vdb.collection_name)

    try:
        stats = vdb.get_stats()
        logger.info("\n📊 Estadísticas finales de Qdrant:")
        logger.info("  - Points: %s", stats.get("points_count"))
        logger.info("  - Vector size: %s", stats.get("vector_size"))
        logger.info("  - Model: %s", stats.get("model"))
    except Exception as e:
        logger.warning("⚠️  Stats error: %s", e)

    logger.info("=" * 70)
    pg_conn.close()


if __name__ == "__main__":
    main()
