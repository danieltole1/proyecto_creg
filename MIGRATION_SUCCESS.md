# MIGRACIÓN EXITOSA: Gemini API → SentenceTransformers
Fecha: 2025-12-11

## Problema Original
- Error 429: Gemini API quota exceeded
- Límite embed_content_free_tier_requests: 0
- No se podían generar más embeddings

## Solución Implementada
- Modelo: SentenceTransformers all-MiniLM-L6-v2
- Dimensiones: 384 (antes 768)
- Costo: $0 (ilimitado, local)
- Velocidad: ~100ms por embedding

## Resultados
- 480 chunks migrados exitosamente
- Búsqueda semántica operativa (scores 0.4-0.7)
- Sistema completamente funcional sin dependencias de API

## Archivos Modificados
- src/vectordb_qdrant.py (migrado a SentenceTransformers)
- migrate_embeddings.py (script de migración)
