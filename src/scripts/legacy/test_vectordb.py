# test_vectordb.py
"""Test: Agregar documentos y buscar en ChromaDB"""

import logging
from src.vectordb import VectorDB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Documentos de ejemplo (simulando normas CREG)
documentos_ejemplo = [
    {
        "id": "creg-001",
        "texto": "Resolución 174/2021: Metodología para el cálculo de tarifas de energía eléctrica en el Sistema Interconectado Nacional.",
        "metadata": {"tipo": "resolucion", "año": 2021, "tema": "tarifas"}
    },
    {
        "id": "creg-002",
        "texto": "Circular 006620/2024: Procedimientos para la solicitud y aprobación de proyectos de expansión de la red de distribución.",
        "metadata": {"tipo": "circular", "año": 2024, "tema": "expansion"}
    },
    {
        "id": "creg-003",
        "texto": "Resolución 045/2023: Estándares mínimos de calidad de servicio técnico para empresas prestadoras de servicios públicos.",
        "metadata": {"tipo": "resolucion", "año": 2023, "tema": "calidad"}
    },
    {
        "id": "creg-004",
        "texto": "Documento sobre la regulación de precios en el mercado mayorista de energía eléctrica.",
        "metadata": {"tipo": "documento", "año": 2024, "tema": "precios"}
    },
]

def main():
    print("=" * 60)
    print("TEST: ChromaDB Vector Search")
    print("=" * 60)
    
    # Conectar a ChromaDB
    print("\n[1] Conectando a ChromaDB...")
    try:
        db = VectorDB()
    except Exception as e:
        print(f"❌ No se pudo conectar: {e}")
        print("   Asegúrate de que ChromaDB esté corriendo:")
        print("   docker compose up chromadb -d")
        return
    
    # Obtener info de colección
    print("\n[2] Estado de la colección...")
    info = db.get_collection_info()
    print(f"   {info}")
    
    # Agregar documentos de ejemplo
    print("\n[3] Agregando documentos de ejemplo...")
    docs_textos = [d["texto"] for d in documentos_ejemplo]
    docs_ids = [d["id"] for d in documentos_ejemplo]
    docs_metas = [d["metadata"] for d in documentos_ejemplo]
    
    if db.add_documents(docs_textos, ids=docs_ids, metadatas=docs_metas):
        print("   ✅ Documentos agregados correctamente")
    else:
        print("   ❌ Error agregando documentos")
        return
    
    # Verificar cantidad
    info = db.get_collection_info()
    print(f"   {info}")
    
    # Realizar búsquedas
    print("\n[4] Realizando búsquedas de prueba...")
    queries = [
        "¿Cuál es la metodología para calcular tarifas?",
        "¿Qué procedimientos existen para expansión de red?",
        "¿Cuáles son los estándares de calidad de servicio?",
        "Precios en el mercado mayorista",
    ]
    
    for query in queries:
        print(f"\n   Query: '{query}'")
        results = db.search(query, n_results=2)
        
        if results:
            for i, (doc, dist, meta, id_) in enumerate(zip(
                results['documents'][0],
                results['distances'][0],
                results['metadatas'][0],
                results['ids'][0]
            )):
                # En ChromaDB, distances es 1 - similitud (cosine distance)
                similitud = 1 - dist
                print(f"     [{i+1}] ID: {id_} | Similitud: {similitud:.3f}")
                print(f"          {doc[:80]}...")
                print(f"          Meta: {meta}")

if __name__ == "__main__":
    main()
