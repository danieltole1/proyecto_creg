import os
from dotenv import load_dotenv
from src.vectordb_qdrant import VectorDB

load_dotenv()

print("\n" + "=" * 70)
print("🔍 PRUEBA DE BÚSQUEDA SEMÁNTICA")
print("=" * 70)

vdb = VectorDB()

# Queries de prueba
queries = [
    "regulación de energía eléctrica",
    "transmisión y distribución",
    "tarifas y precios",
    "resoluciones CREG 2024",
]

for query in queries:
    print(f"\n🔎 Query: '{query}'")
    print("-" * 70)
    
    results = vdb.search(query, limit=3)
    
    if not results:
        print("  ⚠️  No se encontraron resultados")
        continue
    
    for i, r in enumerate(results, 1):
        print(f"\n  [{i}] Score: {r.score:.4f}")
        print(f"      Doc ID: {r.document_id}")
        print(f"      Título: {r.metadata.get('title', 'N/A')[:60]}...")
        print(f"      Resolución: {r.metadata.get('resolution_number', 'N/A')}")
        print(f"      Preview: {r.content[:100]}...")

print("\n" + "=" * 70)
print("✅ PRUEBAS COMPLETADAS")
print("=" * 70)
