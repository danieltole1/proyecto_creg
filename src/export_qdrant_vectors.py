import json
from qdrant_client import QdrantClient

# Conectar a Qdrant local
client = QdrantClient(host="localhost", port=6333)

# Exportar puntos con payloads
collection_name = "creg_documents"
vectors_data = []

print("📥 Exportando vectores de Qdrant...")

# Paginar para no cargar todo en memoria
scroll_size = 100
scroll_offset = None

while True:
    points, scroll_offset = client.scroll(
        collection_name=collection_name,
        limit=scroll_size,
        offset=scroll_offset,
        with_payload=True,
        with_vectors=True
    )
    
    if not points:
        break
    
    for point in points:
        vectors_data.append({
            "id": point.id,
            "vector": point.vector,
            "payload": point.payload
        })
    
    print(f"   Exportados {len(vectors_data)} vectores...")

# Guardar a JSON
with open("qdrant_vectors_backup.json", "w", encoding="utf-8") as f:
    json.dump(vectors_data, f, indent=2)

print(f"✅ Total vectores exportados: {len(vectors_data)}")
print(f"📁 Guardado en: qdrant_vectors_backup.json")
