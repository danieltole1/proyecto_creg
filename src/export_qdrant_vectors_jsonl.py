import json
from qdrant_client import QdrantClient

COLLECTION = "creg_documents"
OUTFILE = "qdrant_vectors_backup.jsonl"
LIMIT = 256

client = QdrantClient(host="localhost", port=6333)

info = client.get_collection(COLLECTION)
expected = info.points_count
print(f"📌 Colección: {COLLECTION}")
print(f"📌 points_count (esperado aprox): {expected}")

offset = None
count = 0

with open(OUTFILE, "w", encoding="utf-8") as f:
    while True:
        points, next_offset = client.scroll(
            collection_name=COLLECTION,
            limit=LIMIT,
            offset=offset,
            with_payload=True,
            with_vectors=True,
        )

        if not points:
            break

        for p in points:
            f.write(json.dumps({
                "id": p.id,
                "vector": p.vector,
                "payload": p.payload
            }, ensure_ascii=False) + "\n")
            count += 1

        offset = next_offset
        if count % 5000 == 0:
            print(f"   Exportados {count}...")

        if offset is None:
            break

print(f"✅ Export terminado. Total exportados: {count}")
print(f"📄 Archivo: {OUTFILE}")
