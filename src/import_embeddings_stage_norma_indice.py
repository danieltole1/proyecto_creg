import json
from supabase import create_client

SUPABASE_URL = "https://kiouqyvvgbvrlrfguswp.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imtpb3VxeXZ2Z2J2cmxyZmd1c3dwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjU4NDE5OTksImV4cCI6MjA4MTQxNzk5OX0.e-qkDhpmnF-Zk1GeRduBJVG1zt2LBvYobu4j8SFaefs"

FILEPATH = r"C:\temp_creg\qdrant_vectors_backup.jsonl"
BATCH = 200

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def flush(rows):
    # Deduplicar dentro del batch por (norma_id, indice)
    dedup = {}
    for r in rows:
        dedup[(r["norma_id"], r["indice"])] = r  # se queda con el último
    supabase.table("chunks_embedding_stage").upsert(list(dedup.values())).execute()

count = 0
rows = []
skipped = 0

with open(FILEPATH, "r", encoding="utf-8") as f:
    for line in f:
        item = json.loads(line)
        payload = item.get("payload") or {}

        norma_id = payload.get("norma_id")
        indice = payload.get("chunk_index")

        if norma_id is None or indice is None:
            skipped += 1
            continue

        rows.append({
            "norma_id": int(norma_id),
            "indice": int(indice),
            "embedding": item["vector"]
        })
        count += 1

        if len(rows) >= BATCH:
            flush(rows)
            rows = []
            if count % 5000 == 0:
                print(f"✅ Procesados: {count} (skipped={skipped})")

if rows:
    flush(rows)

print(f"🎉 Terminado. Procesados: {count} (skipped={skipped})")
