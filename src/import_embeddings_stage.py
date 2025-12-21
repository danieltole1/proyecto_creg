import json
from supabase import create_client

SUPABASE_URL = "https://kiouqyvvgbvrlrfguswp.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imtpb3VxeXZ2Z2J2cmxyZmd1c3dwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjU4NDE5OTksImV4cCI6MjA4MTQxNzk5OX0.e-qkDhpmnF-Zk1GeRduBJVG1zt2LBvYobu4j8SFaefs"  # pega la misma anon key que usaste antes

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

FILEPATH = r"C:\temp_creg\qdrant_vectors_backup.jsonl"
BATCH = 200  # si da error por tamaño, bájalo a 100

def flush(batch_rows):
    # upsert: si ya existe id, lo actualiza; si no, lo inserta
    supabase.table("chunks_embedding_stage").upsert(batch_rows).execute()

count = 0
batch_rows = []

with open(FILEPATH, "r", encoding="utf-8") as f:
    for line in f:
        item = json.loads(line)
        batch_rows.append({
            "id": int(item["id"]),
            "embedding": item["vector"]
        })
        count += 1

        if len(batch_rows) >= BATCH:
            flush(batch_rows)
            batch_rows = []
            if count % 5000 == 0:
                print(f" Subidos a staging: {count}")

if batch_rows:
    flush(batch_rows)

print(f" Terminado staging. Total: {count}")
