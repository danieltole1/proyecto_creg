import json
import os
import time
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from openai import OpenAI

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMB_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

BATCH = 50
SLEEP = 0.05

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
oa = OpenAI(api_key=OPENAI_API_KEY)

def embed(text: str):
    r = oa.embeddings.create(model=EMB_MODEL, input=text)
    return r.data[0].embedding

# Lee desde backfill_state.json para conocer el punto donde paró
state_file = Path("backfill_state.json")
if state_file.exists():
    state = json.loads(state_file.read_text(encoding="utf-8"))
    start_id = state.get("last_id", 0)
else:
    start_id = 0

# Paralelizar: esta instancia comienza desde (start_id + 66000)
# Permite que 2 instancias corran simultáneamente
last_id = start_id + 66000
updated = 0

print(f"[PARALELO-2] Iniciando desde ID > {last_id}")

while True:
    resp = (
        sb.table("chunks")
        .select("id,texto")
        .gt("id", last_id)
        .is_("embedding_openai", "null")
        .order("id", desc=False)
        .limit(BATCH)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        print(f"[PARALELO-2] DONE - No hay más chunks. Updated: {updated}")
        break

    for row in rows:
        chunk_id = row["id"]
        last_id = chunk_id
        texto = (row.get("texto") or "").strip()
        if not texto:
            continue

        try:
            vec = embed(texto)
            sb.table("chunks").update({"embedding_openai": vec}).eq("id", chunk_id).execute()
            updated += 1
            if updated % 10 == 0:
                print(f"[PARALELO-2] updated: {updated} last_id: {last_id}")
            time.sleep(SLEEP)
        except Exception as e:
            print(f"[PARALELO-2] error id {chunk_id}: {e}")
            time.sleep(0.05)

print(f"[PARALELO-2] TERMINADO - updated: {updated}")
