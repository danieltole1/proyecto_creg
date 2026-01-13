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

BATCH = int(os.getenv("BACKFILL_BATCH", "50"))
SLEEP = float(os.getenv("BACKFILL_SLEEP", "0.05"))

STATE_FILE = Path("backfill_state.json")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
oa = OpenAI(api_key=OPENAI_API_KEY)


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"last_id": 0, "updated": 0}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def embed(text: str):
    r = oa.embeddings.create(model=EMB_MODEL, input=text)
    return r.data[0].embedding


state = load_state()
last_id = int(state.get("last_id", 0))
updated = int(state.get("updated", 0))

print(f"[BACKFILL] Iniciando desde last_id={last_id}, updated={updated}")

attempt = 0
max_attempts = 3

while True:
    try:
        # Con .limit(1000) evitamos timeout en búsqueda
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
        attempt = 0  # reset on success
        
        if not rows:
            print(f"[BACKFILL] ✅ DONE - No hay más chunks sin embedding")
            print(f"[BACKFILL] Total updated: {updated}")
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
                    print(f"[BACKFILL] updated: {updated:,} last_id: {last_id}")
                    save_state({"last_id": last_id, "updated": updated})

                time.sleep(SLEEP)

            except Exception as e:
                print(f"[BACKFILL] ❌ Error embedding chunk {chunk_id}: {e}")
                time.sleep(0.05)

    except Exception as e:
        attempt += 1
        print(f"[BACKFILL] ⚠️  Error en query (intento {attempt}/3): {e}")
        if attempt >= max_attempts:
            print(f"[BACKFILL] ❌ Máximo de intentos alcanzado. Guardando state...")
            save_state({"last_id": last_id, "updated": updated})
            break
        time.sleep(5.0)  # espera 5 seg antes de reintentar

save_state({"last_id": last_id, "updated": updated})
print(f"[BACKFILL] Finalizado. Total embeddings procesados: {updated:,}")
# ============ FIN backfill_openai_embeddings_supabase.py ============