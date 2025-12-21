import os
from dotenv import load_dotenv
from src.vectordb_supabase import VectorDBSupabase

load_dotenv()

db = VectorDBSupabase()
res = db.search("¿Qué dice la CREG sobre tarifas?", n_results=3, threshold=0.4)
print("ok:", bool(res))
print(res[:1] if res else res)
