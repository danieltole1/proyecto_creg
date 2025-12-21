import os
import psycopg2
from supabase import create_client
from dotenv import load_dotenv
import json

load_dotenv()

# Credenciales Postgres local
LOCAL_DB_HOST = "localhost"
LOCAL_DB_USER = "normativa_user"
LOCAL_DB_PASSWORD = "n8n_creg"
LOCAL_DB_NAME = "normativa_db"

# Credenciales Supabase
SUPABASE_URL = "https://kiouqyvvgbvrlrfguswp.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imtpb3VxeXZ2Z2J2cmxyZmd1c3dwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjU4NDE5OTksImV4cCI6MjA4MTQxNzk5OX0.e-qkDhpmnF-Zk1GeRduBJVG1zt2LBvYobu4j8SFaefs"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🔄 Conectando a Postgres local...")
conn = psycopg2.connect(
    host=LOCAL_DB_HOST,
    user=LOCAL_DB_USER,
    password=LOCAL_DB_PASSWORD,
    database=LOCAL_DB_NAME
)
cursor = conn.cursor()

# ============ EXPORTAR NORMAS ============
print("📥 Exportando normas...")
cursor.execute("""
    SELECT id, numero, año, titulo, url, fecha_publicacion, 
           estado, fecha_creacion, fecha_ultima_revision, tipo, doc_key
    FROM normas
    ORDER BY id
""")

normas = cursor.fetchall()
print(f"   Total: {len(normas)} normas")

# Insertar en Supabase (por lotes de 100)
batch_size = 100
for i in range(0, len(normas), batch_size):
    batch = normas[i:i+batch_size]
    
    rows = []
    for norm in batch:
        rows.append({
            "id": norm[0],
            "numero": norm[1],
            "año": norm[2],
            "titulo": norm[3],
            "url": norm[4],
            "fecha_publicacion": norm[5].isoformat() if norm[5] else None,
            "estado": norm[6],
            "fecha_creacion": norm[7].isoformat() if norm[7] else None,
            "fecha_ultima_revision": norm[8].isoformat() if norm[8] else None,
            "tipo": norm[9],
            "doc_key": norm[10]
        })
    
    try:
        supabase.table("normas").insert(rows).execute()
        print(f"   ✅ Insertadas normas {i+1}-{min(i+batch_size, len(normas))}")
    except Exception as e:
        print(f"   ❌ Error en lote {i//batch_size}: {e}")

# ============ EXPORTAR CHUNKS (SIN embedding aún) ============
print("\n📥 Exportando chunks (sin embeddings)...")
cursor.execute("""
    SELECT id, norma_id, indice, texto, fecha_creacion
    FROM chunks
    ORDER BY id
""")

chunks = cursor.fetchall()
print(f"   Total: {len(chunks)} chunks")

# Insertar chunks sin embedding
for i in range(0, len(chunks), batch_size):
    batch = chunks[i:i+batch_size]
    
    rows = []
    for chunk in batch:
        rows.append({
            "id": chunk[0],
            "norma_id": chunk[1],
            "indice": chunk[2],
            "texto": chunk[3],
            "fecha_creacion": chunk[4].isoformat() if chunk[4] else None,
            "embedding": None,  # Se llenará después con los vectores
            "tipo_chunk": "texto"
        })
    
    try:
        supabase.table("chunks").insert(rows).execute()
        print(f"   ✅ Insertados chunks {i+1}-{min(i+batch_size, len(chunks))}")
    except Exception as e:
        print(f"   ❌ Error en lote {i//batch_size}: {e}")

cursor.close()
conn.close()

print("\n✅ Migración de datos completada")
print("⏭️  Siguiente: Importar vectores desde qdrant_vectors_backup.json")
# ============ IMPORTAR VECTORES DESDE QDRANT ============