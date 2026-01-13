import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

pg_conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DB", "creg_system"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
cursor = pg_conn.cursor()

print("📋 COLUMNAS DE LA TABLA 'normas':")
print("=" * 50)

cursor.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'normas'
    ORDER BY ordinal_position;
""")

for col in cursor.fetchall():
    print(f"  ✅ {col[0]:30s} ({col[1]})")

print("\n📋 COLUMNAS DE LA TABLA 'chunks':")
print("=" * 50)

cursor.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'chunks'
    ORDER BY ordinal_position;
""")

for col in cursor.fetchall():
    print(f"  ✅ {col[0]:30s} ({col[1]})")

pg_conn.close()
