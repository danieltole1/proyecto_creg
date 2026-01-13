import os
import sys
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# Conectar
pg_conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DB", "creg_system"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
cursor = pg_conn.cursor()

print("🔍 TABLAS EN LA BASE DE DATOS:")
print("=" * 50)

cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")

tables = cursor.fetchall()
if not tables:
    print("  ⚠️  NO HAY TABLAS")
else:
    for t in tables:
        print(f"  ✅ {t[0]}")

print("\n🔍 VERIFICANDO DATOS:")
print("=" * 50)

for t in tables:
    table_name = t[0]
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  📊 {table_name}: {count} registros")
    except Exception as e:
        print(f"  ❌ {table_name}: {e}")

pg_conn.close()
