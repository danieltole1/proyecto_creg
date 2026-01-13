#!/usr/bin/env python3
import os
import sys
from dotenv import load_dotenv

load_dotenv()

print("=" * 70)
print("🔍 DIAGNÓSTICO DE SUPABASE")
print("=" * 70)

# PRUEBA 1: Verificar que las credenciales están cargadas
print("\n✅ PRUEBA 1: Credenciales cargadas")
print("-" * 70)

supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

if not supabase_url:
    print("❌ SUPABASE_URL no está configurada en .env")
    sys.exit(1)

if not supabase_key:
    print("❌ SUPABASE_KEY no está configurada en .env")
    sys.exit(1)

print(f"✅ SUPABASE_URL: {supabase_url[:30]}...")
print(f"✅ SUPABASE_KEY: {supabase_key[:30]}...")

# PRUEBA 2: Importar librería Supabase
print("\n✅ PRUEBA 2: Importar librería Supabase")
print("-" * 70)

try:
    from supabase import create_client
    print("✅ Librería supabase importada correctamente")
except ImportError as e:
    print(f"❌ Error importando supabase: {e}")
    sys.exit(1)

# PRUEBA 3: Conectar a Supabase
print("\n✅ PRUEBA 3: Conectar a Supabase")
print("-" * 70)

try:
    supabase = create_client(supabase_url, supabase_key)
    print("✅ Conexión a Supabase establecida")
except Exception as e:
    print(f"❌ Error conectando a Supabase: {e}")
    sys.exit(1)

# PRUEBA 4: Listar tablas
print("\n✅ PRUEBA 4: Listar tablas disponibles")
print("-" * 70)

try:
    # Intentar hacer una query simple
    result = supabase.table("chunks").select("id").limit(1).execute()
    print("✅ Tabla 'chunks' existe y es accesible")
    print(f"   Registros encontrados: {len(result.data) if result.data else 0}")
except Exception as e:
    print(f"❌ Error accediendo tabla 'chunks': {e}")
    sys.exit(1)

# PRUEBA 5: Contar registros
print("\n✅ PRUEBA 5: Contar registros en tabla 'chunks'")
print("-" * 70)

try:
    result = supabase.table("chunks").select("*", count="exact").execute()
    count = result.count if hasattr(result, 'count') else len(result.data)
    print(f"✅ Total de registros en 'chunks': {count}")
except Exception as e:
    print(f"❌ Error contando registros: {e}")
    sys.exit(1)

# PRUEBA 6: Probar búsqueda RPC
print("\n✅ PRUEBA 6: Probar función RPC 'match_chunks'")
print("-" * 70)

try:
    # Crear un embedding de prueba (vector de 1536 dimensiones de ceros)
    test_embedding = [0.0] * 1536
    
    result = supabase.rpc(
        "match_chunks",
        {
            "query_embedding": test_embedding,
            "match_count": 3,
            "similarity_threshold": 0.0  # Sin filtro para ver si funciona
        }
    ).execute()
    
    if result.data:
        print(f"✅ Función RPC 'match_chunks' funciona correctamente")
        print(f"   Resultados devueltos: {len(result.data)}")
    else:
        print("⚠️  Función RPC retorna datos vacíos (puede ser normal si no hay similares)")
except Exception as e:
    print(f"❌ Error en RPC 'match_chunks': {e}")
    sys.exit(1)

print("\n" + "=" * 70)
print("✅ TODAS LAS PRUEBAS PASARON - Supabase está bien configurado")
print("=" * 70)
