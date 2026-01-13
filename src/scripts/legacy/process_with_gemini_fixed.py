import os
import json
from dotenv import load_dotenv
from supabase import create_client
from google import genai
from google.genai import types
import time

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Configurar Gemini
client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY'))

print('=' * 80)
print('PROCESANDO CON GOOGLE GEMINI (GRATIS)')
print('=' * 80)

# Obtener chunks sin embedding
print('\nObteniendo chunks sin embedding...')
result = supabase.table('chunks').select('id, texto').is_('embedding_openai', 'null').execute()
chunks = result.data

print(f'Total: {len(chunks)} chunks')
print(f'Costo: GRATIS')
print(f'Tiempo estimado: ~45 minutos')

confirm = input('\nContinuar? (si/no): ')
if confirm.lower() != 'si':
    raise SystemExit()

print('\nProcesando...')
processed = 0
errors = 0
batch_updates = []

for chunk in chunks:
    try:
        response = client.models.embed_content(
            model='text-embedding-004',
            contents=chunk['texto']
        )
        
        embedding = response.embeddings[0].values
        batch_updates.append({'id': chunk['id'], 'embedding_openai': embedding})
        processed += 1
        
        if len(batch_updates) >= 100:
            for upd in batch_updates:
                supabase.table('chunks').update(
                    {'embedding_openai': upd['embedding_openai']}
                ).eq('id', upd['id']).execute()
            pct = processed * 100 / len(chunks)
            print(f'Progreso: {processed}/{len(chunks)} ({pct:.1f}%) - Errores: {errors}')
            batch_updates = []
        
        time.sleep(0.1)
        
    except Exception as e:
        chunk_id = chunk.get('id', 'unknown')
        print(f'Error en chunk {chunk_id}: {e}')
        errors += 1
        time.sleep(2)
        continue

if batch_updates:
    for upd in batch_updates:
        supabase.table('chunks').update(
            {'embedding_openai': upd['embedding_openai']}
        ).eq('id', upd['id']).execute()

print(f'\n' + '=' * 80)
print(f'COMPLETADO - COSTO: GRATIS')
print(f'=' * 80)
print(f'Embeddings: {processed}')
print(f'Errores: {errors}')
print(f'Cobertura: {(5085 + processed) / 132653 * 100:.1f}%')
