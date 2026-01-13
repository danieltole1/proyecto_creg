import os
import json
from dotenv import load_dotenv
from supabase import create_client
from google import genai
import time

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY'))

print('=' * 80)
print('PROCESANDO TODOS LOS CHUNKS CON GOOGLE GEMINI (GRATIS)')
print('=' * 80)

print('\nObteniendo TODOS los chunks sin embedding_gemini...')
result = supabase.table('chunks').select('id, texto').is_('embedding_gemini', 'null').execute()
chunks = result.data

print(f'Total chunks: {len(chunks)}')
print(f'Costo: GRATIS')
print(f'Tiempo estimado: ~{len(chunks) * 0.1 / 60:.0f} minutos')
print(f'\nEsto procesara TODO desde cero con Gemini.')

confirm = input('\nContinuar? (si/no): ')
if confirm.lower() != 'si':
    raise SystemExit()

print('\nProcesando...')
processed = 0
errors = 0
batch_updates = []
start_time = time.time()

for i, chunk in enumerate(chunks, 1):
    try:
        response = client.models.embed_content(
            model='text-embedding-004',
            contents=chunk['texto']
        )
        
        embedding = response.embeddings[0].values
        batch_updates.append({'id': chunk['id'], 'embedding_gemini': embedding})
        processed += 1
        
        if len(batch_updates) >= 100:
            for upd in batch_updates:
                supabase.table('chunks').update(
                    {'embedding_gemini': upd['embedding_gemini']}
                ).eq('id', upd['id']).execute()
            
            pct = processed * 100 / len(chunks)
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (len(chunks) - processed) / rate / 60 if rate > 0 else 0
            
            print(f'Progreso: {processed}/{len(chunks)} ({pct:.1f}%) - Errores: {errors} - ETA: {eta:.0f} min')
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
            {'embedding_gemini': upd['embedding_gemini']}
        ).eq('id', upd['id']).execute()

elapsed_total = time.time() - start_time

print(f'\n' + '=' * 80)
print(f'COMPLETADO - COSTO:  USD (GRATIS)')
print(f'=' * 80)
print(f'Embeddings procesados: {processed}')
print(f'Errores: {errors}')
print(f'Tiempo total: {elapsed_total / 60:.1f} minutos')
print(f'Cobertura: {processed / 132653 * 100:.1f}%')
print(f'\nVerifica en Supabase:')
print(f'SELECT COUNT(embedding_gemini) FROM chunks WHERE embedding_gemini IS NOT NULL;')
