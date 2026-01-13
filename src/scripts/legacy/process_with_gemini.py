import os
import json
from dotenv import load_dotenv
from supabase import create_client
import google.generativeai as genai
import time

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Configurar Gemini (necesitas API key de Google)
genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))

print('=' * 80)
print('PROCESANDO CON GOOGLE GEMINI (GRATIS)')
print('=' * 80)

# Obtener chunks sin embedding
print('\nObteniendo chunks sin embedding...')
result = supabase.table('chunks').select('id, text').is_('embedding_openai', 'null').execute()
chunks = result.data

print(f'Total: {len(chunks)} chunks')
print(f'Costo: GRATIS (Google Gemini)')
print(f'Tiempo estimado: ~45 minutos')

confirm = input('\nContinuar? (si/no): ')
if confirm.lower() != 'si':
    raise SystemExit()

processed = 0
errors = 0
batch_updates = []

for chunk in chunks:
    try:
        # Usar Gemini para embeddings
        result = genai.embed_content(
            model='models/text-embedding-004',
            content=chunk['text'],
            task_type='retrieval_document'
        )
        
        embedding = result['embedding']
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
        
        time.sleep(0.1)  # Rate limit de Gemini gratis
        
    except Exception as e:
        print(f'Error: {e}')
        errors += 1
        time.sleep(2)
        continue

if batch_updates:
    for upd in batch_updates:
        supabase.table('chunks').update(
            {'embedding_openai': upd['embedding_openai']}
        ).eq('id', upd['id']).execute()

print(f'\nCompletado: {processed} embeddings - COSTO:  USD')
