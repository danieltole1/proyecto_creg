import os
from dotenv import load_dotenv
from supabase import create_client
from google import genai
import time

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY'))

print('=' * 80)
print('PROCESAMIENTO CONTINUO DE EMBEDDINGS CON GEMINI (GRATIS)')
print('=' * 80)

total_processed = 0
round_number = 1

while True:
    print(f'\n🔄 RONDA {round_number}')
    print('-' * 80)
    
    result = supabase.table('chunks').select('id, texto').is_('embedding_gemini', 'null').limit(1000).execute()
    chunks = result.data
    
    if not chunks:
        print('\n✅ NO HAY MÁS CHUNKS POR PROCESAR')
        break
    
    print(f'Procesando {len(chunks)} chunks...')
    
    batch_updates = []
    errors = 0
    
    for chunk in chunks:
        try:
            response = client.models.embed_content(
                model='text-embedding-004',
                contents=chunk['texto']
            )
            
            embedding = response.embeddings[0].values
            batch_updates.append({'id': chunk['id'], 'embedding_gemini': embedding})
            
            if len(batch_updates) >= 50:
                for upd in batch_updates:
                    supabase.table('chunks').update(
                        {'embedding_gemini': upd['embedding_gemini']}
                    ).eq('id', upd['id']).execute()
                batch_updates = []
            
            time.sleep(0.1)
            
        except Exception as e:
            print(f'✗ Error: {e}')
            errors += 1
            time.sleep(2)
    
    if batch_updates:
        for upd in batch_updates:
            supabase.table('chunks').update(
                {'embedding_gemini': upd['embedding_gemini']}
            ).eq('id', upd['id']).execute()
    
    processed_this_round = len(chunks) - errors
    total_processed += processed_this_round
    
    print(f'✓ Ronda {round_number}: {processed_this_round} procesados | Errores: {errors}')
    print(f'📊 Total acumulado: {total_processed:,} chunks')
    
    round_number += 1
    time.sleep(5)

print(f'\n' + '=' * 80)
print(f'✅ PROCESAMIENTO COMPLETO')
print(f'=' * 80)
print(f'Total embeddings creados: {total_processed:,}')
print(f'Costo total: \ USD (GRATIS)')
