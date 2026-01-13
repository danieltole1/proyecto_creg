import os
from dotenv import load_dotenv
from supabase import create_client
from google import genai
import time

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY'))

print('=' * 80)
print('PROCESAMIENTO CON AUTO-RETRY Y LOGGING')
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
    
    print(f'📦 Procesando {len(chunks)} chunks...')
    
    batch_updates = []
    errors = 0
    processed_in_round = 0
    start_time = time.time()
    
    for i, chunk in enumerate(chunks, 1):
        max_retries = 3
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                response = client.models.embed_content(
                    model='text-embedding-004',
                    contents=chunk['texto']
                )
                
                embedding = response.embeddings[0].values
                batch_updates.append({'id': chunk['id'], 'embedding_gemini': embedding})
                processed_in_round += 1
                success = True
                
                if len(batch_updates) >= 50:
                    for upd in batch_updates:
                        supabase.table('chunks').update(
                            {'embedding_gemini': upd['embedding_gemini']}
                        ).eq('id', upd['id']).execute()
                    batch_updates = []
                
                time.sleep(0.15)
                
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    print(f'  ⚠️  Retry {retry_count}/{max_retries} en chunk {i}: {str(e)[:80]}')
                    time.sleep(5 * retry_count)  # Espera incremental
                else:
                    print(f'  ✗ Error definitivo en chunk {i} después de {max_retries} intentos')
                    errors += 1
        
        # Progreso cada 100 chunks
        if i % 100 == 0:
            elapsed = time.time() - start_time
            rate = processed_in_round / elapsed if elapsed > 0 else 0
            print(f'  → {i}/{len(chunks)} ({i/len(chunks)*100:.0f}%) | {rate:.1f} chunks/s | Errores: {errors}')
    
    # Guardar últimos del batch
    if batch_updates:
        for upd in batch_updates:
            supabase.table('chunks').update(
                {'embedding_gemini': upd['embedding_gemini']}
            ).eq('id', upd['id']).execute()
    
    total_processed += processed_in_round
    elapsed_total = time.time() - start_time
    
    print(f'✓ Ronda {round_number}: {processed_in_round} procesados | Errores: {errors} | Tiempo: {elapsed_total/60:.1f} min')
    print(f'📊 Total acumulado: {total_processed:,} chunks')
    
    round_number += 1
    time.sleep(3)

print(f'\n' + '=' * 80)
print(f'✅ PROCESAMIENTO COMPLETO')
print(f'=' * 80)
print(f'Total embeddings creados: {total_processed:,}')
print(f'Costo total: USD 0 (GRATIS)')
