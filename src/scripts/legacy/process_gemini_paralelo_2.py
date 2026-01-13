import os
from dotenv import load_dotenv
from supabase import create_client
from google import genai
import time

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY'))

print('PROCESADOR PARALELO #1')

total_processed = 0
round_number = 1

while True:
    result = supabase.table('chunks').select('id, texto').is_('embedding_gemini', 'null').limit(1000).execute()
    chunks = result.data
    
    if not chunks:
        break
    
    print(f'Ronda {round_number}: {len(chunks)} chunks')
    
    batch_updates = []
    errors = 0
    processed = 0
    
    for chunk in chunks:
        try:
            response = client.models.embed_content(
                model='text-embedding-004',
                contents=chunk['texto']
            )
            
            embedding = response.embeddings[0].values
            batch_updates.append({'id': chunk['id'], 'embedding_gemini': embedding})
            processed += 1
            
            if len(batch_updates) >= 50:
                for upd in batch_updates:
                    supabase.table('chunks').update(
                        {'embedding_gemini': upd['embedding_gemini']}
                    ).eq('id', upd['id']).execute()
                batch_updates = []
            
            time.sleep(0.15)
            
        except Exception as e:
            errors += 1
            time.sleep(2)
    
    if batch_updates:
        for upd in batch_updates:
            supabase.table('chunks').update(
                {'embedding_gemini': upd['embedding_gemini']}
            ).eq('id', upd['id']).execute()
    
    total_processed += processed
    print(f'✓ Ronda {round_number}: {processed} procesados')
    round_number += 1
    time.sleep(3)
