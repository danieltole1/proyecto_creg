import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

resp = client.embeddings.create(
    model=os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
    input="hola mundo"
)

print("dims:", len(resp.data[0].embedding))
